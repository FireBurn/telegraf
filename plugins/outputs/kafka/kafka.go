//go:generate ../../../tools/readme_config_includer/generator
package kafka

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"text/template"
	"time"

	"github.com/IBM/sarama"
	"github.com/gofrs/uuid/v5"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/common/kafka"
	"github.com/influxdata/telegraf/plugins/common/proxy"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

var zeroTime = time.Unix(0, 0)

// Allow one replacement attempt while cleanup of the original producer is
// stuck, but do not accumulate producers indefinitely on repeated timeouts.
const (
	maxPoisonedProducers = 2
	defaultCloseTimeout  = 5 * time.Second
)

type Kafka struct {
	Brokers           []string          `toml:"brokers"`
	Topic             string            `toml:"topic"`
	TopicTag          string            `toml:"topic_tag"`
	ExcludeTopicTag   bool              `toml:"exclude_topic_tag"`
	TopicSuffix       TopicSuffix       `toml:"topic_suffix"`
	RoutingTag        string            `toml:"routing_tag"`
	RoutingKey        string            `toml:"routing_key"`
	ProducerTimestamp string            `toml:"producer_timestamp"`
	MetricNameHeader  string            `toml:"metric_name_header" deprecated:"1.39.0;1.45.0;please use 'headers' instead"`
	Headers           map[string]string `toml:"headers"`
	Log               telegraf.Logger   `toml:"-"`
	proxy.Socks5ProxyConfig
	kafka.WriteConfig

	saramaConfig *sarama.Config
	producerFunc func(addrs []string, config *sarama.Config) (sarama.SyncProducer, error)
	producerMu   sync.Mutex
	producer     sarama.SyncProducer
	producerGen  uint64
	attempt      *producerAttempt
	poisoned     int
	limitLogged  bool
	closed       bool
	closeTimeout time.Duration
	headerTmpl   map[string]*template.Template

	serializer telegraf.Serializer
}

type producerAttempt struct {
	done       chan struct{}
	producer   sarama.SyncProducer
	generation uint64
	err        error
}

type TopicSuffix struct {
	Method    string   `toml:"method"`
	Keys      []string `toml:"keys"`
	Separator string   `toml:"separator"`
}

func (*Kafka) SampleConfig() string {
	return sampleConfig
}

func (k *Kafka) SetSerializer(serializer telegraf.Serializer) {
	k.serializer = serializer
}

func (k *Kafka) Init() error {
	kafka.SetLogger(k.Log.Level())

	// Validate the topic-suffix method
	switch k.TopicSuffix.Method {
	case "", "measurement", "tags":
		// Do nothing, those are valid
	default:
		return fmt.Errorf("unknown topic suffix method provided: %s", k.TopicSuffix.Method)
	}

	// Legacy support for metric_name_header
	if k.MetricNameHeader != "" {
		if k.Headers == nil {
			k.Headers = make(map[string]string, 1)
		}
		k.Headers[k.MetricNameHeader] = "{{ .Name }}"
	}

	// Create new configuration
	config := sarama.NewConfig()
	if err := k.SetConfig(config, k.Log); err != nil {
		return err
	}

	if k.Socks5ProxyEnabled {
		config.Net.Proxy.Enable = true

		dialer, err := k.Socks5ProxyConfig.GetDialer()
		if err != nil {
			return fmt.Errorf("connecting to proxy server failed: %w", err)
		}
		config.Net.Proxy.Dialer = dialer
	}
	k.saramaConfig = config

	switch k.ProducerTimestamp {
	case "":
		k.ProducerTimestamp = "metric"
	case "metric", "now":
	default:
		return fmt.Errorf("unknown producer_timestamp option: %s", k.ProducerTimestamp)
	}

	// Setup header templates
	k.headerTmpl = make(map[string]*template.Template, len(k.Headers))
	for name, expr := range k.Headers {
		tmpl, err := template.New(name).Parse(expr)
		if err != nil {
			return fmt.Errorf("creating template for header %q failed: %w", name, err)
		}
		k.headerTmpl[name] = tmpl
	}

	return nil
}

func (k *Kafka) Connect() error {
	return k.ConnectContext(context.Background())
}

func (k *Kafka) ConnectContext(ctx context.Context) error {
	_, _, err := k.acquireProducer(ctx)
	return err
}

func (k *Kafka) Close() error {
	k.producerMu.Lock()
	k.closed = true
	producer := k.producer
	k.producer = nil
	k.producerMu.Unlock()
	if producer == nil {
		return nil
	}

	done := make(chan error, 1)
	go func() {
		done <- producer.Close()
	}()
	timeout := k.producerCloseTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return fmt.Errorf("closing Kafka producer timed out after %s", timeout)
	}
}

func (k *Kafka) acquireProducer(ctx context.Context) (sarama.SyncProducer, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	k.producerMu.Lock()
	if k.closed {
		k.producerMu.Unlock()
		return nil, 0, errors.New("kafka output is closed")
	}
	if k.producer != nil {
		producer, generation := k.producer, k.producerGen
		k.producerMu.Unlock()
		return producer, generation, nil
	}
	if k.poisoned >= maxPoisonedProducers {
		if !k.limitLogged {
			k.Log.Errorf("Kafka producer replacement limit reached; refusing to create another producer because previous producers are still shutting down")
			k.limitLogged = true
		}
		k.producerMu.Unlock()
		return nil, 0, errors.New("kafka producer replacement limit reached while previous producers are still shutting down")
	}

	attempt := k.attempt
	startAttempt := false
	if attempt == nil {
		attempt = &producerAttempt{done: make(chan struct{})}
		k.attempt = attempt
		startAttempt = true
	}
	k.producerMu.Unlock()
	if startAttempt {
		go k.createProducer(attempt)
	}

	select {
	case <-attempt.done:
		if attempt.err != nil && ctx.Err() != nil {
			return nil, 0, ctx.Err()
		}
		return attempt.producer, attempt.generation, attempt.err
	case <-ctx.Done():
		return nil, 0, ctx.Err()
	}
}

func (k *Kafka) createProducer(attempt *producerAttempt) {
	producer, err := k.producerFunc(k.Brokers, k.saramaConfig)

	k.producerMu.Lock()
	install := err == nil && !k.closed && k.attempt == attempt && k.producer == nil
	if install {
		k.producer = producer
		k.producerGen++
		attempt.producer = producer
		attempt.generation = k.producerGen
	} else if err != nil {
		attempt.err = &internal.StartupError{Err: err, Retry: true}
	} else {
		attempt.err = errors.New("kafka producer creation result is no longer usable")
	}
	if k.attempt == attempt {
		k.attempt = nil
	}
	close(attempt.done)
	k.producerMu.Unlock()

	if producer != nil && !install {
		k.disposeProducer(producer)
	}
}

func (k *Kafka) disposeProducer(producer sarama.SyncProducer) {
	done := make(chan error, 1)
	go func() {
		done <- producer.Close()
	}()
	timer := time.NewTimer(k.producerCloseTimeout())
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			k.Log.Errorf("Error closing unused Kafka producer: %v", err)
		}
	case <-timer.C:
		k.Log.Errorf("Timed out closing unused Kafka producer; abandoning it")
	}
}

func (k *Kafka) producerCloseTimeout() time.Duration {
	if k.closeTimeout > 0 {
		return k.closeTimeout
	}
	return defaultCloseTimeout
}

// Write writes the metrics to Kafka. It ignores context cancellation.
func (k *Kafka) Write(metrics []telegraf.Metric) error {
	return k.WriteContext(context.Background(), metrics)
}

// WriteContext writes the metrics to Kafka. It can be cancelled via the context.
func (k *Kafka) WriteContext(ctx context.Context, metrics []telegraf.Metric) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	producer, generation, err := k.acquireProducer(ctx)
	if err != nil {
		return err
	}

	msgs := make([]*sarama.ProducerMessage, 0, len(metrics))
	for _, metric := range metrics {
		metric, topic := k.getTopicName(metric)

		buf, err := k.serializer.Serialize(metric)
		if err != nil {
			k.Log.Debugf("Could not serialize metric: %v", err)
			continue
		}

		m := &sarama.ProducerMessage{
			Topic:   topic,
			Value:   sarama.ByteEncoder(buf),
			Headers: make([]sarama.RecordHeader, 0, len(k.headerTmpl)),
		}

		// Set the message headers
		var headerValue bytes.Buffer
		for name, tmpl := range k.headerTmpl {
			headerValue.Reset()
			if err := tmpl.Execute(&headerValue, metric); err != nil {
				k.Log.Errorf("adding header %q failed: %v", name, err)
				continue
			}
			m.Headers = append(m.Headers, sarama.RecordHeader{
				Key:   []byte(name),
				Value: slices.Clone(headerValue.Bytes()),
			})
		}

		// Negative timestamps are not allowed by the Kafka protocol.
		if k.ProducerTimestamp == "metric" && !metric.Time().Before(zeroTime) {
			m.Timestamp = metric.Time()
		}

		// Add the routing key if configured
		key, err := k.routingKey(metric)
		if err != nil {
			return fmt.Errorf("could not generate routing key: %w", err)
		}
		if key != "" {
			m.Key = sarama.StringEncoder(key)
		}

		msgs = append(msgs, m)
	}

	done := make(chan error, 1)
	go func() {
		done <- producer.SendMessages(msgs)
	}()

	var sendErr error
	select {
	case <-ctx.Done():
		// Prefer a result that became available concurrently with cancellation.
		select {
		case sendErr = <-done:
		default:
			k.poisonProducer(producer, generation, done)
			return ctx.Err()
		}
	case err := <-done:
		sendErr = err
	}

	if sendErr != nil {
		// We could have many errors, return only the first encountered.
		var errs sarama.ProducerErrors
		if errors.As(sendErr, &errs) && len(errs) > 0 {
			// Just return the first error encountered
			firstErr := errs[0]
			if errors.Is(firstErr.Err, sarama.ErrMessageSizeTooLarge) {
				k.Log.Error("Message too large, consider increasing `max_message_bytes`; dropping batch")
				return nil
			}
			if errors.Is(firstErr.Err, sarama.ErrInvalidTimestamp) {
				k.Log.Error(
					"The timestamp of the message is out of acceptable range, consider increasing broker `message.timestamp.difference.max.ms`; " +
						"dropping batch",
				)
				return nil
			}
			return firstErr
		}
		return sendErr
	}

	return nil
}

// poisonProducer detaches a producer whose delivery state is unknown. Cleanup
// is best-effort because Sarama's graceful Close can itself wait indefinitely.
func (k *Kafka) poisonProducer(producer sarama.SyncProducer, generation uint64, sendDone <-chan error) {
	k.producerMu.Lock()
	if k.producerGen != generation || k.producer == nil {
		k.producerMu.Unlock()
		return
	}
	k.producer = nil
	k.poisoned++
	k.producerMu.Unlock()

	closeDone := make(chan struct{})
	go func() {
		if err := producer.Close(); err != nil {
			k.Log.Errorf("Error closing cancelled producer: %v", err)
		}
		close(closeDone)
	}()
	go func() {
		<-sendDone
		<-closeDone
		k.producerMu.Lock()
		k.poisoned--
		k.limitLogged = false
		k.producerMu.Unlock()
	}()
}

func (k *Kafka) getTopicName(metric telegraf.Metric) (telegraf.Metric, string) {
	topic := k.Topic
	if k.TopicTag != "" {
		if t, ok := metric.GetTag(k.TopicTag); ok {
			topic = t

			// If excluding the topic tag, a copy is required to avoid modifying
			// the metric buffer.
			if k.ExcludeTopicTag {
				metric = metric.Copy()
				metric.Accept()
				metric.RemoveTag(k.TopicTag)
			}
		}
	}

	var topicName string
	switch k.TopicSuffix.Method {
	case "measurement":
		topicName = topic + k.TopicSuffix.Separator + metric.Name()
	case "tags":
		topicNameComponents := []string{topic}
		for _, tag := range k.TopicSuffix.Keys {
			tagValue := metric.Tags()[tag]
			if tagValue != "" {
				topicNameComponents = append(topicNameComponents, tagValue)
			}
		}
		topicName = strings.Join(topicNameComponents, k.TopicSuffix.Separator)
	default:
		topicName = topic
	}
	return metric, topicName
}

func (k *Kafka) routingKey(metric telegraf.Metric) (string, error) {
	if k.RoutingTag != "" {
		key, ok := metric.GetTag(k.RoutingTag)
		if ok {
			return key, nil
		}
	}

	if k.RoutingKey == "random" {
		u, err := uuid.NewV4()
		if err != nil {
			return "", err
		}
		return u.String(), nil
	}

	return k.RoutingKey, nil
}

func init() {
	outputs.Add("kafka", func() telegraf.Output {
		return &Kafka{
			WriteConfig: kafka.WriteConfig{
				MaxRetry:     3,
				RequiredAcks: -1,
			},
			producerFunc: sarama.NewSyncProducer,
		}
	})
}
