//go:generate ../../../tools/readme_config_includer/generator
package quix

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/internal"
	common_http "github.com/influxdata/telegraf/plugins/common/http"
	common_kafka "github.com/influxdata/telegraf/plugins/common/kafka"
	"github.com/influxdata/telegraf/plugins/outputs"
	"github.com/influxdata/telegraf/plugins/serializers/json"
)

//go:embed sample.conf
var sampleConfig string

// Allow one replacement attempt while cleanup of the original producer is
// stuck, but do not accumulate producers indefinitely on repeated timeouts.
const (
	maxPoisonedProducers = 2
	defaultCloseTimeout  = 5 * time.Second
)

type Quix struct {
	APIURL    string          `toml:"url"`
	Workspace string          `toml:"workspace"`
	Topic     string          `toml:"topic"`
	Token     config.Secret   `toml:"token"`
	Log       telegraf.Logger `toml:"-"`
	common_http.HTTPClientConfig

	// producerFunc constructs the underlying Kafka producer. It is a field
	// (rather than a direct call to sarama.NewSyncProducer) so tests can
	// substitute a fake that never dials.
	producerFunc func(addrs []string, config *sarama.Config) (sarama.SyncProducer, error)

	producerMu   sync.Mutex
	producer     sarama.SyncProducer
	producerGen  uint64
	attempt      *producerAttempt
	poisoned     int
	limitLogged  bool
	closed       bool
	closeTimeout time.Duration

	serializer telegraf.Serializer
	kakfaTopic string
}

type producerAttempt struct {
	done       chan struct{}
	producer   sarama.SyncProducer
	generation uint64
	err        error
}

func (*Quix) SampleConfig() string {
	return sampleConfig
}

func (q *Quix) Init() error {
	// Set defaults
	if q.APIURL == "" {
		q.APIURL = "https://portal-api.platform.quix.io"
	}
	q.APIURL = strings.TrimSuffix(q.APIURL, "/")

	// Check input parameters
	if q.Topic == "" {
		return errors.New("option 'topic' must be set")
	}
	if q.Workspace == "" {
		return errors.New("option 'workspace' must be set")
	}
	if q.Token.Empty() {
		return errors.New("option 'token' must be set")
	}
	q.kakfaTopic = q.Workspace + "-" + q.Topic

	if q.producerFunc == nil {
		q.producerFunc = sarama.NewSyncProducer
	}

	// Create a JSON serializer for the output
	q.serializer = &json.Serializer{
		TimestampUnits: config.Duration(time.Nanosecond), // Hardcoded nanoseconds precision
	}

	return nil
}

func (q *Quix) Connect() error {
	return q.ConnectContext(context.Background())
}

func (q *Quix) ConnectContext(ctx context.Context) error {
	_, _, err := q.acquireProducer(ctx)
	return err
}

func (q *Quix) Close() error {
	q.producerMu.Lock()
	q.closed = true
	producer := q.producer
	q.producer = nil
	q.producerMu.Unlock()
	if producer == nil {
		return nil
	}

	done := make(chan error, 1)
	go func() {
		done <- producer.Close()
	}()
	timeout := q.producerCloseTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case err := <-done:
		return err
	case <-timer.C:
		return fmt.Errorf("closing Quix producer timed out after %s", timeout)
	}
}

// acquireProducer returns the current live producer if one exists, or races
// a shared single-flight creation attempt against ctx.Done(). This mirrors
// outputs.kafka's acquireProducer/createProducer/disposeProducer pattern.
func (q *Quix) acquireProducer(ctx context.Context) (sarama.SyncProducer, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	q.producerMu.Lock()
	if q.closed {
		q.producerMu.Unlock()
		return nil, 0, errors.New("quix output is closed")
	}
	if q.producer != nil {
		producer, generation := q.producer, q.producerGen
		q.producerMu.Unlock()
		return producer, generation, nil
	}
	if q.poisoned >= maxPoisonedProducers {
		if !q.limitLogged {
			q.Log.Errorf("Quix producer replacement limit reached; refusing to create another producer because previous producers are still shutting down")
			q.limitLogged = true
		}
		q.producerMu.Unlock()
		return nil, 0, errors.New("quix producer replacement limit reached while previous producers are still shutting down")
	}

	attempt := q.attempt
	startAttempt := false
	if attempt == nil {
		attempt = &producerAttempt{done: make(chan struct{})}
		q.attempt = attempt
		startAttempt = true
	}
	q.producerMu.Unlock()
	if startAttempt {
		go q.createProducer(attempt)
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

// createProducer runs the whole "fetch broker config, dial producer"
// sequence in the background on behalf of whichever caller started the
// single-flight attempt. Neither step is context-aware: the HTTP client
// used to fetch the broker config could accept one natively, but this is a
// *shared* attempt -- tying it to one particular caller's context would let
// that caller's timeout abort work that other concurrent callers are still
// waiting on. So, like outputs.kafka's createProducer, this always runs to
// completion; callers only bound their own wait via acquireProducer.
func (q *Quix) createProducer(attempt *producerAttempt) {
	producer, err := q.newProducer()

	q.producerMu.Lock()
	install := err == nil && !q.closed && q.attempt == attempt && q.producer == nil
	if install {
		q.producer = producer
		q.producerGen++
		attempt.producer = producer
		attempt.generation = q.producerGen
	} else if err != nil {
		attempt.err = &internal.StartupError{Err: err, Retry: true}
	} else {
		attempt.err = errors.New("quix producer creation result is no longer usable")
	}
	if q.attempt == attempt {
		q.attempt = nil
	}
	close(attempt.done)
	q.producerMu.Unlock()

	if producer != nil && !install {
		q.disposeProducer(producer)
	}
}

func (q *Quix) newProducer() (sarama.SyncProducer, error) {
	quixConfig, err := q.fetchBrokerConfig()
	if err != nil {
		return nil, fmt.Errorf("fetching broker config failed: %w", err)
	}
	brokers := strings.Split(quixConfig.BootstrapServers, ",")
	if len(brokers) == 0 {
		return nil, errors.New("no brokers received")
	}

	cfg, err := q.buildSaramaConfig(quixConfig)
	if err != nil {
		return nil, err
	}

	return q.producerFunc(brokers, cfg)
}

func (q *Quix) buildSaramaConfig(quixConfig *brokerConfig) (*sarama.Config, error) {
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true

	switch quixConfig.SecurityProtocol {
	case "SASL_SSL":
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.User = quixConfig.SaslUsername
		cfg.Net.SASL.Password = quixConfig.SaslPassword
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &common_kafka.XDGSCRAMClient{HashGeneratorFcn: common_kafka.SHA256}
		}

		switch quixConfig.SaslMechanism {
		case "SCRAM-SHA-512":
			cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &common_kafka.XDGSCRAMClient{HashGeneratorFcn: common_kafka.SHA512}
			}
			cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		case "SCRAM-SHA-256":
			cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
			cfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &common_kafka.XDGSCRAMClient{HashGeneratorFcn: common_kafka.SHA256}
			}
		case "PLAIN":
			cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		default:
			return nil, fmt.Errorf("unsupported SASL mechanism: %s", quixConfig.SaslMechanism)
		}

		cfg.Net.TLS.Enable = true

		// Add the CA certificate sent by the server if there is any. Newer
		// cloud instances do not need this and we can go with the system
		// certificates.
		if len(quixConfig.cert) > 0 {
			certPool := x509.NewCertPool()
			if !certPool.AppendCertsFromPEM(quixConfig.cert) {
				return nil, errors.New("appending CA cert to pool failed")
			}
			cfg.Net.TLS.Config = &tls.Config{RootCAs: certPool}
		}
	case "PLAINTEXT":
		// No additional configuration required for plaintext communication
	default:
		return nil, fmt.Errorf("unsupported security protocol: %s", quixConfig.SecurityProtocol)
	}

	return cfg, nil
}

func (q *Quix) disposeProducer(producer sarama.SyncProducer) {
	done := make(chan error, 1)
	go func() {
		done <- producer.Close()
	}()
	timer := time.NewTimer(q.producerCloseTimeout())
	defer timer.Stop()
	select {
	case err := <-done:
		if err != nil {
			q.Log.Errorf("Error closing unused Quix producer: %v", err)
		}
	case <-timer.C:
		q.Log.Errorf("Timed out closing unused Quix producer; abandoning it")
	}
}

func (q *Quix) producerCloseTimeout() time.Duration {
	if q.closeTimeout > 0 {
		return q.closeTimeout
	}
	return defaultCloseTimeout
}

// Write writes the metrics to Quix. It ignores context cancellation.
func (q *Quix) Write(metrics []telegraf.Metric) error {
	return q.WriteContext(context.Background(), metrics)
}

// WriteContext writes the metrics to Quix. It can be cancelled via the
// context. sarama's SyncProducer.SendMessage has no context support, so
// each send is raced in a goroutine; on cancellation the producer that
// owned the in-flight send is detached (poisoned) and closed in the
// background, and the next write creates a fresh one.
func (q *Quix) WriteContext(ctx context.Context, metrics []telegraf.Metric) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	producer, generation, err := q.acquireProducer(ctx)
	if err != nil {
		return err
	}

	for _, m := range metrics {
		serialized, err := q.serializer.Serialize(m)
		if err != nil {
			q.Log.Errorf("Error serializing metric: %v", err)
			continue
		}

		msg := &sarama.ProducerMessage{
			Topic:     q.kakfaTopic,
			Value:     sarama.ByteEncoder(serialized),
			Timestamp: m.Time(),
			Key:       sarama.StringEncoder("telegraf"),
		}

		done := make(chan error, 1)
		go func() {
			_, _, sendErr := producer.SendMessage(msg)
			done <- sendErr
		}()

		select {
		case sendErr := <-done:
			if sendErr != nil {
				q.Log.Errorf("Error sending message to Kafka: %v", sendErr)
			}
		case <-ctx.Done():
			q.poisonProducer(producer, generation, done)
			return ctx.Err()
		}
	}

	return nil
}

// poisonProducer detaches a producer whose delivery state is unknown.
// Cleanup is best-effort because Sarama's graceful Close can itself wait
// indefinitely.
func (q *Quix) poisonProducer(producer sarama.SyncProducer, generation uint64, sendDone <-chan error) {
	q.producerMu.Lock()
	if q.producerGen != generation || q.producer == nil {
		q.producerMu.Unlock()
		return
	}
	q.producer = nil
	q.poisoned++
	q.producerMu.Unlock()

	closeDone := make(chan struct{})
	go func() {
		if err := producer.Close(); err != nil {
			q.Log.Errorf("Error closing cancelled producer: %v", err)
		}
		close(closeDone)
	}()
	go func() {
		<-sendDone
		<-closeDone
		q.producerMu.Lock()
		q.poisoned--
		q.limitLogged = false
		q.producerMu.Unlock()
	}()
}

func init() {
	outputs.Add("quix", func() telegraf.Output {
		return &Quix{producerFunc: sarama.NewSyncProducer}
	})
}
