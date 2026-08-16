//go:generate ../../../tools/readme_config_includer/generator
package amqp

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"strings"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/common/proxy"
	"github.com/influxdata/telegraf/plugins/common/tls"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

const (
	DefaultURL             = "amqp://localhost:5672/influxdb"
	DefaultAuthMethod      = "PLAIN"
	DefaultExchangeType    = "topic"
	DefaultRetentionPolicy = "default"
	DefaultDatabase        = "telegraf"
)

type externalAuth struct{}

func (*externalAuth) Mechanism() string {
	return "EXTERNAL"
}

func (*externalAuth) Response() string {
	return "\000"
}

type AMQP struct {
	Brokers            []string          `toml:"brokers"`
	Exchange           string            `toml:"exchange"`
	ExchangeType       string            `toml:"exchange_type"`
	ExchangePassive    bool              `toml:"exchange_passive"`
	ExchangeDurability string            `toml:"exchange_durability"`
	ExchangeArguments  map[string]string `toml:"exchange_arguments"`
	Username           config.Secret     `toml:"username"`
	Password           config.Secret     `toml:"password"`
	MaxMessages        int               `toml:"max_messages"`
	AuthMethod         string            `toml:"auth_method"`
	RoutingTag         string            `toml:"routing_tag"`
	RoutingKey         string            `toml:"routing_key"`
	DeliveryMode       string            `toml:"delivery_mode"`
	Headers            map[string]string `toml:"headers"`
	Timeout            config.Duration   `toml:"timeout"`
	UseBatchFormat     bool              `toml:"use_batch_format"`
	ContentEncoding    string            `toml:"content_encoding"`
	Log                telegraf.Logger   `toml:"-"`
	tls.ClientConfig
	proxy.TCPProxy

	serializer   telegraf.Serializer
	connect      func(*ClientConfig) (Client, error)
	client       Client
	config       *ClientConfig
	sentMessages int
	encoder      internal.ContentEncoder
}

type Client interface {
	Publish(ctx context.Context, key string, body []byte) error
	Close() error
}

func (*AMQP) SampleConfig() string {
	return sampleConfig
}

func (q *AMQP) SetSerializer(serializer telegraf.Serializer) {
	q.serializer = serializer
}

func (q *AMQP) Init() error {
	var err error
	q.config, err = q.makeClientConfig()
	if err != nil {
		return err
	}

	q.encoder, err = internal.NewContentEncoder(q.ContentEncoding)
	if err != nil {
		return err
	}

	return nil
}

func (q *AMQP) Connect() error {
	return q.ConnectContext(context.Background())
}

// ConnectContext dials the broker and declares the exchange, returning early
// if ctx is cancelled. amqp091-go's DialConfig has no context-aware variant,
// so the whole connect is raced in a goroutine and abandoned on
// cancellation, closing whatever connection it eventually produces.
func (q *AMQP) ConnectContext(ctx context.Context) error {
	type result struct {
		client Client
		err    error
	}
	resultCh := make(chan result, 1)
	go func() {
		c, err := q.connect(q.config)
		resultCh <- result{c, err}
	}()

	select {
	case res := <-resultCh:
		if res.err != nil {
			return res.err
		}
		q.client = res.client
		return nil
	case <-ctx.Done():
		go func() {
			res := <-resultCh
			if res.client != nil {
				res.client.Close()
			}
		}()
		return ctx.Err()
	}
}

func (q *AMQP) Close() error {
	if q.client != nil {
		return q.client.Close()
	}
	return nil
}

func (q *AMQP) routingKey(metric telegraf.Metric) string {
	if q.RoutingTag != "" {
		key, ok := metric.GetTag(q.RoutingTag)
		if ok {
			return key
		}
	}
	return q.RoutingKey
}

func (q *AMQP) Write(metrics []telegraf.Metric) error {
	return q.WriteContext(context.Background(), metrics)
}

// WriteContext publishes the batches to the broker, returning early if ctx
// is cancelled.
func (q *AMQP) WriteContext(ctx context.Context, metrics []telegraf.Metric) error {
	batches := make(map[string][]telegraf.Metric)
	if q.ExchangeType == "header" {
		// Since the routing_key is ignored for this exchange type send as a
		// single batch.
		batches[""] = metrics
	} else {
		for _, metric := range metrics {
			routingKey := q.routingKey(metric)
			if _, ok := batches[routingKey]; !ok {
				batches[routingKey] = make([]telegraf.Metric, 0)
			}

			batches[routingKey] = append(batches[routingKey], metric)
		}
	}

	first := true
	for key, metrics := range batches {
		body, err := q.serialize(metrics)
		if err != nil {
			return err
		}

		body, err = q.encoder.Encode(body)
		if err != nil {
			return err
		}

		err = q.publish(ctx, key, body)
		if err != nil {
			// A cancelled context always takes precedence: report the
			// cancellation rather than retrying or swallowing the error.
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}

			// If this is the first attempt to publish and the connection is
			// closed, try to reconnect and retry once.

			var aerr *amqp.Error
			if first && errors.As(err, &aerr) && errors.Is(aerr, amqp.ErrClosed) {
				q.client = nil
				err := q.publish(ctx, key, body)
				if err != nil {
					if ctxErr := ctx.Err(); ctxErr != nil {
						return ctxErr
					}
					return err
				}
			} else if q.client != nil {
				if err := q.client.Close(); err != nil {
					q.Log.Errorf("Closing connection failed: %v", err)
				}
				q.client = nil
				return err
			}
		}
		first = false
	}

	if q.sentMessages >= q.MaxMessages && q.MaxMessages > 0 && q.client != nil {
		q.Log.Debug("Sent MaxMessages; closing connection")
		if err := q.client.Close(); err != nil {
			q.Log.Errorf("Closing connection failed: %v", err)
		}
		q.client = nil
	}

	return nil
}

func (q *AMQP) publish(ctx context.Context, key string, body []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	if q.client == nil {
		if err := q.ConnectContext(ctx); err != nil {
			return err
		}
		q.sentMessages = 0
	}

	if err := q.publishClient(ctx, q.client, key, body); err != nil {
		return err
	}
	q.sentMessages++
	return nil
}

// publishClient races client.Publish against ctx cancellation. amqp091-go's
// PublishWithContext only checks ctx.Err() up front before calling the
// blocking Publish -- it does not itself abort a write already in flight to
// a broker with, e.g., a full TCP send buffer -- so the whole call is raced
// here. On cancellation the client's delivery state is unknown (it may be
// mid-write) so it is detached from the plugin and closed in the background
// once the abandoned call returns; the next write reconnects instead of
// reusing it.
func (q *AMQP) publishClient(ctx context.Context, client Client, key string, body []byte) error {
	done := make(chan error, 1)
	go func() {
		done <- client.Publish(ctx, key, body)
	}()

	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		q.client = nil
		go func() {
			if err := <-done; err != nil {
				q.Log.Debugf("Publish on abandoned connection failed: %v", err)
			}
			if err := client.Close(); err != nil {
				q.Log.Errorf("Error closing cancelled connection: %v", err)
			}
		}()
		return ctx.Err()
	}
}

func (q *AMQP) serialize(metrics []telegraf.Metric) ([]byte, error) {
	if q.UseBatchFormat {
		return q.serializer.SerializeBatch(metrics)
	}

	var buf bytes.Buffer
	for _, metric := range metrics {
		octets, err := q.serializer.Serialize(metric)
		if err != nil {
			q.Log.Debugf("Could not serialize metric: %v", err)
			continue
		}
		buf.Write(octets)
	}
	body := buf.Bytes()
	return body, nil
}

func (q *AMQP) makeClientConfig() (*ClientConfig, error) {
	clientConfig := &ClientConfig{
		exchange:        q.Exchange,
		exchangeType:    q.ExchangeType,
		exchangePassive: q.ExchangePassive,
		encoding:        q.ContentEncoding,
		timeout:         time.Duration(q.Timeout),
		log:             q.Log,
	}

	switch q.ExchangeDurability {
	case "transient":
		clientConfig.exchangeDurable = false
	default:
		clientConfig.exchangeDurable = true
	}

	clientConfig.brokers = q.Brokers

	switch q.DeliveryMode {
	case "transient":
		clientConfig.deliveryMode = amqp.Transient
	case "persistent":
		clientConfig.deliveryMode = amqp.Persistent
	default:
		clientConfig.deliveryMode = amqp.Transient
	}

	if len(q.Headers) > 0 {
		clientConfig.headers = make(amqp.Table, len(q.Headers))
		for k, v := range q.Headers {
			clientConfig.headers[k] = v
		}
	}

	if len(q.ExchangeArguments) > 0 {
		clientConfig.exchangeArguments = make(amqp.Table, len(q.ExchangeArguments))
		for k, v := range q.ExchangeArguments {
			clientConfig.exchangeArguments[k] = v
		}
	}

	tlsConfig, err := q.ClientConfig.TLSConfig()
	if err != nil {
		return nil, err
	}
	clientConfig.tlsConfig = tlsConfig

	dialer, err := q.TCPProxy.Proxy()
	if err != nil {
		return nil, err
	}
	clientConfig.dialer = dialer

	var auth []amqp.Authentication
	if strings.EqualFold(q.AuthMethod, "EXTERNAL") {
		auth = []amqp.Authentication{&externalAuth{}}
	} else if !q.Username.Empty() || !q.Password.Empty() {
		username, err := q.Username.Get()
		if err != nil {
			return nil, fmt.Errorf("getting username failed: %w", err)
		}
		defer username.Destroy()
		password, err := q.Password.Get()
		if err != nil {
			return nil, fmt.Errorf("getting password failed: %w", err)
		}
		defer password.Destroy()
		auth = []amqp.Authentication{
			&amqp.PlainAuth{
				Username: username.String(),
				Password: password.String(),
			},
		}
	}
	clientConfig.auth = auth

	return clientConfig, nil
}

func connect(clientConfig *ClientConfig) (Client, error) {
	return newClient(clientConfig)
}

func init() {
	outputs.Add("amqp", func() telegraf.Output {
		return &AMQP{
			Brokers:      []string{DefaultURL},
			ExchangeType: DefaultExchangeType,
			AuthMethod:   DefaultAuthMethod,
			Headers: map[string]string{
				"database":         DefaultDatabase,
				"retention_policy": DefaultRetentionPolicy,
			},
			Timeout: config.Duration(time.Second * 5),
			connect: connect,
		}
	})
}
