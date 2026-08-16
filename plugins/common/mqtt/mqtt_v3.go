package mqtt

import (
	"context"
	"fmt"
	"time"

	mqttv3 "github.com/eclipse/paho.mqtt.golang" // Library that supports v3.1.1

	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/logger"
)

type mqttv311Client struct {
	client  mqttv3.Client
	timeout time.Duration
	qos     int
	retain  bool
}

func NewMQTTv311Client(cfg *MqttConfig) (*mqttv311Client, error) {
	opts := mqttv3.NewClientOptions()
	opts.KeepAlive = cfg.KeepAlive
	opts.WriteTimeout = time.Duration(cfg.Timeout)
	if time.Duration(cfg.ConnectionTimeout) >= 1*time.Second {
		opts.ConnectTimeout = time.Duration(cfg.ConnectionTimeout)
	}
	opts.SetCleanSession(!cfg.PersistentSession)
	if cfg.OnConnectionLost != nil {
		onConnectionLost := func(_ mqttv3.Client, err error) {
			cfg.OnConnectionLost(err)
		}
		opts.SetConnectionLostHandler(onConnectionLost)
	}
	opts.SetAutoReconnect(cfg.AutoReconnect)

	if cfg.ClientID != "" {
		opts.SetClientID(cfg.ClientID)
	} else {
		id, err := internal.RandomString(5)
		if err != nil {
			return nil, fmt.Errorf("generating random client ID failed: %w", err)
		}
		opts.SetClientID("Telegraf-Output-" + id)
	}

	tlsCfg, err := cfg.ClientConfig.TLSConfig()
	if err != nil {
		return nil, err
	}
	opts.SetTLSConfig(tlsCfg)

	if !cfg.Username.Empty() {
		user, err := cfg.Username.Get()
		if err != nil {
			return nil, fmt.Errorf("getting username failed: %w", err)
		}
		opts.SetUsername(user.String())
		user.Destroy()
	}
	if !cfg.Password.Empty() {
		password, err := cfg.Password.Get()
		if err != nil {
			return nil, fmt.Errorf("getting password failed: %w", err)
		}
		opts.SetPassword(password.String())
		password.Destroy()
	}

	servers, err := parseServers(cfg.Servers)
	if err != nil {
		return nil, err
	}
	for _, server := range servers {
		if tlsCfg != nil {
			server.Scheme = "tls"
		}
		broker := server.String()
		opts.AddBroker(broker)
	}

	if cfg.ClientTrace {
		log := &mqttLogger{logger.New("paho", "", "")}
		mqttv3.ERROR = log
		mqttv3.CRITICAL = log
		mqttv3.WARN = log
		mqttv3.DEBUG = log
	}

	return &mqttv311Client{
		client:  mqttv3.NewClient(opts),
		timeout: time.Duration(cfg.Timeout),
		qos:     cfg.QoS,
		retain:  cfg.Retain,
	}, nil
}

func (m *mqttv311Client) Connect() (bool, error) {
	token := m.client.Connect()

	if token.Wait() && token.Error() != nil {
		return false, token.Error()
	}

	// Persistent sessions should skip subscription if a session is present, as
	// the subscriptions are stored by the server.
	type sessionPresent interface {
		SessionPresent() bool
	}
	if t, ok := token.(sessionPresent); ok {
		return t.SessionPresent(), nil
	}

	return false, nil
}

// Publish sends body to topic, returning promptly once ctx is cancelled.
// paho.mqtt.golang's Client.Publish has no context support and can itself
// block (bounded internally by WriteTimeout, default 30s) queuing the
// message before it even returns a Token, so that call is raced in a
// goroutine; the wait for the token to complete (broker ack) is then raced
// again. Neither race requires detaching/closing the connection on
// cancellation: Publish is safe to have in flight concurrently with other
// publishes on the same client, and an abandoned call/token is cleaned up
// by the client's own internal bookkeeping once it eventually completes.
func (m *mqttv311Client) Publish(ctx context.Context, topic string, body []byte) error {
	tokenCh := make(chan mqttv3.Token, 1)
	go func() {
		tokenCh <- m.client.Publish(topic, byte(m.qos), m.retain, body)
	}()

	select {
	case token := <-tokenCh:
		return m.waitToken(ctx, token)
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *mqttv311Client) waitToken(ctx context.Context, token mqttv3.Token) error {
	var timeoutCh <-chan time.Time
	if m.timeout > 0 {
		timer := time.NewTimer(m.timeout)
		defer timer.Stop()
		timeoutCh = timer.C
	}

	select {
	case <-token.Done():
		return token.Error()
	case <-ctx.Done():
		return ctx.Err()
	case <-timeoutCh:
		return internal.ErrTimeout
	}
}

func (m *mqttv311Client) SubscribeMultiple(filters map[string]byte, callback mqttv3.MessageHandler) error {
	token := m.client.SubscribeMultiple(filters, callback)
	token.Wait()
	return token.Error()
}

func (m *mqttv311Client) AddRoute(topic string, callback mqttv3.MessageHandler) {
	m.client.AddRoute(topic, callback)
}

func (m *mqttv311Client) Close() error {
	if m.client.IsConnected() {
		m.client.Disconnect(100)
	}
	return nil
}
