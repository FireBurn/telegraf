package mqtt

import (
	"context"
	"net"
	"testing"
	"time"

	paho "github.com/eclipse/paho.mqtt.golang"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/common/mqtt"
	serializers_influx "github.com/influxdata/telegraf/plugins/serializers/influx"
	"github.com/influxdata/telegraf/testutil"
)

// fakeClient implements mqtt.Client. Publish blocks until unblock is
// closed, giving a deterministic point to cancel against instead of racing
// a real sleep against a hang.
type fakeClient struct {
	unblock chan struct{}
	entered chan struct{}
	closed  chan struct{}
}

func newFakeClient() *fakeClient {
	return &fakeClient{
		unblock: make(chan struct{}),
		entered: make(chan struct{}, 1),
		closed:  make(chan struct{}),
	}
}

func (*fakeClient) Connect() (bool, error) { return false, nil }

// Publish blocks until unblock is closed or ctx is cancelled, mirroring the
// real mqtt.Client implementations: they race the broker-ack wait against
// ctx.Done() and return immediately on cancellation without waiting for
// that wait to actually resolve.
func (c *fakeClient) Publish(ctx context.Context, _ string, _ []byte) error {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	select {
	case <-c.unblock:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (*fakeClient) SubscribeMultiple(map[string]byte, paho.MessageHandler) error { return nil }
func (*fakeClient) AddRoute(string, paho.MessageHandler)                         {}

func (c *fakeClient) Close() error {
	close(c.closed)
	return nil
}

// TestWriteContextCancelUnblocksBlockedPublish checks that WriteContext
// returns promptly once ctx is cancelled even though the underlying
// mqtt.Client's Publish call is still blocked (e.g. waiting on a broker ack
// that never arrives).
func TestWriteContextCancelUnblocksBlockedPublish(t *testing.T) {
	client := newFakeClient()

	s := &serializers_influx.Serializer{}
	require.NoError(t, s.Init())

	m := &MQTT{
		Topic:  "test/{{.Name}}",
		Layout: "non-batch",
		Log:    testutil.Logger{},
		MqttConfig: mqtt.MqttConfig{
			Servers: []string{"tcp://127.0.0.1:1"},
		},
		client: client,
	}
	require.NoError(t, m.Init())
	m.SetSerializer(s)

	metrics := []telegraf.Metric{
		metric.New("cpu", map[string]string{}, map[string]interface{}{"value": 1.0}, time.Unix(0, 0)),
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- m.WriteContext(ctx, metrics)
	}()

	select {
	case <-client.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish was never entered")
	}

	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("WriteContext did not return promptly after context cancellation")
	}

	close(client.unblock)
}

// TestConnectContextCancelUnblocksBlockedConnect checks that ConnectContext
// returns promptly once ctx is cancelled even though the v3 client's
// Connect blocks on token.Wait() with no timeout: the test server accepts
// the TCP connection but never sends CONNACK, giving a deterministic
// (non-timing dependent) blocked handshake to cancel against.
func TestConnectContextCancelUnblocksBlockedConnect(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer listener.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := listener.Accept()
		if err == nil {
			accepted <- conn
		}
	}()

	m := &MQTT{
		Topic:  "test/{{.Name}}",
		Layout: "non-batch",
		Log:    testutil.Logger{},
		MqttConfig: mqtt.MqttConfig{
			Servers: []string{"tcp://" + listener.Addr().String()},
		},
	}
	require.NoError(t, m.Init())

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- m.ConnectContext(ctx)
	}()

	select {
	case conn := <-accepted:
		defer conn.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("server never accepted the connection")
	}

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("ConnectContext did not return promptly after context cancellation")
	}
	require.Nil(t, m.client)
}
