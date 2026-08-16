package amqp

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/serializers/influx"
	"github.com/influxdata/telegraf/testutil"
)

// blockingClient blocks in Publish until unblock is closed, giving a
// deterministic point to cancel against instead of racing a real sleep
// against a hang.
type blockingClient struct {
	unblock chan struct{}
	entered chan struct{}

	closed chan struct{}
}

func newBlockingClient() *blockingClient {
	return &blockingClient{
		unblock: make(chan struct{}),
		entered: make(chan struct{}, 1),
		closed:  make(chan struct{}),
	}
}

func (c *blockingClient) Publish(ctx context.Context, _ string, _ []byte) error {
	select {
	case c.entered <- struct{}{}:
	default:
	}
	select {
	case <-c.unblock:
		return nil
	case <-ctx.Done():
		// amqp091-go's PublishWithContext behaves the same way: it does not
		// itself observe cancellation mid-write, only up front. Emulate
		// that by continuing to block here; the plugin's own race in
		// publishClient is what must unblock the caller.
		<-c.unblock
		return ctx.Err()
	}
}

func (c *blockingClient) Close() error {
	close(c.closed)
	return nil
}

// TestWriteContextCancelUnblocksBlockedPublish checks that WriteContext
// returns promptly once ctx is cancelled even though the underlying client's
// Publish call is still blocked, and that the connection is detached so a
// subsequent write reconnects instead of reusing it.
func TestWriteContextCancelUnblocksBlockedPublish(t *testing.T) {
	client := newBlockingClient()

	s := &influx.Serializer{}
	require.NoError(t, s.Init())

	q := &AMQP{
		Log:        testutil.Logger{},
		serializer: s,
		client:     client,
		encoder:    noopEncoder{},
	}

	metrics := []telegraf.Metric{
		metric.New("cpu", map[string]string{}, map[string]interface{}{"value": 1.0}, time.Unix(0, 0)),
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- q.WriteContext(ctx, metrics)
	}()

	// Wait until the blocking Publish has actually been entered before
	// cancelling, so the test deterministically exercises the in-flight
	// cancellation path.
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

	// The client must be detached so the next write reconnects rather than
	// reusing a connection whose in-flight publish outcome is unknown.
	require.Nil(t, q.client)

	// Unblock the abandoned goroutine and confirm it closes the connection
	// in the background.
	close(client.unblock)
	select {
	case <-client.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("abandoned client was never closed")
	}
}

// TestConnectContextCancelUnblocksBlockedConnect checks that ConnectContext
// returns promptly once ctx is cancelled even though the connect function
// blocks, and that any connection eventually produced by the abandoned
// attempt is closed rather than installed.
func TestConnectContextCancelUnblocksBlockedConnect(t *testing.T) {
	release := make(chan struct{})
	client := newBlockingClient()
	q := &AMQP{
		Log: testutil.Logger{},
		connect: func(*ClientConfig) (Client, error) {
			<-release
			return client, nil
		},
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- q.ConnectContext(ctx)
	}()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("ConnectContext did not return promptly after context cancellation")
	}
	require.Nil(t, q.client)

	// Let the abandoned connect finish and confirm the resulting client is
	// closed instead of being installed on the plugin.
	close(release)
	select {
	case <-client.closed:
	case <-time.After(2 * time.Second):
		t.Fatal("abandoned client produced after cancellation was never closed")
	}
	require.Nil(t, q.client)
}

// noopEncoder passes bodies through unchanged.
type noopEncoder struct{}

func (noopEncoder) Encode(body []byte) ([]byte, error) { return body, nil }
