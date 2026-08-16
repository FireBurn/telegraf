package riemann

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/testutil"
)

// TestWriteContextCancelReturnsPromptly checks that WriteContext returns
// once ctx is cancelled even though raidman.Client cannot itself be
// interrupted: SendMulti writes the message (which fits the OS send
// buffer and returns immediately) and then blocks reading the server's
// response header. The test server accepts the connection but never
// writes a response, giving a deterministic (non-timing-dependent)
// blocked read to cancel against.
func TestWriteContextCancelReturnsPromptly(t *testing.T) {
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

	r := &Riemann{
		URL: "tcp://" + listener.Addr().String(),
		Log: testutil.Logger{},
		// Timeout=0 disables raidman's own SetDeadline call, so the test
		// exercises Telegraf's context-based cancellation rather than
		// racing raidman's internal timeout.
		Timeout: 0,
	}
	require.NoError(t, r.Connect())
	defer r.Close()

	select {
	case conn := <-accepted:
		defer conn.Close()
	case <-time.After(2 * time.Second):
		t.Fatal("server never accepted the connection")
	}

	m := metric.New(
		"cpu",
		map[string]string{"host": "myhost"},
		map[string]interface{}{"value": 3.14},
		time.Unix(0, 0),
	)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- r.WriteContext(ctx, []telegraf.Metric{m})
	}()

	// Give the write a moment to reach the blocked response read before
	// cancelling.
	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("WriteContext did not return promptly after context cancellation")
	}

	// The client must be detached so the next write reconnects rather
	// than reusing a client whose abandoned SendMulti is still pending.
	require.Nil(t, r.client)
}
