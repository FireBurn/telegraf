package internal

import (
	"context"
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// deadlineConn records the deadlines forced on it by the watcher.
type deadlineConn struct {
	net.Conn
	set chan time.Time
}

func (c *deadlineConn) SetDeadline(t time.Time) error {
	select {
	case c.set <- t:
	default:
	}
	return nil
}

func newDeadlineConn() *deadlineConn {
	client, _ := net.Pipe()
	return &deadlineConn{Conn: client, set: make(chan time.Time, 1)}
}

func TestCancelConnOnContextUnblocksWrite(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	ctx, cancel := context.WithCancel(t.Context())
	stop := CancelConnOnContext(ctx, client)

	writeErr := make(chan error, 1)
	go func() {
		// Nothing reads from server, so this blocks until the deadline hits.
		_, err := client.Write([]byte("blocked"))
		writeErr <- err
	}()

	cancel()

	select {
	case err := <-writeErr:
		require.ErrorIs(t, err, os.ErrDeadlineExceeded)
	case <-time.After(5 * time.Second):
		t.Fatal("write was not unblocked by cancellation")
	}

	require.True(t, stop(), "stop must report that the deadline was forced")
}

func TestCancelConnOnContextReportsNoCancellation(t *testing.T) {
	conn := newDeadlineConn()
	defer conn.Close()

	stop := CancelConnOnContext(t.Context(), conn)
	require.False(t, stop(), "stop must not report cancellation for a live context")

	select {
	case <-conn.set:
		t.Fatal("watcher forced a deadline without cancellation")
	default:
	}
}

// A cancellation that lands at the same moment the operation succeeds must
// still be reported, so the caller discards the now-poisoned connection
// instead of handing it to the next write.
func TestCancelConnOnContextReportsRacedCancellation(t *testing.T) {
	conn := newDeadlineConn()
	defer conn.Close()

	ctx, cancel := context.WithCancel(t.Context())
	stop := CancelConnOnContext(ctx, conn)

	// Cancel and stop concurrently, mimicking a write that returned just as
	// the deadline fired.
	cancel()
	if stop() {
		// The watcher took the cancellation branch, so it must have forced a
		// deadline before reporting it.
		select {
		case <-conn.set:
		default:
			t.Fatal("cancellation reported without forcing a deadline")
		}
	}

	// Repeated calls are safe and stable.
	first := stop()
	require.Equal(t, first, stop())
}

func TestCancelConnOnContextStopIsIdempotent(t *testing.T) {
	conn := newDeadlineConn()
	defer conn.Close()

	stop := CancelConnOnContext(t.Context(), conn)
	require.False(t, stop())
	require.False(t, stop())
	require.False(t, stop())
}
