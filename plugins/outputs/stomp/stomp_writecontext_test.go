package stomp

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	gostomp "github.com/go-stomp/stomp"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/serializers/json"
	"github.com/influxdata/telegraf/testutil"
)

// TestConnectContextCancelUnblocksBlockedDial checks that ConnectContext
// returns once ctx is cancelled even though neither net.Dial nor
// stomp.Connect accept a context: the test server accepts the TCP
// connection but never replies, giving a deterministic (non-timing
// dependent) blocked handshake read to cancel against.
func TestConnectContextCancelUnblocksBlockedDial(t *testing.T) {
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

	q := &STOMP{
		Host: listener.Addr().String(),
		Log:  testutil.Logger{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- q.ConnectContext(ctx)
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
	require.Nil(t, q.stomp)
}

// TestWriteContextCancelUnblocksBlockedSend checks that WriteContext
// returns once ctx is cancelled even though stomp.Conn.Send has no
// context support. It sends more metrics than the stomp library's
// internal write-channel capacity (20, unconfigured default) while the
// fake server, after completing the handshake, never reads again: the
// first frame the library's writer goroutine attempts blocks on the
// net.Pipe (which has no internal buffering), the write channel fills
// up behind it, and a subsequent Send() call blocking on that full
// channel gives a deterministic point to cancel against.
func TestWriteContextCancelUnblocksBlockedSend(t *testing.T) {
	clientConn, serverConn := net.Pipe()

	handshakeDone := make(chan struct{})
	go func() {
		defer close(handshakeDone)
		buf := make([]byte, 4096)
		total := 0
		for {
			n, err := serverConn.Read(buf[total:])
			if err != nil {
				return
			}
			total += n
			if bytes.IndexByte(buf[:total], 0) >= 0 {
				break
			}
		}
		if _, err := serverConn.Write([]byte("CONNECTED\nversion:1.2\n\n\x00")); err != nil {
			return
		}
		// Go silent from here: no further reads, so the next frame the
		// client writes blocks on the pipe.
	}()

	sc, err := gostomp.Connect(clientConn)
	require.NoError(t, err)

	select {
	case <-handshakeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handshake did not complete")
	}

	s := &json.Serializer{}
	require.NoError(t, s.Init())

	q := &STOMP{
		QueueName: "test",
		Log:       testutil.Logger{},
		conn:      clientConn,
		stomp:     sc,
		serialize: s,
	}

	metrics := make([]telegraf.Metric, 0, 25)
	for i := 0; i < 25; i++ {
		metrics = append(metrics, metric.New(
			"cpu",
			map[string]string{},
			map[string]interface{}{"value": 3.14},
			time.Unix(0, 0),
		))
	}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- q.WriteContext(ctx, metrics)
	}()

	// Give the writes enough time to fill the internal write channel and
	// reach the blocked send before cancelling.
	time.Sleep(200 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("WriteContext did not return promptly after context cancellation")
	}

	// The connection must be detached so the next write reconnects rather
	// than reusing a stomp.Conn whose internal lock the abandoned Send may
	// still hold.
	require.Nil(t, q.stomp)
	require.Nil(t, q.conn)
}
