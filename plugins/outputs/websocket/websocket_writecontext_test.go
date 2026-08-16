package websocket

import (
	"bufio"
	"context"
	"crypto/sha1" //nolint:gosec // Required by the WebSocket handshake spec (RFC 6455), not used for security.
	"encoding/base64"
	"net"
	"net/http"
	"testing"
	"time"

	ws "github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/testutil"
)

const websocketAcceptMagic = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

// websocketAcceptKey implements the Sec-WebSocket-Accept computation from
// RFC 6455 section 1.3, needed to hand-roll a minimal server-side handshake
// below.
func websocketAcceptKey(challengeKey string) string {
	h := sha1.New() //nolint:gosec // Required by the WebSocket handshake spec (RFC 6455).
	h.Write([]byte(challengeKey))
	h.Write([]byte(websocketAcceptMagic))
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// pipeClientConn performs a minimal server-side WebSocket handshake over one
// end of a net.Pipe, then deliberately never reads again. Because net.Pipe
// has no internal buffering, the very next client write blocks until
// blockReads is closed -- giving a deterministic (non-timing dependent)
// blocked write to cancel against instead of racing a real sleep against a
// hang.
func pipeClientConn(t *testing.T, blockReads <-chan struct{}) *ws.Conn {
	t.Helper()

	clientConn, serverConn := net.Pipe()

	handshakeDone := make(chan struct{})
	go func() {
		defer close(handshakeDone)
		req, err := http.ReadRequest(bufio.NewReader(serverConn))
		if err != nil {
			return
		}
		accept := websocketAcceptKey(req.Header.Get("Sec-WebSocket-Key"))
		resp := "HTTP/1.1 101 Switching Protocols\r\n" +
			"Upgrade: websocket\r\n" +
			"Connection: Upgrade\r\n" +
			"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
		if _, err := serverConn.Write([]byte(resp)); err != nil {
			return
		}
	}()

	dialer := ws.Dialer{
		NetDial: func(string, string) (net.Conn, error) {
			return clientConn, nil
		},
	}
	conn, resp, err := dialer.Dial("ws://pipe.local/ws", nil)
	require.NoError(t, err)
	_ = resp.Body.Close()

	select {
	case <-handshakeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("handshake did not complete")
	}

	// Close the server side once the test is done blocking reads, so the
	// pipe doesn't leak past the end of the test.
	go func() {
		<-blockReads
		_ = serverConn.Close()
	}()

	return conn
}

// TestWebSocket_WriteContext_CancelUnblocksBlockedWrite checks that
// WriteContext returns promptly once ctx is cancelled even though the
// underlying gorilla Conn.WriteMessage call is still blocked (the fake
// server never reads), and that the connection is detached so the next
// write reconnects rather than reusing one whose in-flight write outcome is
// unknown.
func TestWebSocket_WriteContext_CancelUnblocksBlockedWrite(t *testing.T) {
	release := make(chan struct{})
	defer close(release)

	conn := pipeClientConn(t, release)

	w := newWebSocket()
	w.Log = testutil.Logger{}
	w.conn = conn
	w.SetSerializer(newTestSerializer())

	metrics := []telegraf.Metric{testutil.TestMetric(0.4, "test")}

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- w.WriteContext(ctx, metrics)
	}()

	// Give the write time to reach the blocked state on the pipe before
	// cancelling.
	time.Sleep(100 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("WriteContext did not return promptly after context cancellation")
	}

	require.Nil(t, w.conn)
}

// TestWebSocket_ConnectContext_CancelUnblocksBlockedHandshake checks that
// ConnectContext returns promptly once ctx is cancelled even though the
// underlying dial/handshake is still blocked. The fake server accepts the
// TCP connection but never writes an HTTP response, which -- since
// gorilla's Dialer.DialContext only ever applies ctx.Deadline() up front
// and does not select on ctx.Done() -- would otherwise hang past our plain
// (non-deadline) cancellation.
func TestWebSocket_ConnectContext_CancelUnblocksBlockedHandshake(t *testing.T) {
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

	w := newWebSocket()
	w.Log = testutil.Logger{}
	w.URL = "ws://" + listener.Addr().String() + "/"
	w.SetSerializer(newTestSerializer())

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- w.ConnectContext(ctx)
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
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("ConnectContext did not return promptly after context cancellation")
	}
	require.Nil(t, w.conn)
}
