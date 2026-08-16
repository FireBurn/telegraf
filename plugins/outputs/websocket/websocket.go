//go:generate ../../../tools/readme_config_includer/generator
package websocket

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"time"

	ws "github.com/gorilla/websocket"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/common/proxy"
	"github.com/influxdata/telegraf/plugins/common/tls"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

const (
	defaultConnectTimeout = 30 * time.Second
	defaultWriteTimeout   = 30 * time.Second
	defaultReadTimeout    = 30 * time.Second
)

// WebSocket can output to WebSocket endpoint.
type WebSocket struct {
	URL            string                    `toml:"url"`
	ConnectTimeout config.Duration           `toml:"connect_timeout"`
	WriteTimeout   config.Duration           `toml:"write_timeout"`
	ReadTimeout    config.Duration           `toml:"read_timeout"`
	Headers        map[string]*config.Secret `toml:"headers"`
	UseTextFrames  bool                      `toml:"use_text_frames"`
	Log            telegraf.Logger           `toml:"-"`
	proxy.HTTPProxy
	proxy.Socks5ProxyConfig
	tls.ClientConfig

	conn       *ws.Conn
	serializer telegraf.Serializer
}

func (*WebSocket) SampleConfig() string {
	return sampleConfig
}

// SetSerializer implements serializers.SerializerOutput.
func (w *WebSocket) SetSerializer(serializer telegraf.Serializer) {
	w.serializer = serializer
}

var errInvalidURL = errors.New("invalid websocket URL")

// Init the output plugin.
func (w *WebSocket) Init() error {
	if parsedURL, err := url.Parse(w.URL); err != nil || (parsedURL.Scheme != "ws" && parsedURL.Scheme != "wss") {
		return fmt.Errorf("%w: %q", errInvalidURL, w.URL)
	}
	return nil
}

// Connect to the output endpoint.
func (w *WebSocket) Connect() error {
	return w.ConnectContext(context.Background())
}

// ConnectContext dials and performs the websocket handshake, aborting early
// if ctx is cancelled. gorilla/websocket's Dialer.DialContext accepts a
// context and is passed it directly (it applies ctx.Deadline(), if any, as
// the connection deadline for the dial and handshake), but it only ever
// consults ctx.Deadline() once up front -- it does not select on ctx.Done()
// -- so a plain cancellation with no deadline (e.g. agent shutdown) would
// not be honored by the library alone. The whole call is therefore also
// raced in a goroutine and abandoned on cancellation, closing whatever
// connection it eventually produces instead of installing it.
func (w *WebSocket) ConnectContext(ctx context.Context) error {
	tlsCfg, err := w.ClientConfig.TLSConfig()
	if err != nil {
		return fmt.Errorf("error creating TLS config: %w", err)
	}

	dialProxy, err := w.HTTPProxy.Proxy()
	if err != nil {
		return fmt.Errorf("error creating proxy: %w", err)
	}

	dialer := &ws.Dialer{
		Proxy:            dialProxy,
		HandshakeTimeout: time.Duration(w.ConnectTimeout),
		TLSClientConfig:  tlsCfg,
	}

	if w.Socks5ProxyEnabled {
		netDialer, err := w.Socks5ProxyConfig.GetDialer()
		if err != nil {
			return fmt.Errorf("error connecting to socks5 proxy: %w", err)
		}
		dialer.NetDial = netDialer.Dial
	}

	headers := http.Header{}
	for k, v := range w.Headers {
		secret, err := v.Get()
		if err != nil {
			return fmt.Errorf("getting header secret %q failed: %w", k, err)
		}

		headers.Set(k, secret.String())
		secret.Destroy()
	}

	type result struct {
		conn *ws.Conn
		resp *http.Response
		err  error
	}
	resultCh := make(chan result, 1)
	go func() {
		conn, resp, err := dialer.DialContext(ctx, w.URL, headers)
		resultCh <- result{conn, resp, err}
	}()

	select {
	case res := <-resultCh:
		if res.err != nil {
			return fmt.Errorf("error dial: %w", res.err)
		}
		_ = res.resp.Body.Close()
		if res.resp.StatusCode != http.StatusSwitchingProtocols {
			return fmt.Errorf("wrong status code while connecting to server: %d", res.resp.StatusCode)
		}
		w.conn = res.conn
		go w.read(res.conn)
		return nil
	case <-ctx.Done():
		go func() {
			res := <-resultCh
			if res.conn != nil {
				res.conn.Close()
			}
		}()
		return ctx.Err()
	}
}

func (w *WebSocket) read(conn *ws.Conn) {
	defer func() { _ = conn.Close() }()
	if w.ReadTimeout > 0 {
		if err := conn.SetReadDeadline(time.Now().Add(time.Duration(w.ReadTimeout))); err != nil {
			w.Log.Errorf("error setting read deadline: %v", err)
			return
		}
		conn.SetPingHandler(func(string) error {
			err := conn.SetReadDeadline(time.Now().Add(time.Duration(w.ReadTimeout)))
			if err != nil {
				w.Log.Errorf("error setting read deadline: %v", err)
				return err
			}
			return conn.WriteControl(ws.PongMessage, nil, time.Now().Add(time.Duration(w.WriteTimeout)))
		})
	}
	for {
		// Need to read a connection (to properly process pings from a server).
		_, _, err := conn.ReadMessage()
		if err != nil {
			// Websocket connection is not readable after first error, it's going to error state.
			// In the beginning of this goroutine we have defer section that closes such connection.
			// After that connection will be tried to reestablish on next Write.
			if ws.IsUnexpectedCloseError(err, ws.CloseGoingAway, ws.CloseAbnormalClosure) {
				w.Log.Errorf("error reading websocket connection: %v", err)
			}
			return
		}
		if w.ReadTimeout > 0 {
			if err := conn.SetReadDeadline(time.Now().Add(time.Duration(w.ReadTimeout))); err != nil {
				return
			}
		}
	}
}

// Write writes the given metrics to the destination. Not thread-safe.
func (w *WebSocket) Write(metrics []telegraf.Metric) error {
	return w.WriteContext(context.Background(), metrics)
}

// WriteContext writes the given metrics to the destination, aborting an
// in-flight write via closing the connection when ctx is cancelled.
// gorilla's Conn.WriteMessage has no native context support, and unlike a
// raw net.Conn, gorilla only documents Close (and WriteControl) as safe to
// call concurrently with an in-progress WriteMessage from another
// goroutine -- calling SetWriteDeadline concurrently races with
// WriteMessage's internal state -- so cancellation is implemented by
// closing the connection rather than forcing its deadline.
func (w *WebSocket) WriteContext(ctx context.Context, metrics []telegraf.Metric) error {
	if w.conn == nil {
		// Previous write failed with error and ws conn was closed.
		if err := w.ConnectContext(ctx); err != nil {
			return err
		}
	}

	messageData, err := w.serializer.SerializeBatch(metrics)
	if err != nil {
		return err
	}

	if w.WriteTimeout > 0 {
		if err := w.conn.SetWriteDeadline(time.Now().Add(time.Duration(w.WriteTimeout))); err != nil {
			return fmt.Errorf("error setting write deadline: %w", err)
		}
	}

	// Force the write to abort by closing the connection if ctx is
	// cancelled. The connection is captured in a local so the watcher never
	// touches w.conn after it may have been reset by a concurrent caller.
	conn := w.conn
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			conn.Close()
		case <-done:
		}
	}()

	messageType := ws.BinaryMessage
	if w.UseTextFrames {
		messageType = ws.TextMessage
	}
	err = conn.WriteMessage(messageType, messageData)
	close(done)
	if err != nil {
		_ = conn.Close()
		w.conn = nil
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("error writing to connection: %w", err)
	}
	return nil
}

// Close closes the connection. Noop if already closed.
func (w *WebSocket) Close() error {
	if w.conn == nil {
		return nil
	}
	err := w.conn.Close()
	w.conn = nil
	return err
}

func newWebSocket() *WebSocket {
	return &WebSocket{
		ConnectTimeout: config.Duration(defaultConnectTimeout),
		WriteTimeout:   config.Duration(defaultWriteTimeout),
		ReadTimeout:    config.Duration(defaultReadTimeout),
	}
}

func init() {
	outputs.Add("websocket", func() telegraf.Output {
		return newWebSocket()
	})
}
