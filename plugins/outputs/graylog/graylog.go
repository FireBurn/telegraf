//go:generate ../../../tools/readme_config_includer/generator
package graylog

import (
	"bytes"
	"compress/zlib"
	"context"
	"crypto/rand"
	"crypto/tls"
	_ "embed"
	"encoding/binary"
	ejson "encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/internal"
	common_tls "github.com/influxdata/telegraf/plugins/common/tls"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

const (
	defaultEndpoint         = "127.0.0.1:12201"
	defaultConnection       = "wan"
	defaultMaxChunkSizeWan  = 1420
	defaultMaxChunkSizeLan  = 8154
	defaultScheme           = "udp"
	defaultTimeout          = 5 * time.Second
	defaultReconnectionTime = 15 * time.Second
)

var defaultSpecFields = []string{"version", "host", "short_message", "full_message", "timestamp", "level", "facility", "line", "file"}

type gelfConfig struct {
	Endpoint        string
	Connection      string
	MaxChunkSizeWan int
	MaxChunkSizeLan int
}

type gelf interface {
	io.WriteCloser
	ConnectContext(ctx context.Context) error
	WriteContext(ctx context.Context, message []byte) error
}

type gelfCommon struct {
	gelfConfig
	dialer *net.Dialer
	conn   net.Conn
}

type gelfUDP struct {
	gelfCommon
}

type gelfTCP struct {
	gelfCommon
	tlsConfig *tls.Config
}

func newGelfWriter(cfg gelfConfig, dialer *net.Dialer, tlsConfig *tls.Config) gelf {
	if cfg.Endpoint == "" {
		cfg.Endpoint = defaultEndpoint
	}

	if cfg.Connection == "" {
		cfg.Connection = defaultConnection
	}

	if cfg.MaxChunkSizeWan == 0 {
		cfg.MaxChunkSizeWan = defaultMaxChunkSizeWan
	}

	if cfg.MaxChunkSizeLan == 0 {
		cfg.MaxChunkSizeLan = defaultMaxChunkSizeLan
	}

	scheme := defaultScheme
	parts := strings.SplitN(cfg.Endpoint, "://", 2)
	if len(parts) == 2 {
		scheme = strings.ToLower(parts[0])
		cfg.Endpoint = parts[1]
	}
	common := gelfCommon{
		gelfConfig: cfg,
		dialer:     dialer,
	}

	var g gelf
	switch scheme {
	case "tcp":
		g = &gelfTCP{gelfCommon: common, tlsConfig: tlsConfig}
	default:
		g = &gelfUDP{gelfCommon: common}
	}

	return g
}

func (g *gelfUDP) Write(message []byte) (n int, err error) {
	if err := g.WriteContext(context.Background(), message); err != nil {
		return 0, err
	}
	return len(message), nil
}

func (g *gelfUDP) WriteContext(ctx context.Context, message []byte) error {
	compressed, err := g.compress(message)
	if err != nil {
		return err
	}

	chunksize := g.gelfConfig.MaxChunkSizeWan
	length := compressed.Len()

	if length > chunksize {
		chunkCountInt := int(math.Ceil(float64(length) / float64(chunksize)))

		id := make([]byte, 8)
		_, err = rand.Read(id)
		if err != nil {
			return err
		}

		for i, index := 0, 0; i < length; i, index = i+chunksize, index+1 {
			packet, err := g.createChunkedMessage(index, chunkCountInt, id, &compressed)
			if err != nil {
				return err
			}

			if err := g.send(ctx, packet.Bytes()); err != nil {
				return err
			}
		}
	} else if err := g.send(ctx, compressed.Bytes()); err != nil {
		return err
	}

	return nil
}

func (g *gelfUDP) Close() (err error) {
	if g.conn != nil {
		err = g.conn.Close()
		g.conn = nil
	}

	return err
}

func (g *gelfUDP) createChunkedMessage(index, chunkCountInt int, id []byte, compressed *bytes.Buffer) (bytes.Buffer, error) {
	var packet bytes.Buffer

	chunksize := g.getChunksize()

	b, err := g.intToBytes(30)
	if err != nil {
		return packet, err
	}
	packet.Write(b)

	b, err = g.intToBytes(15)
	if err != nil {
		return packet, err
	}
	packet.Write(b)

	packet.Write(id)

	b, err = g.intToBytes(index)
	if err != nil {
		return packet, err
	}
	packet.Write(b)

	b, err = g.intToBytes(chunkCountInt)
	if err != nil {
		return packet, err
	}
	packet.Write(b)

	packet.Write(compressed.Next(chunksize))

	return packet, nil
}

func (g *gelfUDP) getChunksize() int {
	if g.gelfConfig.Connection == "wan" {
		return g.gelfConfig.MaxChunkSizeWan
	}

	if g.gelfConfig.Connection == "lan" {
		return g.gelfConfig.MaxChunkSizeLan
	}

	return g.gelfConfig.MaxChunkSizeWan
}

func (*gelfUDP) intToBytes(i int) ([]byte, error) {
	buf := new(bytes.Buffer)

	err := binary.Write(buf, binary.LittleEndian, int8(i))
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), err
}

func (*gelfUDP) compress(b []byte) (bytes.Buffer, error) {
	var buf bytes.Buffer
	comp := zlib.NewWriter(&buf)

	if _, err := comp.Write(b); err != nil {
		return bytes.Buffer{}, err
	}

	if err := comp.Close(); err != nil {
		return bytes.Buffer{}, err
	}

	return buf, nil
}

func (g *gelfUDP) Connect() error {
	return g.ConnectContext(context.Background())
}

func (g *gelfUDP) ConnectContext(ctx context.Context) error {
	conn, err := g.dialer.DialContext(ctx, "udp", g.gelfConfig.Endpoint)
	if err != nil {
		return err
	}
	g.conn = conn
	return nil
}

func (g *gelfUDP) send(ctx context.Context, b []byte) error {
	if g.conn == nil {
		if err := g.ConnectContext(ctx); err != nil {
			return err
		}
	}

	// Capture the connection the watcher is allowed to touch; the error path
	// below clears g.conn, which a watcher re-reading the field would race.
	conn := g.conn
	stop := internal.CancelConnOnContext(ctx, conn)

	_, err := conn.Write(b)

	// A cancellation that raced a successful write leaves the socket with an
	// expired deadline, which would fail every later write. Drop it either way.
	if stop() {
		_ = conn.Close()
		g.conn = nil
		if err == nil {
			return ctx.Err()
		}
		return err
	}
	if err != nil {
		_ = conn.Close()
		g.conn = nil
	}

	return err
}

func (g *gelfTCP) Write(message []byte) (n int, err error) {
	if err := g.WriteContext(context.Background(), message); err != nil {
		return 0, err
	}
	return len(message), nil
}

func (g *gelfTCP) WriteContext(ctx context.Context, message []byte) error {
	return g.send(ctx, message)
}

func (g *gelfTCP) Close() (err error) {
	if g.conn != nil {
		err = g.conn.Close()
		g.conn = nil
	}

	return err
}

func (g *gelfTCP) Connect() error {
	return g.ConnectContext(context.Background())
}

func (g *gelfTCP) ConnectContext(ctx context.Context) error {
	var err error
	var conn net.Conn
	if g.tlsConfig == nil {
		conn, err = g.dialer.DialContext(ctx, "tcp", g.gelfConfig.Endpoint)
	} else {
		var rawConn net.Conn
		rawConn, err = g.dialer.DialContext(ctx, "tcp", g.gelfConfig.Endpoint)
		if err == nil {
			tlsConn := tls.Client(rawConn, g.tlsConfig)
			if hsErr := tlsConn.HandshakeContext(ctx); hsErr != nil {
				rawConn.Close()
				err = hsErr
			} else {
				conn = tlsConn
			}
		}
	}
	if err != nil {
		return err
	}
	g.conn = conn
	return nil
}

func (g *gelfTCP) send(ctx context.Context, b []byte) error {
	if g.conn == nil {
		if err := g.ConnectContext(ctx); err != nil {
			return err
		}
	}

	if err := g.writeFrame(ctx, b); err == nil {
		return nil
	}

	if ctx.Err() != nil {
		// Cancelled: don't retry against a fresh, unbounded connection.
		return ctx.Err()
	}

	// The peer may have closed the connection without us noticing because we
	// only ever write to it. Reconnect and retry the write once before
	// reporting the error to avoid noisy logs on every graceful close.
	if err := g.ConnectContext(ctx); err != nil {
		g.conn = nil
		return err
	}
	return g.writeFrame(ctx, b)
}

func (g *gelfTCP) writeFrame(ctx context.Context, b []byte) error {
	// Capture the connection the watcher is allowed to touch; the error paths
	// below clear g.conn, which a watcher re-reading the field would race.
	conn := g.conn
	stop := internal.CancelConnOnContext(ctx, conn)
	defer stop()

	discard := func() {
		stop()
		_ = conn.Close()
		g.conn = nil
	}

	if _, err := conn.Write(b); err != nil {
		discard()
		return err
	}
	if _, err := conn.Write([]byte{0}); err != nil { // message delimiter
		discard()
		return err
	}

	// A cancellation that raced the successful write leaves the socket with an
	// expired deadline, which would fail every later write. Drop it, and don't
	// report a frame as delivered when the caller has already given up on it.
	if stop() {
		discard()
		return ctx.Err()
	}

	return nil
}

type Graylog struct {
	Servers           []string        `toml:"servers"`
	ShortMessageField string          `toml:"short_message_field"`
	NameFieldNoPrefix bool            `toml:"name_field_noprefix"`
	Timeout           config.Duration `toml:"timeout"`
	Reconnection      bool            `toml:"connection_retry"`
	ReconnectionTime  config.Duration `toml:"connection_retry_wait_time"`
	Log               telegraf.Logger `toml:"-"`
	common_tls.ClientConfig

	closers     []io.WriteCloser
	endpoints   []gelf
	unconnected []string
	stopRetry   bool
	wg          sync.WaitGroup

	sync.Mutex
}

func (*Graylog) SampleConfig() string {
	return sampleConfig
}

func (g *Graylog) Connect() error {
	return g.ConnectContext(context.Background())
}

// ConnectContext connects to all configured servers, aborting an in-flight
// dial/TLS-handshake when ctx is cancelled. In connection_retry mode the
// retry loop runs in the background and Connect always returns immediately,
// so ctx only bounds the initial (synchronous) connection attempt below.
func (g *Graylog) ConnectContext(ctx context.Context) error {
	if len(g.Servers) == 0 {
		g.Servers = append(g.Servers, "localhost:12201")
	}

	tlsCfg, err := g.ClientConfig.TLSConfig()
	if err != nil {
		return err
	}

	if g.Reconnection {
		g.wg.Add(1)
		go g.connectRetry(tlsCfg)
		return nil
	}

	unconnected, gelfs := g.connectEndpoints(ctx, g.Servers, tlsCfg)
	if len(unconnected) > 0 {
		servers := strings.Join(unconnected, ",")
		return fmt.Errorf("connect: connection failed for %s", servers)
	}
	closers := make([]io.WriteCloser, 0, len(gelfs))
	for _, w := range gelfs {
		closers = append(closers, w)
	}
	g.Lock()
	defer g.Unlock()
	g.closers = closers
	g.endpoints = gelfs

	return nil
}

func (g *Graylog) connectRetry(tlsCfg *tls.Config) {
	defer g.wg.Done()

	var closers []io.WriteCloser
	var endpoints []gelf
	var attempt int64

	servers := make([]string, 0, len(g.Servers))
	servers = append(servers, g.Servers...)
	for {
		unconnected, gelfs := g.connectEndpoints(context.Background(), servers, tlsCfg)
		for _, w := range gelfs {
			closers = append(closers, w)
			endpoints = append(endpoints, w)
		}
		g.Lock()
		g.unconnected = unconnected
		stopRetry := g.stopRetry
		g.Unlock()
		if stopRetry {
			g.Log.Info("Stopping connection retries...")
			break
		}
		if len(unconnected) == 0 {
			break
		}
		attempt++
		servers := strings.Join(unconnected, ",")
		g.Log.Infof("Not connected to endpoints %s after attempt #%d...", servers, attempt)
		time.Sleep(time.Duration(g.ReconnectionTime))
	}
	g.Log.Info("Connected!")

	g.Lock()
	g.closers = closers
	g.endpoints = endpoints
	g.Unlock()
}

func (g *Graylog) connectEndpoints(ctx context.Context, servers []string, tlsCfg *tls.Config) ([]string, []gelf) {
	writers := make([]gelf, 0, len(servers))
	unconnected := make([]string, 0, len(servers))
	dialer := &net.Dialer{Timeout: time.Duration(g.Timeout)}
	for _, server := range servers {
		w := newGelfWriter(gelfConfig{Endpoint: server}, dialer, tlsCfg)
		if err := w.ConnectContext(ctx); err != nil {
			g.Log.Warnf("failed to connect to server [%s]: %v", server, err)
			unconnected = append(unconnected, server)
			continue
		}
		writers = append(writers, w)
	}
	return unconnected, writers
}

func (g *Graylog) Close() error {
	g.Lock()
	g.stopRetry = true
	g.Unlock()
	g.wg.Wait()

	for _, closer := range g.closers {
		_ = closer.Close()
	}
	return nil
}

func (g *Graylog) Write(metrics []telegraf.Metric) error {
	return g.WriteContext(context.Background(), metrics)
}

// WriteContext writes metrics to all connected endpoints. Each endpoint's
// own WriteContext is responsible for aborting an in-flight write when ctx
// is cancelled.
func (g *Graylog) WriteContext(ctx context.Context, metrics []telegraf.Metric) error {
	g.Lock()
	endpoints := g.endpoints
	g.Unlock()

	if len(endpoints) == 0 {
		g.Lock()
		unconnected := strings.Join(g.unconnected, ",")
		g.Unlock()

		return fmt.Errorf("not connected to %s", unconnected)
	}

	for _, metric := range metrics {
		values, err := g.serialize(metric)
		if err != nil {
			return err
		}

		for _, value := range values {
			for _, w := range endpoints {
				if err := w.WriteContext(ctx, []byte(value)); err != nil {
					return fmt.Errorf("error writing message: %q: %w", value, err)
				}
			}
		}
	}
	return nil
}

func (g *Graylog) serialize(metric telegraf.Metric) ([]string, error) {
	m := make(map[string]interface{})
	m["version"] = "1.1"
	m["timestamp"] = float64(metric.Time().UnixNano()) / 1_000_000_000
	m["short_message"] = "telegraf"
	if g.NameFieldNoPrefix {
		m["name"] = metric.Name()
	} else {
		m["_name"] = metric.Name()
	}

	if host, ok := metric.GetTag("host"); ok {
		m["host"] = host
	} else {
		host, err := os.Hostname()
		if err != nil {
			return nil, err
		}
		m["host"] = host
	}

	for _, tag := range metric.TagList() {
		if tag.Key == "host" {
			continue
		}

		if fieldInSpec(tag.Key) {
			m[tag.Key] = tag.Value
		} else {
			m["_"+tag.Key] = tag.Value
		}
	}

	for _, field := range metric.FieldList() {
		if field.Key == g.ShortMessageField {
			m["short_message"] = field.Value
		} else if fieldInSpec(field.Key) {
			m[field.Key] = field.Value
		} else {
			m["_"+field.Key] = field.Value
		}
	}

	serialized, err := ejson.Marshal(m)
	if err != nil {
		return nil, err
	}

	return []string{string(serialized)}, nil
}

func fieldInSpec(field string) bool {
	for _, specField := range defaultSpecFields {
		if specField == field {
			return true
		}
	}

	return false
}

func init() {
	outputs.Add("graylog", func() telegraf.Output {
		return &Graylog{
			Timeout:          config.Duration(defaultTimeout),
			ReconnectionTime: config.Duration(defaultReconnectionTime),
		}
	})
}
