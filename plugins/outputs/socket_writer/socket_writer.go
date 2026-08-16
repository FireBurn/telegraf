//go:generate ../../../tools/readme_config_includer/generator
package socket_writer

import (
	"context"
	"crypto/tls"
	_ "embed"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/mdlayher/vsock"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/internal"
	common_tls "github.com/influxdata/telegraf/plugins/common/tls"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

type SocketWriter struct {
	ContentEncoding string `toml:"content_encoding"`
	Address         string
	KeepAlivePeriod *config.Duration
	common_tls.ClientConfig
	Log telegraf.Logger `toml:"-"`

	serializer telegraf.Serializer

	encoder internal.ContentEncoder

	net.Conn
}

func (*SocketWriter) SampleConfig() string {
	return sampleConfig
}

func (sw *SocketWriter) SetSerializer(s telegraf.Serializer) {
	sw.serializer = s
}

func (sw *SocketWriter) Connect() error {
	return sw.ConnectContext(context.Background())
}

// ConnectContext dials the socket, aborting the dial/handshake when ctx is
// cancelled. vsock has no context-aware dial API, so that path races the
// blocking call in a goroutine and abandons it on cancellation, closing any
// connection it eventually produces.
func (sw *SocketWriter) ConnectContext(ctx context.Context) error {
	spl := strings.SplitN(sw.Address, "://", 2)
	if len(spl) != 2 {
		return fmt.Errorf("invalid address: %s", sw.Address)
	}

	tlsCfg, err := sw.ClientConfig.TLSConfig()
	if err != nil {
		return err
	}

	var c net.Conn
	var sockErr error
	if spl[0] == "vsock" {
		addrTuple := strings.SplitN(spl[1], ":", 2)

		// Check address string for containing two
		if len(addrTuple) < 2 {
			return errors.New("port and/or CID number missing")
		}

		// Parse CID and port number from address string both being 32-bit
		// source: https://man7.org/linux/man-pages/man7/vsock.7.html
		cid, err := strconv.ParseUint(addrTuple[0], 10, 32)
		if err != nil {
			return fmt.Errorf("failed to parse CID %s: %w", addrTuple[0], err)
		}
		if (cid >= uint64(math.Pow(2, 32))-1) && (cid <= 0) {
			return fmt.Errorf("value of CID %d is out of range", cid)
		}
		port, err := strconv.ParseUint(addrTuple[1], 10, 32)
		if err != nil {
			return fmt.Errorf("failed to parse port number %s: %w", addrTuple[1], err)
		}
		if (port >= uint64(math.Pow(2, 32))-1) && (port <= 0) {
			return fmt.Errorf("port number %d is out of range", port)
		}

		type dialResult struct {
			conn net.Conn
			err  error
		}
		resultCh := make(chan dialResult, 1)
		go func() {
			conn, err := vsock.Dial(uint32(cid), uint32(port), nil)
			resultCh <- dialResult{conn, err}
		}()
		select {
		case res := <-resultCh:
			c, sockErr = res.conn, res.err
		case <-ctx.Done():
			go func() {
				if res := <-resultCh; res.conn != nil {
					res.conn.Close()
				}
			}()
			return ctx.Err()
		}
	} else {
		d := net.Dialer{}
		if tlsCfg == nil {
			c, sockErr = d.DialContext(ctx, spl[0], spl[1])
		} else {
			tlsDialer := &tls.Dialer{NetDialer: &d, Config: tlsCfg}
			c, sockErr = tlsDialer.DialContext(ctx, spl[0], spl[1])
		}
	}

	if sockErr != nil {
		return &internal.StartupError{
			Err:   sockErr,
			Retry: true,
		}
	}

	if err := sw.setKeepAlive(c); err != nil {
		sw.Log.Debugf("Unable to configure keep alive (%s): %s", sw.Address, err)
	}
	// set encoder
	sw.encoder, err = internal.NewContentEncoder(sw.ContentEncoding)
	if err != nil {
		return err
	}

	sw.Conn = c
	return nil
}

func (sw *SocketWriter) setKeepAlive(c net.Conn) error {
	if sw.KeepAlivePeriod == nil {
		return nil
	}
	tcpc, ok := c.(*net.TCPConn)
	if !ok {
		return fmt.Errorf("cannot set keep alive on a %s socket", strings.SplitN(sw.Address, "://", 2)[0])
	}
	if *sw.KeepAlivePeriod == 0 {
		return tcpc.SetKeepAlive(false)
	}
	if err := tcpc.SetKeepAlive(true); err != nil {
		return err
	}
	return tcpc.SetKeepAlivePeriod(time.Duration(*sw.KeepAlivePeriod))
}

// Write writes the given metrics to the destination.
// If an error is encountered, it is up to the caller to retry the same write again later.
// Not parallel safe.
func (sw *SocketWriter) Write(metrics []telegraf.Metric) error {
	return sw.WriteContext(context.Background(), metrics)
}

// WriteContext writes the given metrics to the destination, aborting an
// in-flight write via a forced deadline when ctx is cancelled. net.Conn has
// no native context support.
func (sw *SocketWriter) WriteContext(ctx context.Context, metrics []telegraf.Metric) error {
	if sw.Conn == nil {
		// previous write failed with permanent error and socket was closed.
		if err := sw.ConnectContext(ctx); err != nil {
			return err
		}
	}

	// Capture the connection the watcher is allowed to touch. Re-reading
	// sw.Conn from the watcher would race the error path below, which closes
	// the connection and may install a replacement.
	conn := sw.Conn
	stop := internal.CancelConnOnContext(ctx, conn)
	defer stop()

	for _, m := range metrics {
		bs, err := sw.serializer.Serialize(m)
		if err != nil {
			sw.Log.Debugf("Could not serialize metric: %v", err)
			continue
		}

		bs, err = sw.encoder.Encode(bs)
		if err != nil {
			sw.Log.Debugf("Could not encode metric: %v", err)
			continue
		}

		if _, err := conn.Write(bs); err != nil {
			// TODO log & keep going with remaining strings
			var netErr net.Error
			if errors.As(err, &netErr) {
				// permanent error. close the connection
				stop()
				sw.Close()
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				return fmt.Errorf("closing connection: %w", netErr)
			}
			return err
		}
	}

	// A cancellation that raced the last successful write leaves the socket
	// with an expired deadline, which would fail every later write. Drop it.
	if stop() {
		sw.Close()
		return ctx.Err()
	}

	return nil
}

// Close closes the connection. Noop if already closed.
func (sw *SocketWriter) Close() error {
	if sw.Conn == nil {
		return nil
	}
	err := sw.Conn.Close()
	sw.Conn = nil
	return err
}

func init() {
	outputs.Add("socket_writer", func() telegraf.Output {
		return &SocketWriter{}
	})
}
