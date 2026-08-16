//go:generate ../../../tools/readme_config_includer/generator
package stomp

import (
	"context"
	"crypto/tls"
	_ "embed"
	"fmt"
	"net"
	"time"

	"github.com/go-stomp/stomp"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	common_tls "github.com/influxdata/telegraf/plugins/common/tls"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

type STOMP struct {
	Host      string          `toml:"host"`
	Username  config.Secret   `toml:"username"`
	Password  config.Secret   `toml:"password"`
	QueueName string          `toml:"queueName"`
	Log       telegraf.Logger `toml:"-"`

	HeartBeatSend config.Duration `toml:"heartbeat_timeout_send"`
	HeartBeatRec  config.Duration `toml:"heartbeat_timeout_receive"`

	common_tls.ClientConfig

	conn  net.Conn
	stomp *stomp.Conn

	serialize telegraf.Serializer
}

func (q *STOMP) Connect() error {
	return q.ConnectContext(context.Background())
}

// ConnectContext dials and performs the STOMP handshake, returning early
// if ctx is cancelled. Neither net.Dial/tls.Dial nor stomp.Connect accept
// a context, so ConnectContext races the whole dial+handshake in a
// goroutine and abandons it on cancellation, disconnecting/closing
// whatever it eventually produces.
func (q *STOMP) ConnectContext(ctx context.Context) error {
	type connectResult struct {
		conn  net.Conn
		stomp *stomp.Conn
		err   error
	}
	resultCh := make(chan connectResult, 1)
	go func() {
		tlsConfig, err := q.ClientConfig.TLSConfig()
		if err != nil {
			resultCh <- connectResult{err: err}
			return
		}

		var conn net.Conn
		if tlsConfig != nil {
			conn, err = tls.Dial("tcp", q.Host, tlsConfig)
		} else {
			conn, err = net.Dial("tcp", q.Host)
		}
		if err != nil {
			resultCh <- connectResult{err: err}
			return
		}

		authOption, err := q.getAuthOption()
		if err != nil {
			conn.Close()
			resultCh <- connectResult{err: err}
			return
		}
		heartbeatOption := stomp.ConnOpt.HeartBeat(
			time.Duration(q.HeartBeatSend),
			time.Duration(q.HeartBeatRec),
		)
		sc, err := stomp.Connect(conn, heartbeatOption, authOption)
		if err != nil {
			conn.Close()
			resultCh <- connectResult{err: err}
			return
		}
		resultCh <- connectResult{conn: conn, stomp: sc}
	}()

	select {
	case res := <-resultCh:
		if res.err != nil {
			return res.err
		}
		q.conn = res.conn
		q.stomp = res.stomp
		q.Log.Debug("STOMP Connected...")
		return nil
	case <-ctx.Done():
		go func() {
			res := <-resultCh
			if res.stomp != nil {
				res.stomp.Disconnect()
			} else if res.conn != nil {
				res.conn.Close()
			}
		}()
		return ctx.Err()
	}
}

func (q *STOMP) SetSerializer(serializer telegraf.Serializer) {
	q.serialize = serializer
}

func (q *STOMP) Write(metrics []telegraf.Metric) error {
	return q.WriteContext(context.Background(), metrics)
}

// WriteContext sends metrics via STOMP, returning early if ctx is
// cancelled. stomp.Conn.Send has no context support and can block
// indefinitely pushing to the connection's internal write channel if its
// background writer goroutine is stuck, while holding the connection's
// internal lock for the whole blocked call -- so a cancelled send leaves
// the connection unsafe to reuse. Each send is raced in a goroutine;
// on cancellation the connection is detached (closed in the background
// once the abandoned send returns) so the next write reconnects instead
// of blocking behind that lock.
func (q *STOMP) WriteContext(ctx context.Context, metrics []telegraf.Metric) error {
	if q.stomp == nil {
		if err := q.ConnectContext(ctx); err != nil {
			return err
		}
	}

	for _, metric := range metrics {
		if err := ctx.Err(); err != nil {
			return err
		}

		values, err := q.serialize.Serialize(metric)
		if err != nil {
			q.Log.Errorf("Serializing metric %v failed: %s", metric, err)
			continue
		}

		sc := q.stomp
		done := make(chan error, 1)
		go func() {
			done <- sc.Send(q.QueueName, "text/plain", values, nil)
		}()

		select {
		case err := <-done:
			if err != nil {
				return fmt.Errorf("sending metric failed: %w", err)
			}
		case <-ctx.Done():
			q.stomp = nil
			q.conn = nil
			go func() {
				<-done
				sc.Disconnect()
			}()
			return ctx.Err()
		}
	}
	return nil
}
func (*STOMP) SampleConfig() string {
	return sampleConfig
}
func (q *STOMP) Close() error {
	if q.stomp == nil {
		return nil
	}
	return q.stomp.Disconnect()
}

func (q *STOMP) getAuthOption() (func(*stomp.Conn) error, error) {
	username, err := q.Username.Get()
	if err != nil {
		return nil, fmt.Errorf("getting username failed: %w", err)
	}
	defer username.Destroy()
	password, err := q.Password.Get()
	if err != nil {
		return nil, fmt.Errorf("getting password failed: %w", err)
	}
	defer password.Destroy()
	return stomp.ConnOpt.Login(username.String(), password.String()), nil
}

func init() {
	outputs.Add("stomp", func() telegraf.Output {
		return &STOMP{
			Host:      "localhost:61613",
			QueueName: "telegraf",
		}
	})
}
