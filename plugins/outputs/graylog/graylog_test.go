package graylog

import (
	"context"
	"encoding/json"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/testutil"
)

func TestWriteContextCancelUnblocksBlockedWrite(t *testing.T) {
	// net.Pipe is fully synchronous: a Write blocks until the peer Reads,
	// giving a deterministic (non-timing-dependent) blocked write.
	client, server := net.Pipe()
	defer server.Close()
	blocked := &enteredConn{Conn: client, entered: make(chan struct{})}

	w := &gelfTCP{gelfCommon: gelfCommon{conn: blocked}}

	g := &Graylog{Log: testutil.Logger{}}
	g.endpoints = []gelf{w}

	m := metric.New(
		"cpu",
		map[string]string{},
		map[string]interface{}{"value": 3.14},
		time.Unix(0, 0),
	)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- g.WriteContext(ctx, []telegraf.Metric{m})
	}()

	// Cancel only once the write has actually reached the blocked connection.
	<-blocked.entered
	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("WriteContext did not return promptly after context cancellation")
	}

	// The poisoned connection must be dropped so the next write reconnects.
	require.Nil(t, w.conn)
}

// enteredConn signals when a Write has actually reached the connection, so
// cancellation tests never have to guess the timing with a sleep.
type enteredConn struct {
	net.Conn
	entered chan struct{}
	once    sync.Once
}

func (c *enteredConn) Write(b []byte) (int, error) {
	c.once.Do(func() { close(c.entered) })
	return c.Conn.Write(b)
}

func TestSerializer(t *testing.T) {
	m1 := metric.New("testing",
		map[string]string{
			"verb": "GET",
			"host": "hostname",
		},
		map[string]interface{}{
			"full_message":  "full",
			"short_message": "short",
			"level":         "1",
			"facility":      "demo",
			"line":          "42",
			"file":          "graylog.go",
		},
		time.Now(),
	)

	graylog := Graylog{}
	result, err := graylog.serialize(m1)

	require.NoError(t, err)

	for _, r := range result {
		obj := make(map[string]interface{})
		err = json.Unmarshal([]byte(r), &obj)
		require.NoError(t, err)

		require.Equal(t, "1.1", obj["version"])
		require.Equal(t, "testing", obj["_name"])
		require.Equal(t, "GET", obj["_verb"])
		require.Equal(t, "hostname", obj["host"])
		require.Equal(t, "full", obj["full_message"])
		require.Equal(t, "short", obj["short_message"])
		require.Equal(t, "1", obj["level"])
		require.Equal(t, "demo", obj["facility"])
		require.Equal(t, "42", obj["line"])
		require.Equal(t, "graylog.go", obj["file"])
	}
}
