package zabbix

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/datadope-io/go-zabbix/v2"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/testutil"
)

// blockingZabbixSender blocks in Send until unblock is closed, giving a
// deterministic (non-timing-dependent) blocked call to cancel against.
type blockingZabbixSender struct {
	started chan struct{}
	unblock chan struct{}
	once    sync.Once
}

func (s *blockingZabbixSender) Send(*zabbix.Packet) (zabbix.Response, error) {
	s.once.Do(func() { close(s.started) })
	<-s.unblock
	return zabbix.Response{}, nil
}

func (*blockingZabbixSender) SendMetrics([]*zabbix.Metric) (zabbix.Response, zabbix.Response, error) {
	return zabbix.Response{}, zabbix.Response{}, nil
}

func (*blockingZabbixSender) RegisterHost(string, string) error {
	return nil
}

func TestWriteContextCancelReturnsPromptly(t *testing.T) {
	sender := &blockingZabbixSender{started: make(chan struct{}), unblock: make(chan struct{})}
	defer close(sender.unblock)

	z := &Zabbix{
		Log:                        testutil.Logger{},
		HostTag:                    "host",
		KeyPrefix:                  "telegraf.",
		AutoregisterResendInterval: config.Duration(30 * time.Minute),
		LLDSendInterval:            config.Duration(10 * time.Minute),
		LLDClearInterval:           config.Duration(time.Hour),
	}
	require.NoError(t, z.Init())
	z.sender = sender

	m := metric.New(
		"cpu",
		map[string]string{"host": "myhost"},
		map[string]interface{}{"value": 3.14},
		time.Unix(0, 0),
	)

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- z.WriteContext(ctx, []telegraf.Metric{m})
	}()

	select {
	case <-sender.started:
	case <-time.After(2 * time.Second):
		t.Fatal("Send was not called")
	}
	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(2 * time.Second):
		t.Fatal("WriteContext did not return promptly after context cancellation")
	}
}
