package inlong

import (
	"context"
	"testing"
	"time"

	"github.com/apache/inlong/inlong-sdk/dataproxy-sdk-twins/dataproxy-sdk-golang/dataproxy"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf/plugins/serializers/influx"
	"github.com/influxdata/telegraf/testutil"
)

type blockingProducer struct{}

func (blockingProducer) Send(ctx context.Context, _ dataproxy.Message) error {
	<-ctx.Done()
	return ctx.Err()
}

func (blockingProducer) SendAsync(context.Context, dataproxy.Message, dataproxy.Callback) {}

func (blockingProducer) Close() {}

func TestWriteContextCancellation(t *testing.T) {
	plugin := &Inlong{
		GroupID:    "test",
		StreamID:   "test",
		producer:   blockingProducer{},
		serializer: &influx.Serializer{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		done <- plugin.WriteContext(ctx, testutil.MockMetrics())
	}()

	cancel()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("WriteContext did not return after context cancellation")
	}
}

func TestInvalidParameters(t *testing.T) {
	tests := []struct {
		name     string
		url      string
		gid      string
		sid      string
		expected string
	}{
		{
			name:     "all empty",
			expected: "'url' must not be empty",
		},
		{
			name:     "invalid url scheme",
			url:      "unix://localhost",
			gid:      "test",
			sid:      "test",
			expected: "invalid URL scheme",
		},
		{
			name:     "no host",
			url:      "http://?param=123",
			gid:      "test",
			sid:      "test",
			expected: "no host in URL",
		},
		{
			name:     "group id empty",
			url:      "http://localhost",
			expected: "'group_id' must not be empty",
		},
		{
			name:     "stream id empty",
			url:      "http://localhost",
			gid:      "test",
			expected: "'stream_id' must not be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := &Inlong{
				ManagerURL: tt.url,
				GroupID:    tt.gid,
				StreamID:   tt.sid,
			}
			require.ErrorContains(t, plugin.Init(), tt.expected)
		})
	}
}

func TestValidURLs(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{
			name: "http url scheme",
			url:  "http://localhost",
		},
		{
			name: "http url scheme with port",
			url:  "http://localhost:8080",
		},
		{
			name: "http url scheme with port and path",
			url:  "http://localhost:8080/foo",
		},
		{
			name: "https url scheme",
			url:  "https://localhost",
		},
		{
			name: "https url scheme with port",
			url:  "https://localhost:8080",
		},
		{
			name: "https url scheme with port and path",
			url:  "https://localhost:8080/foo",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plugin := &Inlong{
				ManagerURL: tt.url,
				GroupID:    "test",
				StreamID:   "test",
			}
			require.NoError(t, plugin.Init())
		})
	}
}
