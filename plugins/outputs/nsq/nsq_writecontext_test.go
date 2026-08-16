package nsq

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf/plugins/serializers/influx"
	"github.com/influxdata/telegraf/testutil"
)

// controlledProducer is a fake nsqProducer for deterministic tests: it
// never touches the network, and Publish/Stop can be told to block until a
// test explicitly releases them.
type controlledProducer struct {
	publishBlock <-chan struct{}
	publishErr   error
	stopBlock    <-chan struct{}

	publishes atomic.Int32
	stops     atomic.Int32
}

func (p *controlledProducer) Publish(string, []byte) error {
	p.publishes.Add(1)
	if p.publishBlock != nil {
		<-p.publishBlock
	}
	return p.publishErr
}

func (p *controlledProducer) Stop() {
	p.stops.Add(1)
	if p.stopBlock != nil {
		<-p.stopBlock
	}
}

func newTestPlugin(t *testing.T, factory func(string, *nsq.Config) (nsqProducer, error)) *NSQ {
	t.Helper()
	s := &influx.Serializer{}
	require.NoError(t, s.Init())
	plugin := &NSQ{Server: "127.0.0.1:4150", Topic: "telegraf", Log: testutil.Logger{}, producerFunc: factory}
	plugin.SetSerializer(s)
	return plugin
}

func TestWriteContext(t *testing.T) {
	tests := []struct {
		name       string
		publishErr error
		expectErr  bool
	}{
		{name: "success"},
		{name: "publish error", publishErr: errors.New("boom"), expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			producer := &controlledProducer{publishErr: tt.publishErr}
			plugin := newTestPlugin(t, func(string, *nsq.Config) (nsqProducer, error) {
				return producer, nil
			})

			err := plugin.WriteContext(t.Context(), testutil.MockMetrics())
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, int32(1), producer.publishes.Load())
		})
	}
}

func TestWriteContextCancellationDoesNotWaitForProducer(t *testing.T) {
	publishBlock := make(chan struct{})
	stopBlock := make(chan struct{})
	producer := &controlledProducer{publishBlock: publishBlock, stopBlock: stopBlock}
	plugin := newTestPlugin(t, func(string, *nsq.Config) (nsqProducer, error) {
		return producer, nil
	})

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := plugin.WriteContext(ctx, testutil.MockMetrics())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 500*time.Millisecond)

	// Release the fake only after proving that neither Publish nor Stop is
	// on the cancellation path.
	close(publishBlock)
	close(stopBlock)
}

func TestWriteContextReplacesPoisonedProducer(t *testing.T) {
	oldPublishBlock := make(chan struct{})
	oldStopBlock := make(chan struct{})
	oldProducer := &controlledProducer{publishBlock: oldPublishBlock, stopBlock: oldStopBlock}
	newProducer := &controlledProducer{}
	producers := []nsqProducer{oldProducer, newProducer}
	var created atomic.Int32
	plugin := newTestPlugin(t, func(string, *nsq.Config) (nsqProducer, error) {
		return producers[int(created.Add(1))-1], nil
	})

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, plugin.WriteContext(ctx, testutil.MockMetrics()), context.DeadlineExceeded)

	// A subsequent write must go through cleanly against a fresh producer;
	// this proves the poisoning of the old generation doesn't wedge future
	// writes.
	require.NoError(t, plugin.WriteContext(t.Context(), testutil.MockMetrics()))
	require.Equal(t, int32(1), newProducer.publishes.Load())

	close(oldPublishBlock)
	close(oldStopBlock)
	require.Eventually(t, func() bool { return oldProducer.stops.Load() == 1 }, time.Second, time.Millisecond)
	require.Zero(t, newProducer.stops.Load())
}

func TestWriteContextBoundsPoisonedProducers(t *testing.T) {
	blocks := []chan struct{}{make(chan struct{}), make(chan struct{})}
	producers := []nsqProducer{
		&controlledProducer{publishBlock: blocks[0], stopBlock: blocks[0]},
		&controlledProducer{publishBlock: blocks[1], stopBlock: blocks[1]},
	}
	var created atomic.Int32
	plugin := newTestPlugin(t, func(string, *nsq.Config) (nsqProducer, error) {
		return producers[int(created.Add(1))-1], nil
	})
	logger := &testutil.CaptureLogger{}
	plugin.Log = logger

	for range producers {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		require.ErrorIs(t, plugin.WriteContext(ctx, testutil.MockMetrics()), context.DeadlineExceeded)
		cancel()
	}

	start := time.Now()
	err := plugin.WriteContext(t.Context(), testutil.MockMetrics())
	require.ErrorContains(t, err, "replacement limit reached")
	require.Less(t, time.Since(start), 100*time.Millisecond)
	require.Equal(t, int32(2), created.Load())
	require.Len(t, logger.Errors(), 1)
	require.Contains(t, logger.Errors()[0], "NSQ producer replacement limit reached")

	// Repeated attempts while still capped should keep failing fast without
	// re-logging.
	require.Error(t, plugin.WriteContext(t.Context(), testutil.MockMetrics()))
	require.Len(t, logger.Errors(), 1)

	for _, block := range blocks {
		close(block)
	}
}

func TestConnectContextCancellationBeforeCall(t *testing.T) {
	plugin := newTestPlugin(t, func(string, *nsq.Config) (nsqProducer, error) {
		t.Fatal("producer must not be created once the context is already done")
		return nil, nil
	})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, plugin.ConnectContext(ctx), context.Canceled)
}

func TestConnectContext(t *testing.T) {
	producer := &controlledProducer{}
	plugin := newTestPlugin(t, func(string, *nsq.Config) (nsqProducer, error) {
		return producer, nil
	})

	require.NoError(t, plugin.ConnectContext(t.Context()))
	require.NoError(t, plugin.WriteContext(t.Context(), testutil.MockMetrics()))
	require.Equal(t, int32(1), producer.publishes.Load())
}

func TestClose(t *testing.T) {
	producer := &controlledProducer{}
	plugin := newTestPlugin(t, func(string, *nsq.Config) (nsqProducer, error) {
		return producer, nil
	})
	require.NoError(t, plugin.ConnectContext(t.Context()))
	require.NoError(t, plugin.Close())
	require.Equal(t, int32(1), producer.stops.Load())

	// Close on an already-closed / never-connected plugin must be a no-op,
	// not a panic.
	plugin2 := newTestPlugin(t, func(string, *nsq.Config) (nsqProducer, error) {
		return &controlledProducer{}, nil
	})
	require.NoError(t, plugin2.Close())
}

func TestCloseTimesOutOnStuckProducer(t *testing.T) {
	stopBlock := make(chan struct{})
	defer close(stopBlock)
	producer := &controlledProducer{stopBlock: stopBlock}
	plugin := newTestPlugin(t, func(string, *nsq.Config) (nsqProducer, error) {
		return producer, nil
	})
	plugin.closeTimeout = 20 * time.Millisecond
	require.NoError(t, plugin.ConnectContext(t.Context()))

	start := time.Now()
	err := plugin.Close()
	require.ErrorContains(t, err, "timed out")
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestWriteContextEmptyMetrics(t *testing.T) {
	plugin := newTestPlugin(t, func(string, *nsq.Config) (nsqProducer, error) {
		t.Fatal("producer must not be created for an empty batch")
		return nil, nil
	})
	require.NoError(t, plugin.WriteContext(t.Context(), nil))
}
