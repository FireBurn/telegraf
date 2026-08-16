package quix

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/testutil"
)

// controlledProducer is a fake sarama.SyncProducer for deterministic tests:
// it never dials, and SendMessage/Close can be told to block until a test
// explicitly releases them.
type controlledProducer struct {
	sarama.SyncProducer
	sendBlock  <-chan struct{}
	closeBlock <-chan struct{}
	sendErr    error

	sends  atomic.Int32
	closes atomic.Int32
}

func (p *controlledProducer) SendMessage(*sarama.ProducerMessage) (int32, int64, error) {
	p.sends.Add(1)
	if p.sendBlock != nil {
		<-p.sendBlock
	}
	return 0, 0, p.sendErr
}

func (p *controlledProducer) Close() error {
	p.closes.Add(1)
	if p.closeBlock != nil {
		<-p.closeBlock
	}
	return nil
}

// newBrokerConfigServer starts a local HTTP server that immediately answers
// with a plaintext broker config, so tests exercise the real
// fetchBrokerConfig path without any network dependency or extra latency;
// the blocking under test always happens in producerFunc.
func newBrokerConfigServer(t *testing.T) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, err := w.Write([]byte(`{"bootstrap.servers":"127.0.0.1:9092","security.protocol":"PLAINTEXT"}`))
		require.NoError(t, err)
	}))
	t.Cleanup(server.Close)
	return server
}

func newTestPlugin(t *testing.T, factory func([]string, *sarama.Config) (sarama.SyncProducer, error)) *Quix {
	t.Helper()
	server := newBrokerConfigServer(t)
	plugin := &Quix{
		APIURL:       server.URL,
		Workspace:    "ws",
		Topic:        "telegraf",
		Token:        config.NewSecret([]byte("token")),
		Log:          testutil.Logger{},
		producerFunc: factory,
	}
	require.NoError(t, plugin.Init())
	return plugin
}

func TestWriteContext(t *testing.T) {
	producer := &controlledProducer{}
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		return producer, nil
	})

	require.NoError(t, plugin.WriteContext(t.Context(), testutil.MockMetrics()))
	require.Equal(t, int32(1), producer.sends.Load())
}

func TestWriteContextCancellationDoesNotWaitForProducer(t *testing.T) {
	sendBlock := make(chan struct{})
	closeBlock := make(chan struct{})
	producer := &controlledProducer{sendBlock: sendBlock, closeBlock: closeBlock}
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		return producer, nil
	})

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := plugin.WriteContext(ctx, testutil.MockMetrics())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 500*time.Millisecond)

	// Release the fake only after proving that neither SendMessage nor
	// Close is on the cancellation path.
	close(sendBlock)
	close(closeBlock)
}

func TestWriteContextReplacesPoisonedProducer(t *testing.T) {
	oldSendBlock := make(chan struct{})
	oldCloseBlock := make(chan struct{})
	oldProducer := &controlledProducer{sendBlock: oldSendBlock, closeBlock: oldCloseBlock}
	newProducer := &controlledProducer{}
	producers := []sarama.SyncProducer{oldProducer, newProducer}
	var created atomic.Int32
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		return producers[int(created.Add(1))-1], nil
	})

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, plugin.WriteContext(ctx, testutil.MockMetrics()), context.DeadlineExceeded)

	// A subsequent write must go through cleanly against a fresh producer;
	// this proves the poisoning of the old generation doesn't wedge future
	// writes.
	require.NoError(t, plugin.WriteContext(t.Context(), testutil.MockMetrics()))
	require.Equal(t, int32(1), newProducer.sends.Load())

	close(oldSendBlock)
	close(oldCloseBlock)
	require.Eventually(t, func() bool { return oldProducer.closes.Load() == 1 }, time.Second, time.Millisecond)
	require.Zero(t, newProducer.closes.Load())
}

func TestWriteContextBoundsPoisonedProducers(t *testing.T) {
	blocks := []chan struct{}{make(chan struct{}), make(chan struct{})}
	producers := []sarama.SyncProducer{
		&controlledProducer{sendBlock: blocks[0], closeBlock: blocks[0]},
		&controlledProducer{sendBlock: blocks[1], closeBlock: blocks[1]},
	}
	var created atomic.Int32
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
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
	require.Contains(t, logger.Errors()[0], "Quix producer replacement limit reached")

	require.Error(t, plugin.WriteContext(t.Context(), testutil.MockMetrics()))
	require.Len(t, logger.Errors(), 1)

	for _, block := range blocks {
		close(block)
	}
}

func TestWriteContextBoundsProducerCreation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	producer := &controlledProducer{}
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		close(started)
		<-release
		return producer, nil
	})

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := plugin.WriteContext(ctx, testutil.MockMetrics())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 500*time.Millisecond)

	close(release)
	require.Eventually(t, func() bool {
		plugin.producerMu.Lock()
		defer plugin.producerMu.Unlock()
		return plugin.producer == producer
	}, time.Second, time.Millisecond)
	require.NoError(t, plugin.Close())
}

func TestProducerCreationIsShared(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	producer := &controlledProducer{}
	var creations atomic.Int32
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		if creations.Add(1) == 1 {
			close(started)
		}
		<-release
		return producer, nil
	})

	results := make(chan error, 3)
	for range 3 {
		go func() {
			ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
			defer cancel()
			results <- plugin.ConnectContext(ctx)
		}()
	}
	<-started
	for range 3 {
		require.ErrorIs(t, <-results, context.DeadlineExceeded)
	}
	require.Equal(t, int32(1), creations.Load())
	close(release)
}

func TestClose(t *testing.T) {
	producer := &controlledProducer{}
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		return producer, nil
	})
	require.NoError(t, plugin.ConnectContext(t.Context()))
	require.NoError(t, plugin.Close())
	require.Equal(t, int32(1), producer.closes.Load())

	// Close on a never-connected plugin must be a no-op, not a panic.
	plugin2 := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		return &controlledProducer{}, nil
	})
	require.NoError(t, plugin2.Close())
}

func TestCloseTimesOutOnStuckProducer(t *testing.T) {
	closeBlock := make(chan struct{})
	defer close(closeBlock)
	producer := &controlledProducer{closeBlock: closeBlock}
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		return producer, nil
	})
	plugin.closeTimeout = 20 * time.Millisecond
	require.NoError(t, plugin.ConnectContext(t.Context()))

	start := time.Now()
	err := plugin.Close()
	require.ErrorContains(t, err, "timed out")
	require.Less(t, time.Since(start), 500*time.Millisecond)
}
