package iotdb

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/apache/iotdb-client-go/client"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf/testutil"
)

// controlledSession is a fake ioTDBSession for deterministic tests: it
// never dials, and Open/InsertRecords/Close can be told to block until a
// test explicitly releases them.
type controlledSession struct {
	openBlock   <-chan struct{}
	openErr     error
	insertBlock <-chan struct{}
	insertErr   error
	closeBlock  <-chan struct{}
	closeErr    error

	opens   atomic.Int32
	inserts atomic.Int32
	closes  atomic.Int32
}

func (c *controlledSession) Open(bool, int) error {
	c.opens.Add(1)
	if c.openBlock != nil {
		<-c.openBlock
	}
	return c.openErr
}

func (c *controlledSession) InsertRecords([]string, [][]string, [][]client.TSDataType, [][]interface{}, []int64) error {
	c.inserts.Add(1)
	if c.insertBlock != nil {
		<-c.insertBlock
	}
	return c.insertErr
}

func (c *controlledSession) Close() error {
	c.closes.Add(1)
	if c.closeBlock != nil {
		<-c.closeBlock
	}
	return c.closeErr
}

func newTestPlugin(t *testing.T, factory func(cfg *client.Config) ioTDBSession) *IoTDB {
	t.Helper()
	plugin := newIoTDB()
	plugin.Log = testutil.Logger{}
	plugin.sessionFunc = factory
	require.NoError(t, plugin.Init())
	return plugin
}

func TestWriteContext(t *testing.T) {
	tests := []struct {
		name      string
		insertErr error
		expectErr bool
	}{
		{name: "success"},
		{name: "insert error", insertErr: errors.New("boom"), expectErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			session := &controlledSession{insertErr: tt.insertErr}
			plugin := newTestPlugin(t, func(*client.Config) ioTDBSession {
				return session
			})

			err := plugin.WriteContext(t.Context(), testutil.MockMetrics())
			if tt.expectErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, int32(1), session.inserts.Load())
		})
	}
}

func TestWriteContextCancellationDoesNotWaitForSession(t *testing.T) {
	insertBlock := make(chan struct{})
	session := &controlledSession{insertBlock: insertBlock}
	plugin := newTestPlugin(t, func(*client.Config) ioTDBSession {
		return session
	})

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := plugin.WriteContext(ctx, testutil.MockMetrics())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 500*time.Millisecond)

	// Release the fake only after proving InsertRecords is not on the
	// cancellation path. Close must not have been called concurrently with
	// the still in-flight InsertRecords -- see abandonSession.
	require.Zero(t, session.closes.Load())
	close(insertBlock)
	require.Eventually(t, func() bool { return session.closes.Load() == 1 }, time.Second, time.Millisecond)
}

func TestWriteContextReplacesPoisonedSession(t *testing.T) {
	oldInsertBlock := make(chan struct{})
	oldSession := &controlledSession{insertBlock: oldInsertBlock}
	newSession := &controlledSession{}
	sessions := []ioTDBSession{oldSession, newSession}
	var created atomic.Int32
	plugin := newTestPlugin(t, func(*client.Config) ioTDBSession {
		return sessions[int(created.Add(1))-1]
	})

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, plugin.WriteContext(ctx, testutil.MockMetrics()), context.DeadlineExceeded)

	// A subsequent write must go through cleanly against a fresh session;
	// this proves the poisoning of the old generation doesn't wedge future
	// writes.
	require.NoError(t, plugin.WriteContext(t.Context(), testutil.MockMetrics()))
	require.Equal(t, int32(1), newSession.inserts.Load())

	// The old session must only be closed once its abandoned InsertRecords
	// call actually returns -- not before.
	require.Zero(t, oldSession.closes.Load())
	close(oldInsertBlock)
	require.Eventually(t, func() bool { return oldSession.closes.Load() == 1 }, time.Second, time.Millisecond)
	require.Zero(t, newSession.closes.Load())
}

func TestWriteContextBoundsPoisonedSessions(t *testing.T) {
	blocks := []chan struct{}{make(chan struct{}), make(chan struct{})}
	sessions := []ioTDBSession{
		&controlledSession{insertBlock: blocks[0]},
		&controlledSession{insertBlock: blocks[1]},
	}
	var created atomic.Int32
	plugin := newTestPlugin(t, func(*client.Config) ioTDBSession {
		return sessions[int(created.Add(1))-1]
	})
	logger := &testutil.CaptureLogger{}
	plugin.Log = logger

	for range sessions {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		require.ErrorIs(t, plugin.WriteContext(ctx, testutil.MockMetrics()), context.DeadlineExceeded)
		cancel()
	}

	start := time.Now()
	err := plugin.WriteContext(t.Context(), testutil.MockMetrics())
	require.ErrorContains(t, err, "replacement limit reached")
	require.Less(t, time.Since(start), 100*time.Millisecond)
	require.Equal(t, int32(2), created.Load())
	require.Contains(t, logger.Errors()[0], "IoTDB session replacement limit reached")

	require.Error(t, plugin.WriteContext(t.Context(), testutil.MockMetrics()))

	for _, block := range blocks {
		close(block)
	}
}

func TestConnectContextBoundsSessionCreation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	session := &controlledSession{}
	var once atomic.Bool
	plugin := newTestPlugin(t, func(*client.Config) ioTDBSession {
		if once.CompareAndSwap(false, true) {
			close(started)
			<-release
		}
		return session
	})

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := plugin.ConnectContext(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 500*time.Millisecond)

	close(release)
	require.Eventually(t, func() bool {
		plugin.sessionMu.Lock()
		defer plugin.sessionMu.Unlock()
		return plugin.session == session
	}, time.Second, time.Millisecond)
	require.NoError(t, plugin.Close())
}

func TestConnectContextCancellationBeforeCall(t *testing.T) {
	plugin := newTestPlugin(t, func(*client.Config) ioTDBSession {
		t.Fatal("session must not be created once the context is already done")
		return nil
	})

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	require.ErrorIs(t, plugin.ConnectContext(ctx), context.Canceled)
}

func TestClose(t *testing.T) {
	session := &controlledSession{}
	plugin := newTestPlugin(t, func(*client.Config) ioTDBSession {
		return session
	})
	require.NoError(t, plugin.ConnectContext(t.Context()))
	require.NoError(t, plugin.Close())
	require.Equal(t, int32(1), session.closes.Load())

	// Close on a never-connected plugin must be a no-op, not a panic.
	plugin2 := newTestPlugin(t, func(*client.Config) ioTDBSession {
		return &controlledSession{}
	})
	require.NoError(t, plugin2.Close())
}

func TestCloseTimesOutOnStuckSession(t *testing.T) {
	closeBlock := make(chan struct{})
	defer close(closeBlock)
	session := &controlledSession{closeBlock: closeBlock}
	plugin := newTestPlugin(t, func(*client.Config) ioTDBSession {
		return session
	})
	plugin.closeTimeout = 20 * time.Millisecond
	require.NoError(t, plugin.ConnectContext(t.Context()))

	start := time.Now()
	err := plugin.Close()
	require.ErrorContains(t, err, "timed out")
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

// TestCloseDoesNotRaceAbandonedSession proves Close() (which only ever
// touches the current, live session) remains safe to call while a
// previously abandoned session's InsertRecords call is still running in
// the background -- the two never touch the same Session instance, so
// there is no concurrent access even though both are "in flight" at once.
func TestCloseDoesNotRaceAbandonedSession(t *testing.T) {
	oldInsertBlock := make(chan struct{})
	oldSession := &controlledSession{insertBlock: oldInsertBlock}
	newSession := &controlledSession{}
	sessions := []ioTDBSession{oldSession, newSession}
	var created atomic.Int32
	plugin := newTestPlugin(t, func(*client.Config) ioTDBSession {
		return sessions[int(created.Add(1))-1]
	})

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, plugin.WriteContext(ctx, testutil.MockMetrics()), context.DeadlineExceeded)

	require.NoError(t, plugin.ConnectContext(t.Context()))
	require.NoError(t, plugin.Close())
	require.Equal(t, int32(1), newSession.closes.Load())

	// The abandoned old session must still be untouched until its call
	// returns.
	require.Zero(t, oldSession.closes.Load())
	close(oldInsertBlock)
	require.Eventually(t, func() bool { return oldSession.closes.Load() == 1 }, time.Second, time.Millisecond)
}

// A Session.Open() that never returns must not wedge every later connection
// attempt: the timed-out attempt is abandoned so the next Connect starts a
// fresh session.
func TestConnectContextRecoversFromHungOpen(t *testing.T) {
	hang := make(chan struct{})
	defer close(hang)

	hung := &controlledSession{openBlock: hang}
	healthy := &controlledSession{}

	var calls atomic.Int32
	plugin := newTestPlugin(t, func(*client.Config) ioTDBSession {
		if calls.Add(1) == 1 {
			return hung
		}
		return healthy
	})

	// First attempt blocks inside Open() and is cut short by the deadline.
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, plugin.ConnectContext(ctx), context.DeadlineExceeded)
	require.Equal(t, int32(1), hung.opens.Load())

	// The second attempt must not join the still-hung Open().
	require.NoError(t, plugin.ConnectContext(t.Context()))
	require.Equal(t, int32(1), healthy.opens.Load())
	require.Equal(t, int32(1), hung.opens.Load())

	plugin.sessionMu.Lock()
	session := plugin.session
	plugin.sessionMu.Unlock()
	require.Same(t, healthy, session)
}

// Each abandoned Open() holds a poisoned-session slot, so repeated timeouts
// cannot accumulate hung goroutines without bound.
func TestConnectContextBoundsAbandonedOpens(t *testing.T) {
	hang := make(chan struct{})
	defer close(hang)

	plugin := newTestPlugin(t, func(*client.Config) ioTDBSession {
		return &controlledSession{openBlock: hang}
	})

	for i := 0; i < maxPoisonedSessions; i++ {
		ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
		require.ErrorIs(t, plugin.ConnectContext(ctx), context.DeadlineExceeded)
		cancel()
	}

	// The cap is reached, so the next attempt fails fast instead of starting
	// yet another Open() that may never return.
	err := plugin.ConnectContext(t.Context())
	require.ErrorContains(t, err, "replacement limit reached")
}
