package internal

import (
	"context"
	"net"
	"sync"
	"time"
)

// CancelConnOnContext arranges for c's deadlines to expire as soon as ctx is
// cancelled, which is the only way to unblock an in-flight net.Conn operation:
// net.Conn has no context-aware read/write API.
//
// The caller must invoke the returned stop function once the operation has
// returned, and must pass the exact connection the operation runs on rather
// than re-reading a field that a concurrent error path may have already closed
// and replaced.
//
// stop waits for the watcher to exit and reports whether the deadline was
// actually forced. A true result means the connection now carries an expired
// deadline, so every subsequent operation on it would fail immediately; the
// caller must discard the connection instead of returning it to the pool or
// leaving it on the plugin struct. This includes the case where cancellation
// raced a successful operation, which would otherwise leave a live connection
// permanently poisoned.
//
// stop is idempotent.
func CancelConnOnContext(ctx context.Context, c net.Conn) (stop func() bool) {
	done := make(chan struct{})
	exited := make(chan struct{})

	// Written before exited is closed and read only after it, so no
	// synchronization beyond the channel is required.
	var cancelled bool

	go func() {
		defer close(exited)
		select {
		case <-ctx.Done():
			cancelled = true
			//nolint:errcheck // best-effort interrupt; the connection may already be closed
			c.SetDeadline(time.Now())
		case <-done:
		}
	}()

	var once sync.Once
	return func() bool {
		once.Do(func() { close(done) })
		<-exited
		return cancelled
	}
}
