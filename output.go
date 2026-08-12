package telegraf

import "context"

type Output interface {
	PluginDescriber

	// Connect to the Output; connect is only called once when the plugin starts
	Connect() error
	// Close any connections to the Output. Close is called once when the output
	// is shutting down. Close will not be called until all writes have finished,
	// and Write() will not be called once Close() has been, so locking is not
	// necessary.
	//
	// The one exception is an OutputWithContext implementation that detaches an
	// operation it cannot cancel: WriteContext may return on cancellation while
	// that operation is still running in a background goroutine, so for those
	// implementations Close can overlap with work started by an already-returned
	// WriteContext, and they are responsible for their own synchronization. See
	// docs/specs/tsd-012-output-context-aware-write.md.
	Close() error
	// Write takes in group of points to be written to the Output
	Write(metrics []Metric) error
}

// OutputWithContext is an output whose write operation can return on context cancellation.
type OutputWithContext interface {
	Output

	// WriteContext takes in group of points to be written to the Output.
	// It must return when the context is cancelled. The implementation is
	// responsible for any cleanup of an underlying operation that cannot be cancelled.
	WriteContext(ctx context.Context, metrics []Metric) error
}

// OutputWithConnectContext is an output whose connection attempt can be cancelled.
type OutputWithConnectContext interface {
	Output

	// ConnectContext performs any connection setup required for writing, like
	// Connect. It must return when the context is cancelled.
	ConnectContext(ctx context.Context) error
}

// AggregatingOutput adds aggregating functionality to an Output.  May be used
// if the Output only accepts a fixed set of aggregations over a time period.
// These functions may be called concurrently to the Write function.
type AggregatingOutput interface {
	Output

	// Add the metric to the aggregator
	Add(in Metric)
	// Push returns the aggregated metrics and is called every flush interval.
	Push() []Metric
	// Reset signals that the aggregator period is completed.
	Reset()
}
