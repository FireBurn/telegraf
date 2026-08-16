//go:generate ../../../tools/readme_config_includer/generator
package nsq

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nsqio/go-nsq"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

// Allow one replacement attempt while cleanup of the original producer is
// stuck, but do not accumulate producers indefinitely on repeated timeouts.
const (
	maxPoisonedProducers = 2
	defaultCloseTimeout  = 5 * time.Second
)

// nsqProducer is the subset of *nsq.Producer used by this plugin, extracted
// so tests can substitute a controllable fake instead of dialing a real
// nsqd.
type nsqProducer interface {
	Publish(topic string, body []byte) error
	Stop()
}

type NSQ struct {
	Server string
	Topic  string
	Log    telegraf.Logger `toml:"-"`

	serializer telegraf.Serializer

	// producerFunc constructs the producer. It is a field (rather than a
	// direct call to nsq.NewProducer) so tests can substitute a fake that
	// never touches the network.
	producerFunc func(addr string, config *nsq.Config) (nsqProducer, error)

	producerMu   sync.Mutex
	producer     nsqProducer
	producerGen  uint64
	poisoned     int
	limitLogged  bool
	closed       bool
	closeTimeout time.Duration
}

func (*NSQ) SampleConfig() string {
	return sampleConfig
}

func (n *NSQ) SetSerializer(serializer telegraf.Serializer) {
	n.serializer = serializer
}

func (n *NSQ) Connect() error {
	return n.ConnectContext(context.Background())
}

// ConnectContext acquires (creating if necessary) the NSQ producer used for
// writes. Unlike outputs.kafka, go-nsq's Producer.NewProducer performs no
// I/O -- it only validates the config and allocates the Producer struct;
// the actual TCP dial and handshake happen lazily inside the first
// Publish/MultiPublish call. So producer creation itself cannot hang and
// does not need the single-flight creation race outputs.kafka uses; only
// Publish (in WriteContext) does.
func (n *NSQ) ConnectContext(ctx context.Context) error {
	_, _, err := n.acquireProducer(ctx)
	return err
}

func (n *NSQ) Close() error {
	n.producerMu.Lock()
	n.closed = true
	producer := n.producer
	n.producer = nil
	n.producerMu.Unlock()
	if producer == nil {
		return nil
	}
	return n.stopProducer(producer)
}

// acquireProducer returns the current live producer, creating one if none
// exists. Creation is local/non-blocking (see ConnectContext), so this can
// run inline under the lock rather than racing a goroutine like
// outputs.kafka's acquireProducer does.
func (n *NSQ) acquireProducer(ctx context.Context) (nsqProducer, uint64, error) {
	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	n.producerMu.Lock()
	defer n.producerMu.Unlock()

	if n.closed {
		return nil, 0, errors.New("nsq output is closed")
	}
	if n.producer != nil {
		return n.producer, n.producerGen, nil
	}
	if n.poisoned >= maxPoisonedProducers {
		if !n.limitLogged {
			n.Log.Errorf("NSQ producer replacement limit reached; refusing to create another producer because previous producers are still shutting down")
			n.limitLogged = true
		}
		return nil, 0, errors.New("nsq producer replacement limit reached while previous producers are still shutting down")
	}

	if n.producerFunc == nil {
		n.producerFunc = defaultProducerFunc
	}

	config := nsq.NewConfig()
	producer, err := n.producerFunc(n.Server, config)
	if err != nil {
		return nil, 0, &internal.StartupError{Err: err, Retry: true}
	}

	n.producer = producer
	n.producerGen++
	return n.producer, n.producerGen, nil
}

func (n *NSQ) stopProducer(producer nsqProducer) error {
	done := make(chan struct{})
	go func() {
		producer.Stop()
		close(done)
	}()
	timeout := n.producerCloseTimeout()
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case <-done:
		return nil
	case <-timer.C:
		return fmt.Errorf("closing NSQ producer timed out after %s", timeout)
	}
}

func (n *NSQ) producerCloseTimeout() time.Duration {
	if n.closeTimeout > 0 {
		return n.closeTimeout
	}
	return defaultCloseTimeout
}

// Write writes the metrics to NSQ. It ignores context cancellation.
func (n *NSQ) Write(metrics []telegraf.Metric) error {
	return n.WriteContext(context.Background(), metrics)
}

// WriteContext writes the metrics to NSQ. It can be cancelled via the
// context. go-nsq's Publish is synchronous with no context support, so an
// in-flight Publish is raced in a goroutine; on cancellation the producer
// that owned it is detached (poisoned) and closed in the background, and
// the next write creates a fresh one.
func (n *NSQ) WriteContext(ctx context.Context, metrics []telegraf.Metric) error {
	if len(metrics) == 0 {
		return nil
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	producer, generation, err := n.acquireProducer(ctx)
	if err != nil {
		return err
	}

	for _, metric := range metrics {
		buf, err := n.serializer.Serialize(metric)
		if err != nil {
			n.Log.Debugf("Could not serialize metric: %v", err)
			continue
		}

		done := make(chan error, 1)
		go func() {
			done <- producer.Publish(n.Topic, buf)
		}()

		select {
		case err := <-done:
			if err != nil {
				return fmt.Errorf("failed to send NSQD message: %w", err)
			}
		case <-ctx.Done():
			n.poisonProducer(producer, generation, done)
			return ctx.Err()
		}
	}
	return nil
}

// poisonProducer detaches a producer whose in-flight Publish outcome is
// unknown. go-nsq's Producer synchronizes its internal state via channels
// consumed by its own router goroutine, and documents Stop() as safe to
// call while commands are in flight, so -- unlike outputs.iotdb's Thrift
// session -- it is safe to close this producer concurrently with the
// abandoned Publish call rather than waiting for it to finish first.
func (n *NSQ) poisonProducer(producer nsqProducer, generation uint64, publishDone <-chan error) {
	n.producerMu.Lock()
	if n.producerGen != generation || n.producer == nil {
		n.producerMu.Unlock()
		return
	}
	n.producer = nil
	n.poisoned++
	n.producerMu.Unlock()

	stopDone := make(chan struct{})
	go func() {
		producer.Stop()
		close(stopDone)
	}()
	go func() {
		<-publishDone
		<-stopDone
		n.producerMu.Lock()
		n.poisoned--
		n.limitLogged = false
		n.producerMu.Unlock()
	}()
}

func defaultProducerFunc(addr string, config *nsq.Config) (nsqProducer, error) {
	return nsq.NewProducer(addr, config)
}

func init() {
	outputs.Add("nsq", func() telegraf.Output {
		return &NSQ{producerFunc: defaultProducerFunc}
	})
}
