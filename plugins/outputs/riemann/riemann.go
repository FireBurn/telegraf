//go:generate ../../../tools/readme_config_includer/generator
package riemann

import (
	"context"
	_ "embed"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/amir/raidman"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

type Riemann struct {
	URL                    string          `toml:"url"`
	TTL                    float32         `toml:"ttl"`
	Separator              string          `toml:"separator"`
	MeasurementAsAttribute bool            `toml:"measurement_as_attribute"`
	StringAsState          bool            `toml:"string_as_state"`
	TagKeys                []string        `toml:"tag_keys"`
	Tags                   []string        `toml:"tags"`
	DescriptionText        string          `toml:"description_text"`
	Timeout                config.Duration `toml:"timeout"`
	Log                    telegraf.Logger `toml:"-"`

	client *raidman.Client
}

func (*Riemann) SampleConfig() string {
	return sampleConfig
}

func (r *Riemann) Connect() error {
	return r.ConnectContext(context.Background())
}

// ConnectContext dials the Riemann server, returning early if ctx is
// cancelled. raidman.DialWithTimeout has no context support, but is
// already bounded by r.Timeout, so a cancelled dial abandons the
// in-progress call (bounded by r.Timeout) and closes any client it
// eventually produces.
func (r *Riemann) ConnectContext(ctx context.Context) error {
	parsedURL, err := url.Parse(r.URL)
	if err != nil {
		return err
	}

	type dialResult struct {
		client *raidman.Client
		err    error
	}
	resultCh := make(chan dialResult, 1)
	go func() {
		client, err := raidman.DialWithTimeout(parsedURL.Scheme, parsedURL.Host, time.Duration(r.Timeout))
		resultCh <- dialResult{client, err}
	}()

	select {
	case res := <-resultCh:
		if res.err != nil {
			r.client = nil
			return res.err
		}
		r.client = res.client
		return nil
	case <-ctx.Done():
		go func() {
			if res := <-resultCh; res.client != nil {
				res.client.Close()
			}
		}()
		return ctx.Err()
	}
}

func (r *Riemann) Close() (err error) {
	if r.client != nil {
		err = r.client.Close()
		r.client = nil
	}
	return err
}

func (r *Riemann) Write(metrics []telegraf.Metric) error {
	return r.WriteContext(context.Background(), metrics)
}

// WriteContext sends events to Riemann, returning early if ctx is
// cancelled. raidman.Client has no context support, and its SendMulti
// already applies its own SetDeadline(r.Timeout) before writing/reading,
// so a cancelled write abandons the in-progress call (bounded by
// r.Timeout) rather than interrupting it mid-flight -- the client holds
// its internal lock for the call's duration, so forcing Close() from this
// goroutine would just block behind that lock instead of unblocking
// anything.
func (r *Riemann) WriteContext(ctx context.Context, metrics []telegraf.Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	if r.client == nil {
		if err := r.ConnectContext(ctx); err != nil {
			return fmt.Errorf("failed to (re)connect to Riemann: %w", err)
		}
	}

	// build list of Riemann events to send
	var events []*raidman.Event
	for _, m := range metrics {
		evs := r.buildRiemannEvents(m)
		events = append(events, evs...)
	}

	client := r.client
	done := make(chan error, 1)
	go func() {
		done <- client.SendMulti(events)
	}()

	select {
	case err := <-done:
		if err != nil {
			r.Close()
			return fmt.Errorf("failed to send riemann message: %w", err)
		}
		return nil
	case <-ctx.Done():
		// Detach this client rather than blocking on Close() here; close
		// it in the background once the abandoned call returns.
		r.client = nil
		go func() {
			<-done
			client.Close()
		}()
		return ctx.Err()
	}
}

func (r *Riemann) buildRiemannEvents(m telegraf.Metric) []*raidman.Event {
	events := make([]*raidman.Event, 0, len(m.Fields()))
	for fieldName, value := range m.Fields() {
		// get host for Riemann event
		host, ok := m.Tags()["host"]
		if !ok {
			if hostname, err := os.Hostname(); err == nil {
				host = hostname
			} else {
				host = "unknown"
			}
		}

		event := &raidman.Event{
			Host:        host,
			Ttl:         r.TTL,
			Description: r.DescriptionText,
			Time:        m.Time().Unix(),

			Attributes: r.attributes(m.Name(), m.Tags()),
			Service:    r.service(m.Name(), fieldName),
			Tags:       r.tags(m.Tags()),
		}

		switch value := value.(type) {
		case string:
			// only send string metrics if explicitly enabled, skip otherwise
			if !r.StringAsState {
				r.Log.Debugf("Riemann event states disabled, skipping metric value [%s]", value)
				continue
			}
			event.State = value
		case int, int64, uint64, float32, float64:
			event.Metric = value
		default:
			r.Log.Debugf("Riemann does not support metric value [%s]", value)
			continue
		}

		events = append(events, event)
	}
	return events
}

func (r *Riemann) attributes(name string, tags map[string]string) map[string]string {
	if r.MeasurementAsAttribute {
		tags["measurement"] = name
	}

	delete(tags, "host") // exclude 'host' tag
	return tags
}

func (r *Riemann) service(name, field string) string {
	var serviceStrings []string

	// if measurement is not enabled as an attribute then prepend it to service name
	if !r.MeasurementAsAttribute {
		serviceStrings = append(serviceStrings, name)
	}
	serviceStrings = append(serviceStrings, field)

	return strings.Join(serviceStrings, r.Separator)
}

func (r *Riemann) tags(tags map[string]string) []string {
	// always add specified Riemann tags
	values := r.Tags

	// if tag_keys are specified, add those and return tag list
	if len(r.TagKeys) > 0 {
		for _, tagName := range r.TagKeys {
			value, ok := tags[tagName]
			if ok {
				values = append(values, value)
			}
		}
		return values
	}

	// otherwise add all values from telegraf tag key/value pairs
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		if key != "host" { // exclude 'host' tag
			values = append(values, tags[key])
		}
	}
	return values
}

func init() {
	outputs.Add("riemann", func() telegraf.Output {
		return &Riemann{
			Timeout: config.Duration(time.Second * 5),
		}
	})
}
