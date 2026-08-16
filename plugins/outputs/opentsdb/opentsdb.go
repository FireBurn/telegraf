//go:generate ../../../tools/readme_config_includer/generator
package opentsdb

import (
	"context"
	_ "embed"
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

var (
	allowedChars = regexp.MustCompile(`[^a-zA-Z0-9-_./\p{L}]`)
	hyphenChars  = strings.NewReplacer(
		"@", "-",
		"*", "-",
		`%`, "-",
		"#", "-",
		"$", "-")
	defaultHTTPPath  = "/api/put"
	defaultSeparator = "_"
)

type OpenTSDB struct {
	Prefix string `toml:"prefix"`

	Host string `toml:"host"`
	Port int    `toml:"port"`

	HTTPBatchSize int    `toml:"http_batch_size"`
	HTTPPath      string `toml:"http_path"`

	Debug bool `toml:"debug"`

	Separator string `toml:"separator"`

	Log telegraf.Logger `toml:"-"`
}

func ToLineFormat(tags map[string]string) string {
	tagsArray := make([]string, 0, len(tags))
	for k, v := range tags {
		tagsArray = append(tagsArray, fmt.Sprintf("%s=%s", k, v))
	}
	sort.Strings(tagsArray)
	return strings.Join(tagsArray, " ")
}

func (*OpenTSDB) SampleConfig() string {
	return sampleConfig
}

func (o *OpenTSDB) Connect() error {
	return o.ConnectContext(context.Background())
}

// ConnectContext probes connectivity to the OpenTSDB server, aborting the
// dial when ctx is cancelled.
func (o *OpenTSDB) ConnectContext(ctx context.Context) error {
	if !strings.HasPrefix(o.Host, "http") && !strings.HasPrefix(o.Host, "tcp") {
		o.Host = "tcp://" + o.Host
	}
	// Test Connection to OpenTSDB Server
	u, err := url.Parse(o.Host)
	if err != nil {
		return fmt.Errorf("error in parsing host url: %w", err)
	}

	uri := fmt.Sprintf("%s:%d", u.Host, o.Port)
	var d net.Dialer
	connection, err := d.DialContext(ctx, "tcp", uri)
	if err != nil {
		return fmt.Errorf("failed to connect to OpenTSDB: %w", err)
	}
	defer connection.Close()
	return nil
}

func (o *OpenTSDB) Write(metrics []telegraf.Metric) error {
	return o.WriteContext(context.Background(), metrics)
}

// WriteContext writes metrics, aborting an in-flight dial/write/HTTP
// request when ctx is cancelled.
func (o *OpenTSDB) WriteContext(ctx context.Context, metrics []telegraf.Metric) error {
	if len(metrics) == 0 {
		return nil
	}

	u, err := url.Parse(o.Host)
	if err != nil {
		return fmt.Errorf("error in parsing host url: %w", err)
	}

	if u.Scheme == "" || u.Scheme == "tcp" {
		return o.WriteTelnet(ctx, metrics, u)
	} else if u.Scheme == "http" || u.Scheme == "https" {
		return o.WriteHTTP(ctx, metrics, u)
	}
	return errors.New("unknown scheme in host parameter")
}

func (o *OpenTSDB) WriteHTTP(ctx context.Context, metrics []telegraf.Metric, u *url.URL) error {
	http := openTSDBHttp{
		Host:      u.Host,
		Port:      o.Port,
		Scheme:    u.Scheme,
		User:      u.User,
		BatchSize: o.HTTPBatchSize,
		Path:      o.HTTPPath,
		Debug:     o.Debug,
		log:       o.Log,
	}

	for _, m := range metrics {
		now := m.Time().UnixNano() / 1000000000
		tags := cleanTags(m.Tags())

		for fieldName, value := range m.Fields() {
			switch fv := value.(type) {
			case int64:
			case uint64:
			case float64:
				// JSON does not support these special values
				if math.IsNaN(fv) || math.IsInf(fv, 0) {
					continue
				}
			default:
				o.Log.Debugf("OpenTSDB does not support metric value: [%s] of type [%T].", value, value)
				continue
			}

			metric := &HTTPMetric{
				Metric: sanitize(fmt.Sprintf("%s%s%s%s",
					o.Prefix, m.Name(), o.Separator, fieldName)),
				Tags:      tags,
				Timestamp: now,
				Value:     value,
			}

			if err := http.sendDataPoint(ctx, metric); err != nil {
				return err
			}
		}
	}

	return http.flush(ctx)
}

func (o *OpenTSDB) WriteTelnet(ctx context.Context, metrics []telegraf.Metric, u *url.URL) error {
	// Send Data with telnet / socket communication
	uri := fmt.Sprintf("%s:%d", u.Host, o.Port)
	var d net.Dialer
	connection, err := d.DialContext(ctx, "tcp", uri)
	if err != nil {
		return fmt.Errorf("failed to connect to OpenTSDB: %w", err)
	}
	defer connection.Close()

	return o.writeTelnetConn(ctx, connection, metrics)
}

// writeTelnetConn writes metrics to an already-established connection,
// forcing an in-flight write to abort via a deadline on cancellation;
// net.Conn has no native context support.
func (o *OpenTSDB) writeTelnetConn(ctx context.Context, connection net.Conn, metrics []telegraf.Metric) error {
	// The connection is closed by the caller once this returns, so a forced
	// deadline cannot poison a later write; stop() is only needed to retire
	// the watcher before that close happens.
	stop := internal.CancelConnOnContext(ctx, connection)
	defer stop()

	for _, m := range metrics {
		now := m.Time().UnixNano() / 1000000000
		tags := ToLineFormat(cleanTags(m.Tags()))

		for fieldName, value := range m.Fields() {
			switch fv := value.(type) {
			case int64:
			case uint64:
			case float64:
				// JSON does not support these special values
				if math.IsNaN(fv) || math.IsInf(fv, 0) {
					continue
				}
			default:
				o.Log.Debugf("OpenTSDB does not support metric value: [%s] of type [%T].", value, value)
				continue
			}

			metricValue, buildError := buildValue(value)
			if buildError != nil {
				o.Log.Errorf("OpenTSDB: %s", buildError.Error())
				continue
			}

			messageLine := fmt.Sprintf("put %s %v %s %s\n",
				sanitize(fmt.Sprintf("%s%s%s%s", o.Prefix, m.Name(), o.Separator, fieldName)),
				now, metricValue, tags)

			if _, err := connection.Write([]byte(messageLine)); err != nil {
				return fmt.Errorf("telnet writing error: %w", err)
			}
		}
	}

	return nil
}

func cleanTags(tags map[string]string) map[string]string {
	tagSet := make(map[string]string, len(tags))
	for k, v := range tags {
		val := sanitize(v)
		if val != "" {
			tagSet[sanitize(k)] = val
		}
	}
	return tagSet
}

func buildValue(v interface{}) (string, error) {
	var retv string
	switch p := v.(type) {
	case int64:
		retv = IntToString(p)
	case uint64:
		retv = UIntToString(p)
	case float64:
		retv = FloatToString(p)
	default:
		return retv, fmt.Errorf("unexpected type %T with value %v for OpenTSDB", v, v)
	}
	return retv, nil
}

func IntToString(inputNum int64) string {
	return strconv.FormatInt(inputNum, 10)
}

func UIntToString(inputNum uint64) string {
	return strconv.FormatUint(inputNum, 10)
}

func FloatToString(inputNum float64) string {
	return strconv.FormatFloat(inputNum, 'f', 6, 64)
}

func (*OpenTSDB) Close() error {
	return nil
}

func sanitize(value string) string {
	// Apply special hyphenation rules to preserve backwards compatibility
	value = hyphenChars.Replace(value)
	// Replace any remaining illegal chars
	return allowedChars.ReplaceAllLiteralString(value, "_")
}

func init() {
	outputs.Add("opentsdb", func() telegraf.Output {
		return &OpenTSDB{
			HTTPPath:  defaultHTTPPath,
			Separator: defaultSeparator,
		}
	})
}
