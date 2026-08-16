//go:generate ../../../tools/readme_config_includer/generator
package discard

import (
	_ "embed"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/plugins/outputs"
)

//go:embed sample.conf
var sampleConfig string

type Discard struct{}

func (*Discard) SampleConfig() string {
	return sampleConfig
}

func (*Discard) Connect() error { return nil }
func (*Discard) Close() error   { return nil }

// Write is a no-op: this plugin does not implement
// OutputWithContext/OutputWithConnectContext (see
// docs/specs/tsd-012-output-context-aware-write.md) because there is no
// outbound call of any kind to bound with a write_timeout deadline.
func (*Discard) Write([]telegraf.Metric) error {
	return nil
}

func init() {
	outputs.Add("discard", func() telegraf.Output { return &Discard{} })
}
