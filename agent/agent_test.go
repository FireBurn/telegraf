package agent

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/models"
	_ "github.com/influxdata/telegraf/plugins/aggregators/all"
	_ "github.com/influxdata/telegraf/plugins/inputs/all"
	_ "github.com/influxdata/telegraf/plugins/outputs/all"
	"github.com/influxdata/telegraf/plugins/parsers/influx"
	_ "github.com/influxdata/telegraf/plugins/processors/all"
	"github.com/influxdata/telegraf/testutil"
)

func TestAgent_OmitHostname(t *testing.T) {
	c := config.NewConfig()
	c.Agent.OmitHostname = true
	_ = NewAgent(c)
	require.NotContains(t, c.Tags, "host")
}

func TestAgent_LoadPlugin(t *testing.T) {
	c := config.NewConfig()
	c.InputFilters = []string{"mysql"}
	err := c.LoadConfig("../config/testdata/telegraf-agent.toml")
	require.NoError(t, err)
	a := NewAgent(c)
	require.Len(t, a.Config.Inputs, 1)

	c = config.NewConfig()
	c.InputFilters = []string{"foo"}
	err = c.LoadConfig("../config/testdata/telegraf-agent.toml")
	require.NoError(t, err)
	a = NewAgent(c)
	require.Empty(t, a.Config.Inputs)

	c = config.NewConfig()
	c.InputFilters = []string{"mysql", "foo"}
	err = c.LoadConfig("../config/testdata/telegraf-agent.toml")
	require.NoError(t, err)
	a = NewAgent(c)
	require.Len(t, a.Config.Inputs, 1)

	c = config.NewConfig()
	c.InputFilters = []string{"mysql", "redis"}
	err = c.LoadConfig("../config/testdata/telegraf-agent.toml")
	require.NoError(t, err)
	a = NewAgent(c)
	require.Len(t, a.Config.Inputs, 2)

	c = config.NewConfig()
	c.InputFilters = []string{"mysql", "foo", "redis", "bar"}
	err = c.LoadConfig("../config/testdata/telegraf-agent.toml")
	require.NoError(t, err)
	a = NewAgent(c)
	require.Len(t, a.Config.Inputs, 2)
}

func TestAgent_LoadOutput(t *testing.T) {
	c := config.NewConfig()
	c.OutputFilters = []string{"influxdb"}
	err := c.LoadConfig("../config/testdata/telegraf-agent.toml")
	require.NoError(t, err)
	a := NewAgent(c)
	require.Len(t, a.Config.Outputs, 2)

	c = config.NewConfig()
	c.OutputFilters = []string{"kafka"}
	err = c.LoadConfig("../config/testdata/telegraf-agent.toml")
	require.NoError(t, err)
	a = NewAgent(c)
	require.Len(t, a.Config.Outputs, 1)

	c = config.NewConfig()
	err = c.LoadConfig("../config/testdata/telegraf-agent.toml")
	require.NoError(t, err)
	a = NewAgent(c)
	require.Len(t, a.Config.Outputs, 3)

	c = config.NewConfig()
	c.OutputFilters = []string{"foo"}
	err = c.LoadConfig("../config/testdata/telegraf-agent.toml")
	require.NoError(t, err)
	a = NewAgent(c)
	require.Empty(t, a.Config.Outputs)

	c = config.NewConfig()
	c.OutputFilters = []string{"influxdb", "foo"}
	err = c.LoadConfig("../config/testdata/telegraf-agent.toml")
	require.NoError(t, err)
	a = NewAgent(c)
	require.Len(t, a.Config.Outputs, 2)

	c = config.NewConfig()
	c.OutputFilters = []string{"influxdb", "kafka"}
	err = c.LoadConfig("../config/testdata/telegraf-agent.toml")
	require.NoError(t, err)
	require.Len(t, c.Outputs, 3)
	a = NewAgent(c)
	require.Len(t, a.Config.Outputs, 3)

	c = config.NewConfig()
	c.OutputFilters = []string{"influxdb", "foo", "kafka", "bar"}
	err = c.LoadConfig("../config/testdata/telegraf-agent.toml")
	require.NoError(t, err)
	a = NewAgent(c)
	require.Len(t, a.Config.Outputs, 3)
}

func TestWindow(t *testing.T) {
	parse := func(s string) time.Time {
		tm, err := time.Parse(time.RFC3339, s)
		if err != nil {
			panic(err)
		}
		return tm
	}

	tests := []struct {
		name          string
		start         time.Time
		roundInterval bool
		period        time.Duration
		since         time.Time
		until         time.Time
	}{
		{
			name:          "round with exact alignment",
			start:         parse("2018-03-27T00:00:00Z"),
			roundInterval: true,
			period:        30 * time.Second,
			since:         parse("2018-03-27T00:00:00Z"),
			until:         parse("2018-03-27T00:00:30Z"),
		},
		{
			name:          "round with alignment needed",
			start:         parse("2018-03-27T00:00:05Z"),
			roundInterval: true,
			period:        30 * time.Second,
			since:         parse("2018-03-27T00:00:00Z"),
			until:         parse("2018-03-27T00:00:30Z"),
		},
		{
			name:          "no round with exact alignment",
			start:         parse("2018-03-27T00:00:00Z"),
			roundInterval: false,
			period:        30 * time.Second,
			since:         parse("2018-03-27T00:00:00Z"),
			until:         parse("2018-03-27T00:00:30Z"),
		},
		{
			name:          "no found with alignment needed",
			start:         parse("2018-03-27T00:00:05Z"),
			roundInterval: false,
			period:        30 * time.Second,
			since:         parse("2018-03-27T00:00:05Z"),
			until:         parse("2018-03-27T00:00:35Z"),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			since, until := updateWindow(tt.start, tt.roundInterval, tt.period)
			require.Equal(t, tt.since, since, "since")
			require.Equal(t, tt.until, until, "until")
		})
	}
}

func TestCases(t *testing.T) {
	// Get all directories in testcases
	folders, err := os.ReadDir("testcases")
	require.NoError(t, err)

	// Make sure tests contains data
	require.NotEmpty(t, folders)

	for _, f := range folders {
		// Only handle folders
		if !f.IsDir() {
			continue
		}

		fname := f.Name()
		testdataPath := filepath.Join("testcases", fname)
		configFilename := filepath.Join(testdataPath, "telegraf.conf")
		expectedFilename := filepath.Join(testdataPath, "expected.out")

		t.Run(fname, func(t *testing.T) {
			// Get parser to parse input and expected output
			parser := &influx.Parser{}
			require.NoError(t, parser.Init())

			expected, err := testutil.ParseMetricsFromFile(expectedFilename, parser)
			require.NoError(t, err)
			require.NotEmpty(t, expected)

			// Load the config and inject the mock output to be able to verify
			// the resulting metrics
			cfg := config.NewConfig()
			require.NoError(t, cfg.LoadAll(configFilename))
			require.Empty(t, cfg.Outputs, "No output(s) allowed in the config!")

			// Setup the agent and run the agent in "once" mode
			agent := NewAgent(cfg)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			actual, err := collect(ctx, agent, 0)
			require.NoError(t, err)

			// Process expected metrics and compare with resulting metrics
			options := []cmp.Option{
				testutil.IgnoreTags("host"),
				testutil.IgnoreTime(),
			}
			testutil.RequireMetricsEqual(t, expected, actual, options...)
		})
	}
}

func TestAgent_SkipProcessorsAfterAggregatorsDefaultsFalse(t *testing.T) {
	c := config.NewConfig()
	c.Agent.OmitHostname = true

	a := NewAgent(c)
	ctx, cancel := context.WithTimeout(t.Context(), 1*time.Second)
	defer cancel()

	require.NoError(t, a.Run(ctx))
	require.NotNil(t, c.Agent.SkipProcessorsAfterAggregators)
	require.False(t, *c.Agent.SkipProcessorsAfterAggregators)
}

// Implement a "test-mode" like call but collect the metrics
func collect(ctx context.Context, a *Agent, wait time.Duration) ([]telegraf.Metric, error) {
	var received []telegraf.Metric
	var mu sync.Mutex

	src := make(chan telegraf.Metric, 100)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for m := range src {
			mu.Lock()
			received = append(received, m)
			mu.Unlock()
			m.Reject()
		}
	}()

	if err := a.runTest(ctx, wait, src); err != nil {
		return nil, err
	}
	wg.Wait()

	if models.GlobalGatherErrors.Get() != 0 {
		return received, fmt.Errorf("input plugins recorded %d errors", models.GlobalGatherErrors.Get())
	}
	return received, nil
}

func TestFinalFlushContext(t *testing.T) {
	shutdownCtx, shutdown := context.WithCancel(t.Context())
	shutdown()

	t.Run("context-aware default", func(t *testing.T) {
		output := &models.RunningOutput{Output: &contextOutput{}, Config: &models.OutputConfig{}}
		ctx, cancel := finalFlushContext(shutdownCtx, output, 100*time.Millisecond)
		defer cancel()
		require.NoError(t, ctx.Err())
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.WithinDuration(t, time.Now().Add(100*time.Millisecond), deadline, 25*time.Millisecond)
	})

	t.Run("context-aware configured timeout", func(t *testing.T) {
		output := &models.RunningOutput{
			Output: &contextOutput{},
			Config: &models.OutputConfig{WriteTimeout: 250 * time.Millisecond},
		}
		ctx, cancel := finalFlushContext(shutdownCtx, output, time.Second)
		defer cancel()
		require.NoError(t, ctx.Err())
		_, ok := ctx.Deadline()
		require.False(t, ok, "flushOnce should apply the single configured deadline")
	})

	t.Run("legacy", func(t *testing.T) {
		output := &models.RunningOutput{Output: &legacyOutput{}, Config: &models.OutputConfig{}}
		ctx, cancel := finalFlushContext(shutdownCtx, output, 100*time.Millisecond)
		defer cancel()
		require.ErrorIs(t, ctx.Err(), context.Canceled)
	})
}

func TestFinalFlushContextBoundsWrite(t *testing.T) {
	entered := make(chan struct{}, 1)
	plugin := &contextOutput{entered: entered}
	output := &models.RunningOutput{Output: plugin, Config: &models.OutputConfig{}}
	ctx, cancel := finalFlushContext(context.Background(), output, 25*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := plugin.WriteContext(ctx, nil)
	require.NotEmpty(t, entered)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 500*time.Millisecond)
}

func TestConnectOutputCancellation(t *testing.T) {
	plugin := &connectContextOutput{entered: make(chan struct{})}
	output, err := models.NewRunningOutput(plugin, &models.OutputConfig{Name: "test"}, 5, 10)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- (&Agent{}).connectOutput(ctx, output) }()
	<-plugin.entered
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
}

type legacyOutput struct{}

func (*legacyOutput) Connect() error                { return nil }
func (*legacyOutput) Close() error                  { return nil }
func (*legacyOutput) SampleConfig() string          { return "" }
func (*legacyOutput) Write([]telegraf.Metric) error { return nil }
func (*legacyOutput) Description() string           { return "" }
func (*legacyOutput) Init() error                   { return nil }

type contextOutput struct {
	legacyOutput
	entered chan<- struct{}
}

type connectContextOutput struct {
	legacyOutput
	entered chan struct{}
}

func (o *connectContextOutput) ConnectContext(ctx context.Context) error {
	close(o.entered)
	<-ctx.Done()
	return ctx.Err()
}

func (o *contextOutput) WriteContext(ctx context.Context, _ []telegraf.Metric) error {
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("context cancelled before final write: %w", err)
	}
	if o.entered != nil {
		o.entered <- struct{}{}
	}
	<-ctx.Done()
	return ctx.Err()
}
