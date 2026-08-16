package execd

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/plugins/parsers/influx"
	serializers_influx "github.com/influxdata/telegraf/plugins/serializers/influx"
	"github.com/influxdata/telegraf/testutil"
)

var now = time.Date(2020, 6, 30, 16, 16, 0, 0, time.UTC)

func TestExternalOutputWorks(t *testing.T) {
	serializer := &serializers_influx.Serializer{}
	require.NoError(t, serializer.Init())

	exe, err := os.Executable()
	require.NoError(t, err)

	e := &Execd{
		Command:      []string{exe, "-testoutput"},
		Environment:  []string{"PLUGINS_OUTPUTS_EXECD_MODE=application", "METRIC_NAME=cpu", "METRIC_NUM=1"},
		RestartDelay: config.Duration(5 * time.Second),
		serializer:   serializer,
		Log:          testutil.Logger{},
	}

	require.NoError(t, e.Init())

	wg := &sync.WaitGroup{}
	wg.Add(1)
	e.process.ReadStderrFn = func(rstderr io.Reader) {
		scanner := bufio.NewScanner(rstderr)

		for scanner.Scan() {
			t.Errorf("stderr: %q", scanner.Text())
		}

		if err := scanner.Err(); err != nil {
			if !strings.HasSuffix(err.Error(), "already closed") {
				t.Errorf("error reading stderr: %v", err)
			}
		}
		wg.Done()
	}

	m := metric.New(
		"cpu",
		map[string]string{"name": "cpu1"},
		map[string]interface{}{"idle": 50, "sys": 30},
		now,
	)

	require.NoError(t, e.Connect())
	require.NoError(t, e.Write([]telegraf.Metric{m}))
	require.NoError(t, e.Close())
	wg.Wait()
}

func TestBatchOutputWorks(t *testing.T) {
	serializer := &serializers_influx.Serializer{}
	require.NoError(t, serializer.Init())

	exe, err := os.Executable()
	require.NoError(t, err)

	e := &Execd{
		Command:        []string{exe, "-testoutput"},
		Environment:    []string{"PLUGINS_OUTPUTS_EXECD_MODE=application", "METRIC_NAME=cpu", "METRIC_NUM=2"},
		RestartDelay:   config.Duration(5 * time.Second),
		UseBatchFormat: true,
		serializer:     serializer,
		Log:            testutil.Logger{},
	}

	require.NoError(t, e.Init())

	wg := &sync.WaitGroup{}
	wg.Add(1)
	e.process.ReadStderrFn = func(rstderr io.Reader) {
		scanner := bufio.NewScanner(rstderr)

		for scanner.Scan() {
			t.Errorf("stderr: %q", scanner.Text())
		}

		if err := scanner.Err(); err != nil {
			if !strings.HasSuffix(err.Error(), "already closed") {
				t.Errorf("error reading stderr: %v", err)
			}
		}
		wg.Done()
	}

	m := metric.New(
		"cpu",
		map[string]string{"name": "cpu1"},
		map[string]interface{}{"idle": 50, "sys": 30},
		now,
	)

	m2 := metric.New(
		"cpu",
		map[string]string{"name": "cpu1"},
		map[string]interface{}{"idle": 50, "sys": 30},
		now,
	)

	require.NoError(t, e.Connect())
	require.NoError(t, e.Write([]telegraf.Metric{m, m2}))
	require.NoError(t, e.Close())
	wg.Wait()
}

// blockingWriteCloser is a fake stdin pipe that blocks on Write until
// unblock is closed, standing in for a subprocess that is alive but stuck
// and not consuming stdin. It lets the cancellation test below be
// deterministic instead of racing a real subprocess against a hang.
type blockingWriteCloser struct {
	unblock chan struct{}
}

func (w *blockingWriteCloser) Write(p []byte) (int, error) {
	<-w.unblock
	return len(p), nil
}

func (*blockingWriteCloser) Close() error {
	return nil
}

func TestWriteContextReturnsOnCancel(t *testing.T) {
	serializer := &serializers_influx.Serializer{}
	require.NoError(t, serializer.Init())

	e := &Execd{
		Command:      []string{"unused"},
		RestartDelay: config.Duration(5 * time.Second),
		serializer:   serializer,
		Log:          testutil.Logger{},
	}
	require.NoError(t, e.Init())

	// Stand in for a stuck subprocess without spawning a real process:
	// stdin.Write blocks until the test unblocks it.
	unblock := make(chan struct{})
	e.process.Stdin = &blockingWriteCloser{unblock: unblock}
	defer close(unblock) // let the abandoned write finish so it doesn't leak past the test

	m := metric.New(
		"cpu",
		map[string]string{"name": "cpu1"},
		map[string]interface{}{"idle": 50, "sys": 30},
		now,
	)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := e.WriteContext(ctx, []telegraf.Metric{m})
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, elapsed, 2*time.Second, "WriteContext should return promptly once the context is cancelled")
}

func TestPartiallyUnserializableThrowError(t *testing.T) {
	serializer := &serializers_influx.Serializer{}
	require.NoError(t, serializer.Init())

	exe, err := os.Executable()
	require.NoError(t, err)

	e := &Execd{
		Command:                  []string{exe, "-testoutput"},
		Environment:              []string{"PLUGINS_OUTPUTS_EXECD_MODE=application", "METRIC_NAME=cpu"},
		RestartDelay:             config.Duration(5 * time.Second),
		IgnoreSerializationError: false,
		serializer:               serializer,
		Log:                      testutil.Logger{},
	}

	require.NoError(t, e.Init())

	m1 := metric.New(
		"cpu",
		map[string]string{"name": "cpu1"},
		map[string]interface{}{"idle": 50, "sys": 30},
		now,
	)

	m2 := metric.New(
		"cpu",
		map[string]string{"name": "cpu2"},
		map[string]interface{}{},
		now,
	)

	require.NoError(t, e.Connect())
	require.Error(t, e.Write([]telegraf.Metric{m1, m2}))
	require.NoError(t, e.Close())
}

func TestPartiallyUnserializableCanBeSkipped(t *testing.T) {
	serializer := &serializers_influx.Serializer{}
	require.NoError(t, serializer.Init())

	exe, err := os.Executable()
	require.NoError(t, err)

	e := &Execd{
		Command:                  []string{exe, "-testoutput"},
		Environment:              []string{"PLUGINS_OUTPUTS_EXECD_MODE=application", "METRIC_NAME=cpu"},
		RestartDelay:             config.Duration(5 * time.Second),
		IgnoreSerializationError: true,
		serializer:               serializer,
		Log:                      testutil.Logger{},
	}

	require.NoError(t, e.Init())

	m1 := metric.New(
		"cpu",
		map[string]string{"name": "cpu1"},
		map[string]interface{}{"idle": 50, "sys": 30},
		now,
	)

	m2 := metric.New(
		"cpu",
		map[string]string{"name": "cpu2"},
		map[string]interface{}{},
		now,
	)

	require.NoError(t, e.Connect())
	require.NoError(t, e.Write([]telegraf.Metric{m1, m2}))
	require.NoError(t, e.Close())
}

var testoutput = flag.Bool("testoutput", false,
	"if true, act like line input program instead of test")

func TestMain(m *testing.M) {
	flag.Parse()
	runMode := os.Getenv("PLUGINS_OUTPUTS_EXECD_MODE")
	if *testoutput && runMode == "application" {
		runOutputConsumerProgram()
		os.Exit(0)
	}
	code := m.Run()
	os.Exit(code)
}

func runOutputConsumerProgram() {
	metricName := os.Getenv("METRIC_NAME")
	expectedMetrics, err := strconv.Atoi(os.Getenv("METRIC_NUM"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "could not parse METRIC_NUM\n")
		//nolint:revive // error code is important for this "test"
		os.Exit(1)
	}
	parser := influx.NewStreamParser(os.Stdin)
	numMetrics := 0

	for {
		m, err := parser.Next()
		if err != nil {
			if errors.Is(err, influx.EOF) {
				break // stream ended
			}
			var parseErr *influx.ParseError
			if errors.As(err, &parseErr) {
				fmt.Fprintf(os.Stderr, "parse ERR %v\n", parseErr)
				//nolint:revive // error code is important for this "test"
				os.Exit(1)
			}
			fmt.Fprintf(os.Stderr, "ERR %v\n", err)
			//nolint:revive // error code is important for this "test"
			os.Exit(1)
		}
		numMetrics++

		expected := metric.New(metricName,
			map[string]string{"name": "cpu1"},
			map[string]interface{}{"idle": 50, "sys": 30},
			now,
		)

		if !testutil.MetricEqual(expected, m) {
			fmt.Fprintf(os.Stderr, "metric doesn't match expected\n")
			//nolint:revive // error code is important for this "test"
			os.Exit(1)
		}
	}
	if expectedMetrics != numMetrics {
		fmt.Fprintf(os.Stderr, "number of metrics doesn't match expected: %v, %v\n", numMetrics, expectedMetrics)
		//nolint:revive // error code is important for this "test"
		os.Exit(1)
	}
}
