package warp10

import (
	"context"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/testutil"
)

type ErrorTest struct {
	Message  string
	Expected string
}

func TestWriteWarp10(t *testing.T) {
	w := Warp10{
		Prefix:  "unit.test",
		WarpURL: "http://localhost:8090",
		Token:   config.NewSecret([]byte("WRITE")),
	}

	payload := w.GenWarp10Payload(testutil.MockMetrics())
	require.Exactly(t, "1257894000000000// unit.testtest1.value{source=telegraf,tag1=value1} 1.000000\n", payload)
}

func TestWriteWarp10ValueNaN(t *testing.T) {
	w := Warp10{
		Prefix:  "unit.test",
		WarpURL: "http://localhost:8090",
		Token:   config.NewSecret([]byte("WRITE")),
	}

	payload := w.GenWarp10Payload(testutil.MockMetricsWithValue(math.NaN()))
	require.Exactly(t, "1257894000000000// unit.testtest1.value{source=telegraf,tag1=value1} NaN\n", payload)
}

func TestWriteWarp10ValueInfinity(t *testing.T) {
	w := Warp10{
		Prefix:  "unit.test",
		WarpURL: "http://localhost:8090",
		Token:   config.NewSecret([]byte("WRITE")),
	}

	payload := w.GenWarp10Payload(testutil.MockMetricsWithValue(math.Inf(1)))
	require.Exactly(t, "1257894000000000// unit.testtest1.value{source=telegraf,tag1=value1} Infinity\n", payload)
}

func TestWriteWarp10ValueMinusInfinity(t *testing.T) {
	w := Warp10{
		Prefix:  "unit.test",
		WarpURL: "http://localhost:8090",
		Token:   config.NewSecret([]byte("WRITE")),
	}

	payload := w.GenWarp10Payload(testutil.MockMetricsWithValue(math.Inf(-1)))
	require.Exactly(t, "1257894000000000// unit.testtest1.value{source=telegraf,tag1=value1} -Infinity\n", payload)
}

func TestWriteWarp10EncodedTags(t *testing.T) {
	w := Warp10{
		Prefix:  "unit.test",
		WarpURL: "http://localhost:8090",
		Token:   config.NewSecret([]byte("WRITE")),
	}

	metrics := testutil.MockMetrics()
	for _, metric := range metrics {
		metric.AddTag("encoded{tag", "value1,value2")
	}

	payload := w.GenWarp10Payload(metrics)
	require.Exactly(t, "1257894000000000// unit.testtest1.value{encoded%7Btag=value1%2Cvalue2,source=telegraf,tag1=value1} 1.000000\n", payload)
}

func TestWriteContextCancellation(t *testing.T) {
	unblock := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-unblock
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()
	defer close(unblock)

	plugin := &Warp10{
		Prefix:  "unit.test",
		WarpURL: ts.URL,
		Token:   config.NewSecret([]byte("WRITE")),
	}
	require.NoError(t, plugin.Init())
	require.NoError(t, plugin.Connect())

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- plugin.WriteContext(ctx, testutil.MockMetrics())
	}()

	cancel()

	select {
	case err := <-errCh:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("WriteContext did not return after context cancellation")
	}
}

func TestHandleWarp10Error(t *testing.T) {
	tests := [...]*ErrorTest{
		{
			Message: `
			<html>
			<head>
			<meta http-equiv="Content-Type" content="text/html;charset=utf-8"/>
			<title>Error 500 io.warp10.script.WarpScriptException: Invalid token.</title>
			</head>
			<body><h2>HTTP ERROR 500</h2>
			<p>Problem accessing /api/v0/update. Reason:
			<pre>    io.warp10.script.WarpScriptException: Invalid token.</pre></p>
			</body>
			</html>
			`,
			Expected: "Invalid token",
		},
		{
			Message: `
			<html>
			<head>
			<meta http-equiv="Content-Type" content="text/html;charset=utf-8"/>
			<title>Error 500 io.warp10.script.WarpScriptException: Token Expired.</title>
			</head>
			<body><h2>HTTP ERROR 500</h2>
			<p>Problem accessing /api/v0/update. Reason:
			<pre>    io.warp10.script.WarpScriptException: Token Expired.</pre></p>
			</body>
			</html>
			`,
			Expected: "Token Expired",
		},
		{
			Message: `
			<html>
			<head>
			<meta http-equiv="Content-Type" content="text/html;charset=utf-8"/>
			<title>Error 500 io.warp10.script.WarpScriptException: Token revoked.</title>
			</head>
			<body><h2>HTTP ERROR 500</h2>
			<p>Problem accessing /api/v0/update. Reason:
			<pre>    io.warp10.script.WarpScriptException: Token revoked.</pre></p>
			</body>
			</html>
			`,
			Expected: "Token revoked",
		},
		{
			Message: `
			<html>
			<head>
			<meta http-equiv="Content-Type" content="text/html;charset=utf-8"/>
			<title>Error 500 io.warp10.script.WarpScriptException: Write token missing.</title>
			</head>
			<body><h2>HTTP ERROR 500</h2>
			<p>Problem accessing /api/v0/update. Reason:
			<pre>    io.warp10.script.WarpScriptException: Write token missing.</pre></p>
			</body>
			</html>
			`,
			Expected: "Write token missing",
		},
		{
			Message:  `<title>Error 503: server unavailable</title>`,
			Expected: "<title>Error 503: server unavailable</title>",
		},
	}

	for _, handledError := range tests {
		payload := HandleError(handledError.Message, 511)
		require.Exactly(t, handledError.Expected, payload)
	}
}
