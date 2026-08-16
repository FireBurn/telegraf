package amon

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/testutil"
)

func TestWriteContextCancellation(t *testing.T) {
	unblock := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-unblock
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	defer close(unblock)

	a := &Amon{
		ServerKey:    "key",
		AmonInstance: server.URL,
		Timeout:      config.Duration(time.Minute),
	}
	require.NoError(t, a.Connect())

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- a.WriteContext(ctx, []telegraf.Metric{testutil.TestMetric(1.0, "testpt")})
	}()

	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("WriteContext did not return after context cancellation")
	}
}

func TestBuildPoint(t *testing.T) {
	var tagtests = []struct {
		ptIn  telegraf.Metric
		outPt Point
		err   error
	}{
		{
			testutil.TestMetric(float64(0.0), "testpt"),
			Point{
				float64(time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC).Unix()),
				0.0,
			},
			nil,
		},
		{
			testutil.TestMetric(float64(1.0), "testpt"),
			Point{
				float64(time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC).Unix()),
				1.0,
			},
			nil,
		},
		{
			testutil.TestMetric(int(10), "testpt"),
			Point{
				float64(time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC).Unix()),
				10.0,
			},
			nil,
		},
		{
			testutil.TestMetric(int32(112345), "testpt"),
			Point{
				float64(time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC).Unix()),
				112345.0,
			},
			nil,
		},
		{
			testutil.TestMetric(int64(112345), "testpt"),
			Point{
				float64(time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC).Unix()),
				112345.0,
			},
			nil,
		},
		{
			testutil.TestMetric(float32(11234.5), "testpt"),
			Point{
				float64(time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC).Unix()),
				11234.5,
			},
			nil,
		},
		{
			testutil.TestMetric("11234.5", "testpt"),
			Point{
				float64(time.Date(2009, time.November, 10, 23, 0, 0, 0, time.UTC).Unix()),
				11234.5,
			},
			errors.New("unable to extract value from Fields, undeterminable type"),
		},
	}
	for _, tt := range tagtests {
		pt, err := buildMetrics(tt.ptIn)
		if err != nil && tt.err == nil {
			t.Errorf("%s: unexpected error, %+v\n", tt.ptIn.Name(), err)
		}
		if tt.err != nil && err == nil {
			t.Errorf("%s: expected an error (%s) but none returned", tt.ptIn.Name(), tt.err.Error())
		}
		if !reflect.DeepEqual(pt["value"], tt.outPt) && tt.err == nil {
			t.Errorf("%s: \nexpected %+v\ngot %+v\n",
				tt.ptIn.Name(), tt.outPt, pt["value"])
		}
	}
}
