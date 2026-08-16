package cloud_pubsub

import (
	"context"
	"encoding/base64"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/pubsub/v2"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/internal"
	"github.com/influxdata/telegraf/plugins/parsers/influx"
	serializers_influx "github.com/influxdata/telegraf/plugins/serializers/influx"
	"github.com/influxdata/telegraf/testutil"
)

// blockingResult is a publishResult whose Get only returns once its context
// is cancelled or release is closed, used to verify WriteContext returns
// promptly on cancellation without waiting for a real publish.
type blockingResult struct {
	release chan struct{}
}

func (r *blockingResult) Get(ctx context.Context) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	case <-r.release:
		return "id", nil
	}
}

type blockingTopic struct {
	reached atomic.Bool
	release chan struct{}
}

func (*blockingTopic) ID() string { return "test-topic" }
func (*blockingTopic) Stop()      {}
func (bt *blockingTopic) Publish(context.Context, *pubsub.Message) publishResult {
	bt.reached.Store(true)
	return &blockingResult{release: bt.release}
}
func (*blockingTopic) PublishSettings() pubsub.PublishSettings { return pubsub.PublishSettings{} }
func (*blockingTopic) SetPublishSettings(pubsub.PublishSettings) {}

func TestPubSub_WriteContextCancellation(t *testing.T) {
	bt := &blockingTopic{release: make(chan struct{})}
	defer close(bt.release)

	s := &serializers_influx.Serializer{}
	require.NoError(t, s.Init())

	ps := &PubSub{
		Project:   "test-project",
		Topic:     "test-topic",
		stubTopic: func(string) topic { return bt },
	}
	require.NoError(t, ps.Init())
	var err error
	ps.encoder, err = internal.NewContentEncoder("identity")
	require.NoError(t, err)
	ps.SetSerializer(s)

	metrics := []telegraf.Metric{testutil.TestMetric("value_1", "test")}

	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- ps.WriteContext(ctx, metrics) }()

	require.Eventually(t, bt.reached.Load, time.Second, time.Millisecond)
	cancel()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("WriteContext did not return after context cancellation")
	}
}

func TestPubSub_WriteSingle(t *testing.T) {
	testMetrics := []testMetric{
		{testutil.TestMetric("value_1", "test"), false},
	}

	settings := pubsub.DefaultPublishSettings
	settings.CountThreshold = 1
	ps, topic, metrics := getTestResources(t, settings, testMetrics)

	require.NoError(t, ps.Write(metrics))

	for _, testM := range testMetrics {
		verifyRawMetricPublished(t, testM.m, topic.published)
	}
}

func TestPubSub_WriteWithAttribute(t *testing.T) {
	testMetrics := []testMetric{
		{testutil.TestMetric("value_1", "test"), false},
	}

	settings := pubsub.DefaultPublishSettings
	ps, topic, metrics := getTestResources(t, settings, testMetrics)
	ps.Attributes = map[string]string{
		"foo1": "bar1",
		"foo2": "bar2",
	}

	require.NoError(t, ps.Write(metrics))

	for _, testM := range testMetrics {
		msg := verifyRawMetricPublished(t, testM.m, topic.published)
		require.Equalf(t, "bar1", msg.Attributes["foo1"], "expected attribute foo1=bar1")
		require.Equalf(t, "bar2", msg.Attributes["foo2"], "expected attribute foo2=bar2")
	}
}

func TestPubSub_WriteMultiple(t *testing.T) {
	testMetrics := []testMetric{
		{testutil.TestMetric("value_1", "test"), false},
		{testutil.TestMetric("value_2", "test"), false},
	}

	settings := pubsub.DefaultPublishSettings

	ps, topic, metrics := getTestResources(t, settings, testMetrics)

	require.NoError(t, ps.Write(metrics))

	for _, testM := range testMetrics {
		verifyRawMetricPublished(t, testM.m, topic.published)
	}
	require.Equalf(t, 1, topic.getBundleCount(), "unexpected bundle count")
}

func TestPubSub_WriteOverCountThreshold(t *testing.T) {
	testMetrics := []testMetric{
		{testutil.TestMetric("value_1", "test"), false},
		{testutil.TestMetric("value_2", "test"), false},
		{testutil.TestMetric("value_3", "test"), false},
		{testutil.TestMetric("value_4", "test"), false},
	}

	settings := pubsub.DefaultPublishSettings
	settings.CountThreshold = 2

	ps, topic, metrics := getTestResources(t, settings, testMetrics)

	require.NoError(t, ps.Write(metrics))

	for _, testM := range testMetrics {
		verifyRawMetricPublished(t, testM.m, topic.published)
	}
	require.Equalf(t, 2, topic.getBundleCount(), "unexpected bundle count")
}

func TestPubSub_WriteOverByteThreshold(t *testing.T) {
	testMetrics := []testMetric{
		{testutil.TestMetric("value_1", "test"), false},
		{testutil.TestMetric("value_2", "test"), false},
	}

	settings := pubsub.DefaultPublishSettings
	settings.CountThreshold = 10
	settings.ByteThreshold = 1

	ps, topic, metrics := getTestResources(t, settings, testMetrics)

	require.NoError(t, ps.Write(metrics))

	for _, testM := range testMetrics {
		verifyRawMetricPublished(t, testM.m, topic.published)
	}
	require.Equalf(t, 2, topic.getBundleCount(), "unexpected bundle count")
}

func TestPubSub_WriteBase64Single(t *testing.T) {
	testMetrics := []testMetric{
		{testutil.TestMetric("value_1", "test"), false},
		{testutil.TestMetric("value_2", "test"), false},
	}

	settings := pubsub.DefaultPublishSettings
	settings.CountThreshold = 1
	ps, topic, metrics := getTestResources(t, settings, testMetrics)
	ps.Base64Data = true
	topic.Base64Data = true

	require.NoError(t, ps.Write(metrics))

	for _, testM := range testMetrics {
		verifyMetricPublished(t, testM.m, topic.published, true /* base64encoded */, false /* gzipEncoded */)
	}
}

func TestPubSub_Error(t *testing.T) {
	testMetrics := []testMetric{
		// Force this batch to return error
		{testutil.TestMetric("value_1", "test"), true},
		{testutil.TestMetric("value_2", "test"), false},
	}

	settings := pubsub.DefaultPublishSettings
	ps, _, metrics := getTestResources(t, settings, testMetrics)

	err := ps.Write(metrics)
	require.Error(t, err)
	require.ErrorContains(t, err, errMockFail)
}

func TestPubSub_WriteGzipSingle(t *testing.T) {
	testMetrics := []testMetric{
		{testutil.TestMetric("value_1", "test"), false},
		{testutil.TestMetric("value_2", "test"), false},
	}

	settings := pubsub.DefaultPublishSettings
	settings.CountThreshold = 1
	ps, topic, metrics := getTestResources(t, settings, testMetrics)
	topic.ContentEncoding = "gzip"
	ps.ContentEncoding = "gzip"
	var err error
	ps.encoder, err = internal.NewContentEncoder(ps.ContentEncoding)

	require.NoError(t, err)
	require.NoError(t, ps.Write(metrics))

	for _, testM := range testMetrics {
		verifyMetricPublished(t, testM.m, topic.published, false /* base64encoded */, true /* Gzipencoded */)
	}
}

func TestPubSub_WriteGzipAndBase64Single(t *testing.T) {
	testMetrics := []testMetric{
		{testutil.TestMetric("value_1", "test"), false},
		{testutil.TestMetric("value_2", "test"), false},
	}

	settings := pubsub.DefaultPublishSettings
	settings.CountThreshold = 1
	ps, topic, metrics := getTestResources(t, settings, testMetrics)
	topic.ContentEncoding = "gzip"
	topic.Base64Data = true
	ps.ContentEncoding = "gzip"
	ps.Base64Data = true
	var err error
	ps.encoder, err = internal.NewContentEncoder(ps.ContentEncoding)

	require.NoError(t, err)
	require.NoError(t, ps.Write(metrics))

	for _, testM := range testMetrics {
		verifyMetricPublished(t, testM.m, topic.published, true /* base64encoded */, true /* Gzipencoded */)
	}
}

func verifyRawMetricPublished(t *testing.T, m telegraf.Metric, published map[string]*pubsub.Message) *pubsub.Message {
	return verifyMetricPublished(t, m, published, false, false)
}

func verifyMetricPublished(t *testing.T, m telegraf.Metric, published map[string]*pubsub.Message, base64Encoded, gzipEncoded bool) *pubsub.Message {
	p := influx.Parser{}
	require.NoError(t, p.Init())

	v, _ := m.GetField("value")
	psMsg, ok := published[v.(string)]
	if !ok {
		t.Fatalf("expected metric to get published (value: %s)", v.(string))
	}

	data := psMsg.Data

	if gzipEncoded {
		decoder, err := internal.NewContentDecoder("gzip")
		require.NoError(t, err)
		data, err = decoder.Decode(data)
		if err != nil {
			t.Fatalf("Unable to decode expected gzip encoded message: %s", err)
		}
	}

	if base64Encoded {
		v, err := base64.StdEncoding.DecodeString(string(data))
		if err != nil {
			t.Fatalf("Unable to decode expected base64-encoded message: %s", err)
		}
		data = v
	}

	parsed, err := p.Parse(data)
	if err != nil {
		t.Fatalf("could not parse influxdb metric from published message: %s", string(data))
	}
	if len(parsed) > 1 {
		t.Fatalf("expected only one influxdb metric per published message, got %d", len(published))
	}

	publishedV, ok := parsed[0].GetField("value")
	if !ok {
		t.Fatalf("expected published metric to have a value")
	}
	require.Equal(t, v, publishedV, "incorrect published value")

	return psMsg
}
