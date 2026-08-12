package kafka

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/stretchr/testify/require"
	kafkacontainer "github.com/testcontainers/testcontainers-go/modules/kafka"

	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/models"
	"github.com/influxdata/telegraf/plugins/serializers/influx"
	"github.com/influxdata/telegraf/testutil"
)

func TestConnectAndWriteIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	kafkaContainer, err := kafkacontainer.Run(t.Context(), "confluentinc/confluent-local:7.5.0")
	require.NoError(t, err)
	defer kafkaContainer.Terminate(t.Context()) //nolint:errcheck // ignored

	brokers, err := kafkaContainer.Brokers(t.Context())
	require.NoError(t, err)

	// Setup the plugin
	plugin := &Kafka{
		Brokers:      brokers,
		Topic:        "Test",
		Log:          testutil.Logger{},
		producerFunc: sarama.NewSyncProducer,
	}

	// Setup the metric serializer
	s := &influx.Serializer{}
	require.NoError(t, s.Init())
	plugin.SetSerializer(s)

	// Verify that we can connect to the Kafka broker
	require.NoError(t, plugin.Init())
	require.NoError(t, plugin.Connect())
	defer plugin.Close()

	// Verify that we can successfully write data to the kafka broker
	require.NoError(t, plugin.Write(testutil.MockMetrics()))
}

func TestTopicSuffixes(t *testing.T) {
	topic := "Test"

	m := testutil.TestMetric(1)
	metricTagName := "tag1"
	metricTagValue := m.Tags()[metricTagName]
	metricName := m.Name()

	var tests = []struct {
		suffix   TopicSuffix
		expected string
	}{
		// This ensures empty separator is okay
		{
			TopicSuffix{Method: "measurement"},
			topic + metricName,
		},
		{
			TopicSuffix{Method: "measurement", Separator: "sep"},
			topic + "sep" + metricName,
		},
		{
			TopicSuffix{Method: "tags", Keys: []string{metricTagName}, Separator: "_"},
			topic + "_" + metricTagValue,
		},
		{
			TopicSuffix{Method: "tags", Keys: []string{metricTagName, metricTagName, metricTagName}, Separator: "___"},
			topic + "___" + metricTagValue + "___" + metricTagValue + "___" + metricTagValue,
		},
		{
			TopicSuffix{Method: "tags", Keys: []string{metricTagName, metricTagName, metricTagName}},
			topic + metricTagValue + metricTagValue + metricTagValue,
		},
		{
			// Ensure non-existing tags are ignored
			TopicSuffix{Method: "tags", Keys: []string{"non_existing_tag", "non_existing_tag"}, Separator: "___"},
			topic,
		},
		{
			TopicSuffix{Method: "tags", Keys: []string{metricTagName, "non_existing_tag"}, Separator: "___"},
			topic + "___" + metricTagValue,
		},
		{
			// Ensure backward compatibility
			TopicSuffix{},
			topic,
		},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			topicSuffix := tt.suffix
			expectedTopic := tt.expected
			k := &Kafka{
				Topic:       topic,
				TopicSuffix: topicSuffix,
				Log:         testutil.Logger{},
			}

			_, topic := k.getTopicName(m)
			require.Equal(t, expectedTopic, topic)
		})
	}
}

func TestValidTopicSuffixMethod(t *testing.T) {
	for _, method := range []string{"", "measurement", "tags"} {
		name := method
		if method == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			plugin := &Kafka{
				TopicSuffix: TopicSuffix{
					Method: method,
				},
				Log: testutil.Logger{},
			}
			require.NoError(t, plugin.Init())
		})
	}
}

func TestInvalidTopicSuffixMethod(t *testing.T) {
	plugin := &Kafka{
		TopicSuffix: TopicSuffix{
			Method: "invalid_topic_suffix_method",
		},
		Log: testutil.Logger{},
	}
	require.ErrorContains(t, plugin.Init(), "unknown topic suffix method provided")
}

func TestRoutingKeyStatic(t *testing.T) {
	plugin := &Kafka{
		RoutingKey: "static",
		Log:        testutil.Logger{},
	}

	m := metric.New(
		"cpu",
		map[string]string{},
		map[string]interface{}{
			"value": 42.0,
		},
		time.Unix(0, 0),
	)

	key, err := plugin.routingKey(m)
	require.NoError(t, err)
	require.Equal(t, "static", key)
}

func TestRoutingKeyRandom(t *testing.T) {
	plugin := &Kafka{
		RoutingKey: "random",
		Log:        testutil.Logger{},
	}

	m := metric.New(
		"cpu",
		map[string]string{},
		map[string]interface{}{
			"value": 42.0,
		},
		time.Unix(0, 0),
	)

	key, err := plugin.routingKey(m)
	require.NoError(t, err)
	require.Len(t, key, 36)
}

func TestTopicTag(t *testing.T) {
	tests := []struct {
		name            string
		topicTag        string
		excludeTopicTag bool
		expectedTopic   string
		expectedContent string
	}{
		{
			name:            "static topic",
			expectedTopic:   "telegraf",
			expectedContent: "cpu,topic=xyzzy time_idle=42 0\n",
		},
		{
			name:            "topic tag overrides static topic",
			topicTag:        "topic",
			expectedTopic:   "xyzzy",
			expectedContent: "cpu,topic=xyzzy time_idle=42 0\n",
		},
		{
			name:            "missing topic tag falls back to  static topic",
			topicTag:        "non-existant",
			expectedTopic:   "telegraf",
			expectedContent: "cpu,topic=xyzzy time_idle=42 0\n",
		},
		{
			name:            "exclude topic tag removes tag",
			topicTag:        "topic",
			excludeTopicTag: true,
			expectedTopic:   "xyzzy",
			expectedContent: "cpu time_idle=42 0\n",
		},
	}

	// Define an input metric for writing
	input := []telegraf.Metric{
		metric.New(
			"cpu",
			map[string]string{
				"topic": "xyzzy",
			},
			map[string]interface{}{
				"time_idle": 42.0,
			},
			time.Unix(0, 0),
		),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup the serializer
			s := &influx.Serializer{}
			require.NoError(t, s.Init())

			// Setup the plugin under test
			plugin := &Kafka{
				Brokers:         []string{"127.0.0.1"},
				Topic:           "telegraf",
				TopicTag:        tt.topicTag,
				ExcludeTopicTag: tt.excludeTopicTag,
				Log:             testutil.Logger{},
				producerFunc:    newMockProducer,
			}
			plugin.SetSerializer(s)
			require.NoError(t, plugin.Init())

			// Connect and write a metric
			require.NoError(t, plugin.Connect())
			require.NoError(t, plugin.Write(input))

			// Check the content that would be sent by the producer
			producer, ok := plugin.producer.(*mockProducer)
			require.True(t, ok, "invalid producer type")

			producer.Lock()
			message := producer.sent[0]
			producer.Unlock()

			require.Equal(t, tt.expectedTopic, message.Topic)
			encoded, err := message.Value.Encode()
			require.NoError(t, err)
			require.Equal(t, tt.expectedContent, string(encoded))
		})
	}
}

func TestHeaders(t *testing.T) {
	tests := []struct {
		name     string
		headers  map[string]string
		expected []sarama.RecordHeader
	}{
		{
			name: "none",
		},
		{
			name:    "static string",
			headers: map[string]string{"agent": "telegraf"},
			expected: []sarama.RecordHeader{
				{
					Key:   []byte("agent"),
					Value: []byte("telegraf"),
				},
			},
		},
		{
			name:    "metric name header",
			headers: map[string]string{"metric": "{{ .Name }}"},
			expected: []sarama.RecordHeader{
				{
					Key:   []byte("metric"),
					Value: []byte("cpu"),
				},
			},
		},
		{
			name: "complex",
			headers: map[string]string{
				"source": `{{ .Tag "source" }}:{{ .Tag "topic"}}`,
				"device": `{{ .Name }}-{{ .Field "id" }}`,
			},
			expected: []sarama.RecordHeader{
				{
					Key:   []byte("source"),
					Value: []byte("server:xyzzy"),
				},
				{
					Key:   []byte("device"),
					Value: []byte("cpu-3254345daab4"),
				},
			},
		},
	}

	// Define an input metric for writing
	input := []telegraf.Metric{
		metric.New(
			"cpu",
			map[string]string{
				"topic":  "xyzzy",
				"source": "server",
			},
			map[string]interface{}{
				"id":    "3254345daab4",
				"value": 42.0,
				"hours": 255,
			},
			time.Unix(0, 0),
		),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Setup the serializer
			s := &influx.Serializer{}
			require.NoError(t, s.Init())

			// Setup the plugin under test
			plugin := &Kafka{
				Brokers:      []string{"127.0.0.1"},
				Topic:        "telegraf",
				Headers:      tt.headers,
				Log:          testutil.Logger{},
				producerFunc: newMockProducer,
			}
			plugin.SetSerializer(s)
			require.NoError(t, plugin.Init())

			// Connect and write a metric
			require.NoError(t, plugin.Connect())
			require.NoError(t, plugin.Write(input))

			// Check the content that would be sent by the producer
			producer, ok := plugin.producer.(*mockProducer)
			require.True(t, ok, "invalid producer type")

			producer.Lock()
			message := producer.sent[0]
			producer.Unlock()

			require.ElementsMatch(t, tt.expected, message.Headers)
		})
	}
}

func TestWriteContext(t *testing.T) {
	tests := []struct {
		name     string
		sendErr  error
		expected error
	}{
		{name: "success"},
		{name: "producer error", sendErr: sarama.ErrNotConnected, expected: sarama.ErrNotConnected},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			producer := &controlledProducer{sendErr: tt.sendErr}
			plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
				return producer, nil
			})

			err := plugin.WriteContext(t.Context(), testutil.MockMetrics())
			if tt.expected == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.expected)
			}
			require.Equal(t, int32(1), producer.sends.Load())
		})
	}
}

func TestWriteContextCancellationDoesNotWaitForProducer(t *testing.T) {
	sendBlock := make(chan struct{})
	closeBlock := make(chan struct{})
	producer := &controlledProducer{sendBlock: sendBlock, closeBlock: closeBlock}
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		return producer, nil
	})

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := plugin.WriteContext(ctx, testutil.MockMetrics())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 500*time.Millisecond)

	// Release the fake only after proving that neither SendMessages nor Close
	// is on the cancellation path.
	close(sendBlock)
	close(closeBlock)
}

func TestWriteContextReplacesPoisonedProducer(t *testing.T) {
	oldSendBlock := make(chan struct{})
	oldCloseBlock := make(chan struct{})
	oldProducer := &controlledProducer{sendBlock: oldSendBlock, closeBlock: oldCloseBlock}
	newProducer := &controlledProducer{}
	producers := []sarama.SyncProducer{oldProducer, newProducer}
	var created atomic.Int32
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		return producers[int(created.Add(1))-1], nil
	})
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, plugin.WriteContext(ctx, testutil.MockMetrics()), context.DeadlineExceeded)
	require.NoError(t, plugin.WriteContext(t.Context(), testutil.MockMetrics()))
	require.Equal(t, int32(1), newProducer.sends.Load())

	// Cleanup from the old write must only ever close the old generation.
	require.Eventually(t, func() bool { return oldProducer.closes.Load() == 1 }, time.Second, time.Millisecond)
	require.Zero(t, newProducer.closes.Load())
	close(oldSendBlock)
	close(oldCloseBlock)
}

func TestWriteContextCancellationRaceKeepsReplacement(t *testing.T) {
	sendBlock := make(chan struct{})
	closeBlock := make(chan struct{})
	oldProducer := &controlledProducer{sendBlock: sendBlock, closeBlock: closeBlock}
	newProducer := &controlledProducer{}
	producers := []sarama.SyncProducer{oldProducer, newProducer}
	var created atomic.Int32
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		return producers[int(created.Add(1))-1], nil
	})

	ctx, cancel := context.WithCancel(t.Context())
	result := make(chan error, 1)
	go func() { result <- plugin.WriteContext(ctx, testutil.MockMetrics()) }()
	require.Eventually(t, func() bool { return oldProducer.sends.Load() == 1 }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-result, context.Canceled)
	require.NoError(t, plugin.WriteContext(t.Context(), testutil.MockMetrics()))
	close(sendBlock)
	close(closeBlock)
	require.Eventually(t, func() bool { return oldProducer.closes.Load() == 1 }, time.Second, time.Millisecond)
	require.Zero(t, newProducer.closes.Load())
}

func TestWriteContextBoundsPoisonedProducers(t *testing.T) {
	blocks := []chan struct{}{make(chan struct{}), make(chan struct{})}
	producers := []sarama.SyncProducer{
		&controlledProducer{sendBlock: blocks[0], closeBlock: blocks[0]},
		&controlledProducer{sendBlock: blocks[1], closeBlock: blocks[1]},
	}
	var created atomic.Int32
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		return producers[int(created.Add(1))-1], nil
	})
	logger := &testutil.CaptureLogger{}
	plugin.Log = logger

	for range producers {
		ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		require.ErrorIs(t, plugin.WriteContext(ctx, testutil.MockMetrics()), context.DeadlineExceeded)
		cancel()
	}
	start := time.Now()
	err := plugin.WriteContext(t.Context(), testutil.MockMetrics())
	require.ErrorContains(t, err, "replacement limit reached")
	require.Less(t, time.Since(start), 100*time.Millisecond)
	require.Equal(t, int32(2), created.Load())
	require.Len(t, logger.Errors(), 1)
	require.Contains(t, logger.Errors()[0], "Kafka producer replacement limit reached")
	require.Error(t, plugin.WriteContext(t.Context(), testutil.MockMetrics()))
	require.Len(t, logger.Errors(), 1)

	for _, block := range blocks {
		close(block)
	}
}

func TestWriteContextBoundsProducerCreation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	producer := &controlledProducer{}
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		close(started)
		<-release
		return producer, nil
	})

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err := plugin.WriteContext(ctx, testutil.MockMetrics())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 500*time.Millisecond)
	close(release)
	require.Eventually(t, func() bool {
		plugin.producerMu.Lock()
		defer plugin.producerMu.Unlock()
		return plugin.producer == producer
	}, time.Second, time.Millisecond)
	require.NoError(t, plugin.Close())
}

func TestProducerCreationIsShared(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	producer := &controlledProducer{}
	var creations atomic.Int32
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		if creations.Add(1) == 1 {
			close(started)
		}
		<-release
		return producer, nil
	})

	results := make(chan error, 4)
	for range 3 {
		go func() {
			ctx, cancel := context.WithTimeout(t.Context(), 40*time.Millisecond)
			defer cancel()
			results <- plugin.ConnectContext(ctx)
		}()
	}
	<-started
	for range 3 {
		require.ErrorIs(t, <-results, context.DeadlineExceeded)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 25*time.Millisecond)
	defer cancel()
	err := plugin.ConnectContext(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, int32(1), creations.Load())

	close(release)
	require.Eventually(t, func() bool {
		plugin.producerMu.Lock()
		defer plugin.producerMu.Unlock()
		return plugin.producer == producer
	}, time.Second, time.Millisecond)
	require.NoError(t, plugin.Close())
}

func TestRunningOutputRetryConnectionIsBounded(t *testing.T) {
	blocked := make(chan struct{})
	var creations atomic.Int32
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		if creations.Add(1) == 1 {
			return nil, errors.New("initial connection failed")
		}
		<-blocked
		return &controlledProducer{}, nil
	})
	running, err := models.NewRunningOutput(plugin, &models.OutputConfig{
		Name: "kafka", StartupErrorBehavior: "retry",
	}, 5, 10)
	require.NoError(t, err)
	require.NoError(t, running.ConnectContext(t.Context()))
	running.AddMetric(testutil.TestMetric(1))

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	err = running.WriteContext(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(start), 500*time.Millisecond)
	require.Equal(t, int32(2), creations.Load())
	close(blocked)
	require.Eventually(t, func() bool {
		plugin.producerMu.Lock()
		defer plugin.producerMu.Unlock()
		return plugin.producer != nil
	}, time.Second, time.Millisecond)
	running.Close()
}

func TestCloseDuringProducerCreation(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	producer := &controlledProducer{}
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		close(started)
		<-release
		return producer, nil
	})
	plugin.closeTimeout = 25 * time.Millisecond

	result := make(chan error, 1)
	go func() {
		_, _, err := plugin.acquireProducer(t.Context())
		result <- err
	}()
	<-started
	start := time.Now()
	require.NoError(t, plugin.Close())
	require.Less(t, time.Since(start), 500*time.Millisecond)

	close(release)
	require.ErrorContains(t, <-result, "result is no longer usable")
	require.Eventually(t, func() bool { return producer.closes.Load() == 1 }, time.Second, time.Millisecond)
	plugin.producerMu.Lock()
	require.Nil(t, plugin.producer)
	require.True(t, plugin.closed)
	plugin.producerMu.Unlock()
}

func TestSharedProducerCreationSucceeds(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	producer := &controlledProducer{}
	var creations atomic.Int32
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		if creations.Add(1) == 1 {
			close(started)
		}
		<-release
		return producer, nil
	})

	type acquisition struct {
		producer   sarama.SyncProducer
		generation uint64
		err        error
	}
	results := make(chan acquisition, 2)
	for range 2 {
		go func() {
			p, generation, err := plugin.acquireProducer(t.Context())
			results <- acquisition{p, generation, err}
		}()
	}
	<-started
	close(release)
	first, second := <-results, <-results
	require.NoError(t, first.err)
	require.NoError(t, second.err)
	require.Same(t, producer, first.producer)
	require.Same(t, producer, second.producer)
	require.Equal(t, first.generation, second.generation)
	require.Equal(t, int32(1), creations.Load())
	require.NoError(t, plugin.Close())
}

func TestProducerCreationFailureCanRetry(t *testing.T) {
	creationErr := errors.New("creation failed")
	producer := &controlledProducer{}
	var creations atomic.Int32
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		if creations.Add(1) == 1 {
			return nil, creationErr
		}
		return producer, nil
	})

	_, _, err := plugin.acquireProducer(t.Context())
	require.ErrorIs(t, err, creationErr)
	require.Equal(t, int32(1), creations.Load())

	p, _, err := plugin.acquireProducer(t.Context())
	require.NoError(t, err)
	require.Same(t, producer, p)
	require.Equal(t, int32(2), creations.Load())
	require.NoError(t, plugin.Close())
}

func TestClose(t *testing.T) {
	tests := []struct {
		name     string
		closeErr error
	}{
		{name: "success"},
		{name: "producer error", closeErr: sarama.ErrNotConnected},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			producer := &controlledProducer{closeErr: tt.closeErr}
			plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
				return producer, nil
			})
			require.NoError(t, plugin.Connect())
			err := plugin.Close()
			if tt.closeErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tt.closeErr)
			}
			require.Equal(t, int32(1), producer.closes.Load())
		})
	}
}

func TestCloseDoesNotWaitForProducer(t *testing.T) {
	closeBlock := make(chan struct{})
	producer := &controlledProducer{closeBlock: closeBlock}
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		return producer, nil
	})
	plugin.closeTimeout = 25 * time.Millisecond
	require.NoError(t, plugin.Connect())

	start := time.Now()
	err := plugin.Close()
	require.ErrorContains(t, err, "timed out")
	require.Less(t, time.Since(start), 500*time.Millisecond)
	close(closeBlock)
}

func TestCloseRacesWithCancelledWrite(t *testing.T) {
	sendBlock := make(chan struct{})
	closeBlock := make(chan struct{})
	producer := &controlledProducer{sendBlock: sendBlock, closeBlock: closeBlock}
	plugin := newTestPlugin(t, func([]string, *sarama.Config) (sarama.SyncProducer, error) {
		return producer, nil
	})
	plugin.closeTimeout = 25 * time.Millisecond

	ctx, cancel := context.WithCancel(t.Context())
	writeDone := make(chan error, 1)
	go func() { writeDone <- plugin.WriteContext(ctx, testutil.MockMetrics()) }()
	require.Eventually(t, func() bool { return producer.sends.Load() == 1 }, time.Second, time.Millisecond)

	closeDone := make(chan error, 1)
	go func() { closeDone <- plugin.Close() }()
	require.Eventually(t, func() bool { return producer.closes.Load() == 1 }, time.Second, time.Millisecond)
	cancel()
	require.ErrorIs(t, <-writeDone, context.Canceled)
	require.ErrorContains(t, <-closeDone, "timed out")
	require.Equal(t, int32(1), producer.closes.Load())
	close(sendBlock)
	close(closeBlock)
}

func newTestPlugin(t *testing.T, factory func([]string, *sarama.Config) (sarama.SyncProducer, error)) *Kafka {
	t.Helper()
	s := &influx.Serializer{}
	require.NoError(t, s.Init())
	plugin := &Kafka{Brokers: []string{"127.0.0.1"}, Topic: "telegraf", Log: testutil.Logger{}, producerFunc: factory}
	plugin.SetSerializer(s)
	require.NoError(t, plugin.Init())
	return plugin
}

type controlledProducer struct {
	sarama.SyncProducer
	sendBlock  <-chan struct{}
	closeBlock <-chan struct{}
	sendErr    error
	closeErr   error
	sends      atomic.Int32
	closes     atomic.Int32
}

func (p *controlledProducer) SendMessages([]*sarama.ProducerMessage) error {
	p.sends.Add(1)
	if p.sendBlock != nil {
		<-p.sendBlock
	}
	return p.sendErr
}

func (p *controlledProducer) Close() error {
	p.closes.Add(1)
	if p.closeBlock != nil {
		<-p.closeBlock
	}
	return p.closeErr
}

type mockProducer struct {
	sent []*sarama.ProducerMessage
	sarama.SyncProducer
	sync.Mutex
}

func newMockProducer(_ []string, _ *sarama.Config) (sarama.SyncProducer, error) {
	return &mockProducer{}, nil
}

func (p *mockProducer) SendMessage(msg *sarama.ProducerMessage) (partition int32, offset int64, err error) {
	p.Lock()
	defer p.Unlock()
	p.sent = append(p.sent, msg)
	return 0, 0, nil
}

func (p *mockProducer) SendMessages(msgs []*sarama.ProducerMessage) error {
	p.Lock()
	defer p.Unlock()
	p.sent = append(p.sent, msgs...)
	return nil
}

func (*mockProducer) Close() error {
	return nil
}
