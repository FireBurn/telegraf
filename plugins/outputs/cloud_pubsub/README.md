# Google Cloud PubSub Output Plugin

This plugin publishes metrics to a [Google Cloud PubSub][pubsub] topic in one
of the supported [data formats][data_formats].

⭐ Telegraf v1.10.0
🏷️ cloud, messaging
💻 all

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

Plugins support additional global and plugin configuration settings for tasks
such as modifying metrics, tags, and fields, creating aliases, and configuring
plugin ordering. See [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Write timeout support

This plugin implements the optional context-aware output interface and
therefore supports the [`write_timeout`][write_timeout] output option. When
set, the deadline bounds client setup and the publish/acknowledgement wait for
one write.

When a write is cancelled, the delivery outcome of the in-flight batch is
unknown: PubSub may or may not have received some or all of the messages.
Telegraf keeps the batch for retry, so a cancelled write can result in
duplicate messages.

[write_timeout]: ../../../docs/CONFIGURATION.md#output-plugins

## Configuration

```toml @sample.conf
# Publish Telegraf metrics to a Google Cloud PubSub topic
[[outputs.cloud_pubsub]]
  ## Required. Name of Google Cloud Platform (GCP) Project that owns
  ## the given PubSub topic.
  project = "my-project"

  ## Required. Name of PubSub topic to publish metrics to.
  topic = "my-topic"

  ## Content encoding for message payloads, can be set to "gzip" or
  ## "identity" to apply no encoding.
  # content_encoding = "identity"

  ## Required. Data format to consume.
  ## Each data format has its own unique set of configuration options.
  ## Read more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_INPUT.md
  data_format = "influx"

  ## Optional. Filepath for GCP credentials JSON file to authorize calls to
  ## PubSub APIs. If not set explicitly, Telegraf will attempt to use
  ## Application Default Credentials, which is preferred.
  # credentials_file = "path/to/my/creds.json"

  ## Optional. If true, will send all metrics per write in one PubSub message.
  # send_batched = true

  ## The following publish_* parameters specifically configures batching
  ## requests made to the GCP Cloud PubSub API via the PubSub Golang library. Read
  ## more here: https://godoc.org/cloud.google.com/go/pubsub/v2#PublishSettings

  ## Optional. Send a request to PubSub (actually publish a batch)
  ## when it has this many PubSub messages. If send_batched is true,
  ## this is ignored and treated as if it were 1.
  # publish_count_threshold = 1000

  ## Optional. Send a request to PubSub (actually publish a batch)
  ## when it has this many PubSub messages. If send_batched is true,
  ## this is ignored and treated as if it were 1
  # publish_byte_threshold = 1000000

  ## Optional. Specifically configures requests made to the PubSub API.
  # publish_num_go_routines = 2

  ## Optional. Specifies a timeout for requests to the PubSub API.
  # publish_timeout = "30s"

  ## Optional. If true, published PubSub message data will be base64-encoded.
  # base64_data = false

  ## NOTE: Due to the way TOML is parsed, tables must be at the END of the
  ## plugin definition, otherwise additional config options are read as part of
  ## the table

  ## Optional. PubSub attributes to add to metrics.
  # [outputs.cloud_pubsub.attributes]
  #   my_attr = "tag_value"
```

[pubsub]: https://cloud.google.com/pubsub
[data_formats]: /docs/DATA_FORMATS_OUTPUT.md
