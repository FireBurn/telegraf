# Inlong Output Plugin

This plugin publishes metrics to an [Apache InLong][inlong] instance.

⭐ Telegraf v1.35.0
🏷️ messaging
💻 all

[inlong]: https://inlong.apache.org

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

Plugins support additional global and plugin configuration settings for tasks
such as modifying metrics, tags, and fields, creating aliases, and configuring
plugin ordering. See [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Write timeout support

This plugin implements the optional context-aware output interface and
therefore supports the [`write_timeout`][write_timeout] output option. When
set, the deadline bounds the per-message `Send` call made during a write.
It does not bound connection setup: the underlying client's `NewClient` call
has no context parameter.

When a write is cancelled, the delivery outcome of the in-flight message is
unknown: the DataProxy server may or may not have received it. Telegraf
keeps the batch for retry, so a cancelled write can result in duplicate
messages.

[write_timeout]: ../../../docs/CONFIGURATION.md#output-plugins

## Configuration

```toml @sample.conf
# Send telegraf metrics to Apache Inlong
[[outputs.inlong]]
  ## Manager URL to obtain the Inlong data-proxy IP list for sending the data
  url = "http://127.0.0.1:8083"

  ## Unique identifier for the data-stream group
  group_id = "telegraf"  

  ## Unique identifier for the data stream within its group
  stream_id = "telegraf"  

  ## Data format to output.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md
  # data_format = "influx"
```
