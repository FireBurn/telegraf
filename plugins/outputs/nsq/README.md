# NSQ Output Plugin

This plugin writes metrics to the given topic of a [NSQ][nsq] instance as a
producer in one of the supported [data formats][data_formats].

⭐ Telegraf v0.2.1
🏷️ messaging
💻 all

[nsq]: https://nsq.io
[data_formats]: /docs/DATA_FORMATS_OUTPUT.md

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

Plugins support additional global and plugin configuration settings for tasks
such as modifying metrics, tags, and fields, creating aliases, and configuring
plugin ordering. See [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Configuration

```toml @sample.conf
# Send telegraf measurements to NSQD
[[outputs.nsq]]
  ## Location of nsqd instance listening on TCP
  server = "localhost:4150"
  ## NSQ topic for producer messages
  topic = "telegraf"

  ## Data format to output.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md
  data_format = "influx"
```

## Write timeout support

This plugin implements the optional context-aware output interface and
therefore supports the [`write_timeout`][write_timeout] output option. The
configured timeout bounds each `Publish` call to the NSQD daemon, so a write
can no longer block Telegraf indefinitely if the daemon stops acknowledging
traffic.

When a write is cancelled, the delivery outcome of the in-flight message is
unknown: it may or may not have reached NSQD. Telegraf keeps the batch for
retry, so a cancelled write can result in duplicate messages. This also
applies when a slow (but otherwise healthy) daemon exceeds `write_timeout`.
The producer used by the cancelled write is discarded and stopped in the
background, and the next write creates a new one.

[write_timeout]: ../../../docs/CONFIGURATION.md#output-plugins
