# Instrumental Output Plugin

This plugin writes metrics to the [Instrumental Collector API][instrumental]
and requires a project-specific API token.

Instrumental accepts stats in a format very close to Graphite, with the only
difference being that the type of stat (gauge, increment) is the first token,
separated from the metric itself by whitespace. The `increment` type is only
used if the metric comes in as a counter via the [statsd input plugin][statsd].

⭐ Telegraf v0.13.1
🏷️ applications
💻 all

[instrumental]: https://instrumentalapp.com/docs/tcp-collector
[statsd]: /plugins/inputs/statsd/README.md

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

Plugins support additional global and plugin configuration settings for tasks
such as modifying metrics, tags, and fields, creating aliases, and configuring
plugin ordering. See [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Write timeout support

This plugin implements the optional context-aware output interface and
therefore supports the [`write_timeout`][write_timeout] output option. When
set, the deadline bounds connection setup (dial and the authentication
handshake, including waiting for the server's response) and the outbound
network write. On cancellation, the connection is closed and Telegraf
reconnects on the next write.

When a write is cancelled, the delivery outcome of the in-flight batch is
unknown: the destination may or may not have received the metrics. Telegraf
keeps the batch for retry, so a cancelled write can result in duplicate
metrics.

[write_timeout]: ../../../docs/CONFIGURATION.md#output-plugins

## Secret store support

This plugin supports secrets from secret stores for the `api_token` option.
See the [secret store documentation][SECRETSTORE] for more details on how
to use them.

[SECRETSTORE]: ../../../docs/CONFIGURATION.md#secret-store-secrets

## Configuration

```toml @sample.conf
# Configuration for sending metrics to an Instrumental project
[[outputs.instrumental]]
  ## Project API Token (required)
  api_token = "API Token"  # required
  ## Prefix the metrics with a given name
  prefix = ""
  ## Stats output template (Graphite formatting)
  ## see https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md#graphite
  template = "host.tags.measurement.field"
  ## Timeout in seconds to connect
  timeout = "2s"
  ## Debug true - Print communication to Instrumental
  debug = false
```
