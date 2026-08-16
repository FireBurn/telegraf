# New Relic Output Plugin

This plugins writes metrics to [New Relic Insights][newrelic] using the
[Metrics API][metrics_api]. To use this plugin you have to obtain an
[Insights API Key][insights_api_key].

⭐ Telegraf v1.15.0
🏷️ applications
💻 all

[newrelic]: https://newrelic.com
[metrics_api]: https://docs.newrelic.com/docs/data-ingest-apis/get-data-new-relic/metric-api/introduction-metric-api
[insights_api_key]: https://docs.newrelic.com/docs/apis/get-started/intro-apis/types-new-relic-api-keys#user-api-key

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

Plugins support additional global and plugin configuration settings for tasks
such as modifying metrics, tags, and fields, creating aliases, and configuring
plugin ordering. See [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Configuration

```toml @sample.conf
# Send metrics to New Relic metrics endpoint
[[outputs.newrelic]]
  ## The 'insights_key' parameter requires a NR license key.
  ## New Relic recommends you create one
  ## with a convenient name such as TELEGRAF_INSERT_KEY.
  ## reference: https://docs.newrelic.com/docs/apis/intro-apis/new-relic-api-keys/#ingest-license-key
  # insights_key = "New Relic License Key Here"

  ## Prefix to add to add to metric name for easy identification.
  ## This is very useful if your metric names are ambiguous.
  # metric_prefix = ""

  ## Timeout for writes to the New Relic API.
  # timeout = "15s"

  ## HTTP Proxy override. If unset use values from the standard
  ## proxy environment variables to determine proxy, if any.
  # http_proxy = "http://corporate.proxy:3128"

  ## Metric URL override to enable geographic location endpoints.
  # If not set use values from the standard
  # metric_url = "https://metric-api.newrelic.com/metric/v1"
```

## Write timeout support

This plugin implements the optional context-aware output interface and
therefore supports the [`write_timeout`][write_timeout] output option. This
plugin sends data via the New Relic Go telemetry SDK's `Harvester`,
configured with a zero harvest period so that `Write` synchronously calls
`Harvester.HarvestNow` on every write rather than relying on the SDK's own
periodic background dispatch. `HarvestNow` accepts a `context.Context`
natively and threads it down to the actual HTTP request(s) it issues, so the
`write_timeout` deadline bounds real delivery to the New Relic backend, not
just the in-memory enqueueing of metrics into the harvester.

When a write is cancelled, the delivery outcome of the in-flight batch is
unknown: the destination may or may not have received the metrics. Telegraf
keeps the batch for retry, so a cancelled write can result in duplicate
metrics.

[write_timeout]: ../../../docs/CONFIGURATION.md#output-plugins
