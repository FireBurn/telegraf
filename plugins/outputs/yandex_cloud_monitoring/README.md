# Yandex Cloud Monitoring Output Plugin

This plugin writes metrics to the [Yandex Cloud Monitoring][yandex] service.

⭐ Telegraf v1.17.0
🏷️ cloud
💻 all

[yandex]: https://cloud.yandex.com/services/monitoring

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

Plugins support additional global and plugin configuration settings for tasks
such as modifying metrics, tags, and fields, creating aliases, and configuring
plugin ordering. See [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Configuration

```toml @sample.conf
# Send aggregated metrics to Yandex.Cloud Monitoring
[[outputs.yandex_cloud_monitoring]]
  ## Timeout for HTTP writes.
  # timeout = "20s"

  ## Yandex.Cloud monitoring API endpoint. Normally should not be changed
  # endpoint_url = "https://monitoring.api.cloud.yandex.net/monitoring/v2/data/write"

  ## All user metrics should be sent with "custom" service specified. Normally should not be changed
  # service = "custom"
```

### Authentication

This plugin currently support only YC.Compute metadata based authentication.

When plugin is working inside a YC.Compute instance it will take IAM token and
Folder ID from instance metadata.

Other authentication methods will be added later.

## Write timeout support

This plugin implements the optional context-aware output interfaces and
therefore supports the [`write_timeout`][write_timeout] output option. When
set, the deadline bounds the outbound HTTP request(s) made during a write or
connect attempt: the metrics POST to the monitoring endpoint, and any
metadata-service requests needed to fetch the folder ID (on connect) or a
fresh IAM token (on connect, and again during a write whenever the current
token has expired).

When a write is cancelled, the delivery outcome of the in-flight batch is
unknown: the destination may or may not have received the metrics. Telegraf
keeps the batch for retry, so a cancelled write can result in duplicate
metrics.

[write_timeout]: ../../../docs/CONFIGURATION.md#output-plugins
