# Graylog Output Plugin

This plugin writes metrics to a [Graylog][graylog] instance using the
[GELF data format][gelf].

⭐ Telegraf v1.0.0
🏷️ datastore, logging
💻 all

[gelf]: https://docs.graylog.org/en/3.1/pages/gelf.html#gelf-payload-specification
[graylog]: https://graylog.org/

## GELF Fields

The [GELF spec][] spec defines a number of specific fields in a GELF payload.
These fields may have specific requirements set by the spec and users of the
Graylog plugin need to follow these requirements or metrics may be rejected due
to invalid data.

For example, the timestamp field defined in the GELF spec, is required to be a
UNIX timestamp. This output plugin will not modify or check the timestamp field
if one is present and send it as-is to Graylog. If the field is absent then
Telegraf will set the timestamp to the current time.

Any field not defined by the spec will have an underscore (e.g. `_`) prefixed to
the field name.

[GELF spec]: https://docs.graylog.org/docs/gelf#gelf-payload-specification

## Write timeout support

This plugin implements the optional context-aware output interface and
therefore supports the [`write_timeout`][write_timeout] output option. When
set, the deadline bounds connection setup (dial/TLS handshake) and the
outbound network write to each configured server. On cancellation, the
affected connection is closed and Telegraf reconnects on the next write.

When a write is cancelled, the delivery outcome of the in-flight batch is
unknown: some or all configured servers may or may not have received the
metrics. Telegraf keeps the batch for retry, so a cancelled write can result
in duplicate metrics.

[write_timeout]: ../../../docs/CONFIGURATION.md#output-plugins

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

Plugins support additional global and plugin configuration settings for tasks
such as modifying metrics, tags, and fields, creating aliases, and configuring
plugin ordering. See [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Configuration

```toml @sample.conf
# Send telegraf metrics to graylog
[[outputs.graylog]]
  ## Endpoints for your graylog instances.
  servers = ["udp://127.0.0.1:12201"]

  ## Connection timeout.
  # timeout = "5s"

  ## The field to use as the GELF short_message, if unset the static string
  ## "telegraf" will be used.
  ##   example: short_message_field = "message"
  # short_message_field = ""

  ## According to GELF payload specification, additional fields names must be prefixed
  ## with an underscore. Previous versions did not prefix custom field 'name' with underscore.
  ## Set to true for backward compatibility.
  # name_field_no_prefix = false

  ## Connection retry options
  ## Attempt to connect to the endpoints if the initial connection fails.
  ## If 'false', Telegraf will give up after 3 connection attempt and will
  ## exit with an error. If set to 'true', the plugin will retry to connect
  ## to the unconnected endpoints infinitely.
  # connection_retry = false
  ## Time to wait between connection retry attempts.
  # connection_retry_wait_time = "15s"

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false
```

Server endpoint may be specified without UDP or TCP scheme
(eg. "127.0.0.1:12201").  In such case, UDP protocol is assumed. TLS config is
ignored for UDP endpoints.
