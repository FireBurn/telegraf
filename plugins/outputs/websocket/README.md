# Websocket Output Plugin

This plugin writes metrics to a WebSocket endpoint in one of the supported
[data formats][data_formats].

⭐ Telegraf v1.19.0
🏷️ applications, web
💻 all

[data_formats]: /docs/DATA_FORMATS_OUTPUT.md

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

Plugins support additional global and plugin configuration settings for tasks
such as modifying metrics, tags, and fields, creating aliases, and configuring
plugin ordering. See [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Secret store support

This plugin supports secrets from secret stores for the `headers` option.
See the [secret store documentation][SECRETSTORE] for more details on how
to use them.

[SECRETSTORE]: ../../../docs/CONFIGURATION.md#secret-store-secrets

## Configuration

```toml @sample.conf
# A plugin that can transmit metrics over WebSocket.
[[outputs.websocket]]
  ## URL is the address to send metrics to. Make sure ws or wss scheme is used.
  url = "ws://127.0.0.1:3000/telegraf"

  ## Timeouts (make sure read_timeout is larger than server ping interval or set to zero).
  # connect_timeout = "30s"
  # write_timeout = "30s"
  # read_timeout = "30s"

  ## Optionally turn on using text data frames (binary by default).
  # use_text_frames = false

  ## Optional TLS Config
  # tls_ca = "/etc/telegraf/ca.pem"
  # tls_cert = "/etc/telegraf/cert.pem"
  # tls_key = "/etc/telegraf/key.pem"
  ## Use TLS but skip chain & host verification
  # insecure_skip_verify = false

  ## Optional SOCKS5 proxy to use
  # socks5_enabled = true
  # socks5_address = "127.0.0.1:1080"
  # socks5_username = "alice"
  # socks5_password = "pass123"

  ## Optional HTTP proxy to use
  # use_system_proxy = false
  # http_proxy_url = "http://localhost:8888"

  ## Data format to output.
  ## Each data format has it's own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md
  # data_format = "influx"

  ## NOTE: Due to the way TOML is parsed, tables must be at the END of the
  ## plugin definition, otherwise additional config options are read as part of
  ## the table

  ## Additional HTTP Upgrade headers
  # [outputs.websocket.headers]
  #   Authorization = "Bearer <TOKEN>"
```

## Write timeout support

This plugin implements the optional context-aware output interface and
therefore supports the [`write_timeout`][write_timeout] output option. When
set, the deadline bounds a single connect attempt (dialing and completing
the WebSocket handshake, if not already connected) or a single write attempt
(sending one serialized batch as a WebSocket message).

> [!NOTE]
> This plugin already has its own `write_timeout` option (documented above)
> which sets a fixed per-write socket deadline. Because both options share
> the same TOML key, setting `write_timeout` on this plugin configures both
> at once: the fixed socket deadline used for every write, and the
> context-based deadline used to bound and cancel a stuck write. In
> practice this means the two mechanisms enforce the same duration.

When a write is cancelled, the delivery outcome of the in-flight batch is
unknown: the destination may or may not have received the metrics.
Cancellation is implemented by closing the underlying connection (gorilla's
`Conn.WriteMessage` only documents `Close` -- not `SetWriteDeadline` -- as
safe to call concurrently with an in-progress write), so a cancelled write
always forces a reconnect on the next write. Telegraf keeps the batch for
retry, so a cancelled write can result in duplicate metrics.

[write_timeout]: ../../../docs/CONFIGURATION.md#output-plugins
