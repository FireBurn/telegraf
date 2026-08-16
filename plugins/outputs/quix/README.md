# Quix Output Plugin

This plugin writes metrics to a [Quix][quix] endpoint.

Please consult Quix's [official documentation][docs] for more details on the
Quix platform architecture and concepts.

⭐ Telegraf v1.33.0
🏷️ cloud, messaging
💻 all

[quix]: https://quix.io
[docs]: https://quix.io/docs/

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

Plugins support additional global and plugin configuration settings for tasks
such as modifying metrics, tags, and fields, creating aliases, and configuring
plugin ordering. See [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Secret store support

This plugin supports secrets from secret stores for the `token` option.
See the [secret store documentation][SECRETSTORE] for more details on how
to use them.

[SECRETSTORE]: ../../../docs/CONFIGURATION.md#secret-store-secrets

## Configuration

```toml @sample.conf
# Send metrics to a Quix data processing pipeline
[[outputs.quix]]
  ## Endpoint for providing the configuration
  # url = "https://portal-api.platform.quix.io"

  ## Workspace and topics to send the metrics to
  workspace = "your_workspace"
  topic = "your_topic"

  ## Authentication token created in Quix
  token = "your_auth_token"

  ## Amount of time allowed to complete the HTTP request for fetching the config
  # timeout = "5s"
```

The plugin requires a [SDK token][token] for authentication with Quix. You can
generate the `token` in settings under the `API and tokens` section.

Furthermore, the `workspace` parameter must be set to the `Workspace ID` or the
`Environment ID` of your Quix project. Those values can be found in settings
under the `General settings` section.

[token]: https://quix.io/docs/develop/authentication/personal-access-token.html

## Write timeout support

This plugin implements the optional context-aware output interface and
therefore supports the [`write_timeout`][write_timeout] output option. The
configured timeout bounds both producer (re)creation -- including the HTTP
call to fetch the broker configuration from the Quix API and the subsequent
Kafka producer dial -- and message delivery, so a write can no longer block
Telegraf indefinitely when either step gets stuck.

When a write is cancelled, the delivery outcome of the in-flight message is
unknown: it may or may not have reached the broker. Telegraf keeps the batch
for retry, so a cancelled write can result in duplicate messages. This also
applies when a slow (but otherwise healthy) broker exceeds `write_timeout`.
The producer used by the cancelled write is discarded and closed in the
background, and the next write creates a new one.

[write_timeout]: ../../../docs/CONFIGURATION.md#output-plugins
