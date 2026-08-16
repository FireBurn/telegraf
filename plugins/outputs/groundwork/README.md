# GroundWork Output Plugin

This plugin writes metrics to a [GroundWork Monitor][groundwork] instance.

> [!IMPORTANT]
> Plugin only supports GroundWork v8 or later.

⭐ Telegraf v1.21.0
🏷️ applications, messaging
💻 all

[groundwork]: https://www.gwos.com/product/groundwork-monitor/

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

Plugins support additional global and plugin configuration settings for tasks
such as modifying metrics, tags, and fields, creating aliases, and configuring
plugin ordering. See [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Secret store support

This plugin supports secrets from secret stores for the `username` and
`password` option.
See the [secret store documentation][SECRETSTORE] for more details on how
to use them.

[SECRETSTORE]: ../../../docs/CONFIGURATION.md#secret-store-secrets

## Write timeout support

This plugin implements the optional context-aware output interface and
therefore supports the [`write_timeout`][write_timeout] output option. The
configured deadline bounds both the login request made during `Connect`
and the outbound `SendResourcesWithMetrics` request made during `Write`.
One caveat found during investigation: the underlying GroundWork SDK client
does not accept a context on its login call, so on cancellation it is raced
in a goroutine and abandoned rather than cancelled outright; the abandoned
login is safe to leave running because the SDK client guards its own token
state with an internal mutex. Separately, if a write request itself gets a
401 response, the SDK transparently re-authenticates using that same
context-less login call before retrying, so that particular re-auth
sub-step is not bounded by `write_timeout`, only by the SDK's own default
40s HTTP client timeout.

[write_timeout]: ../../../docs/CONFIGURATION.md#output-plugins

## Configuration

```toml @sample.conf
# Send telegraf metrics to GroundWork Monitor
[[outputs.groundwork]]
  ## URL of your groundwork instance.
  url = "https://groundwork.example.com"

  ## Agent uuid for GroundWork API Server.
  agent_id = ""

  ## Username and password to access GroundWork API.
  username = ""
  password = ""

  ## Default application type to use in GroundWork client
  # default_app_type = "TELEGRAF"

  ## Default display name for the host with services(metrics).
  # default_host = "telegraf"

  ## Default service state.
  # default_service_state = "SERVICE_OK"

  ## The name of the tag that contains the hostname.
  # resource_tag = "host"

  ## The name of the tag that contains the host group name.
  # group_tag = "group"
```

## List of tags used by the plugin

* __group__ - to define the name of the group you want to monitor,
  can be changed with config.
* __host__ - to define the name of the host you want to monitor,
  can be changed with config.
* __service__ - to define the name of the service you want to monitor.
* __status__ - to define the status of the service. Supported statuses:
  "SERVICE_OK", "SERVICE_WARNING", "SERVICE_UNSCHEDULED_CRITICAL",
  "SERVICE_PENDING", "SERVICE_SCHEDULED_CRITICAL", "SERVICE_UNKNOWN".
* __message__ - to provide any message you want,
  it overrides __message__ field value.
* __unitType__ - to use in monitoring contexts (subset of The Unified Code for
  Units of Measure standard). Supported types: "1", "%cpu", "KB", "GB", "MB".
* __critical__ - to define the default critical threshold value,
  it overrides value_cr field value.
* __warning__ - to define the default warning threshold value,
  it overrides value_wn field value.
* __value_cr__ - to define critical threshold value,
  it overrides __critical__ tag value and __value_cr__ field value.
* __value_wn__ - to define warning threshold value,
  it overrides __warning__ tag value and __value_wn__ field value.

## NOTE

The current version of GroundWork Monitor does not support metrics whose values
are strings. Such metrics will be skipped and will not be added to the final
payload. You can find more context in this pull request: [#10255][].

[#10255]: https://github.com/influxdata/telegraf/pull/10255
