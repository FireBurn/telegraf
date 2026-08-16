# Executable Output Plugin

This plugin writes metrics to an external application via `stdin`. The command
will be executed on each write creating a new process. Metrics are passed in
one of the supported [data formats][data_formats].

The executable and the individual parameters must be defined as a list.
All outputs of the executable to `stderr` will be logged in the Telegraf log.

> [!TIP]
> For better performance consider execd which runs continuously.

⭐ Telegraf v1.12.0
🏷️ system
💻 all

[data_formats]: /docs/DATA_FORMATS_OUTPUT.md

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

Plugins support additional global and plugin configuration settings for tasks
such as modifying metrics, tags, and fields, creating aliases, and configuring
plugin ordering. See [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Write timeout support

This plugin implements the optional context-aware output interface and
therefore supports the [`write_timeout`][write_timeout] output option. It
bounds how long a single write attempt (one subprocess invocation, or one
metric's worth of invocations when `use_batch_format` is `false`) is allowed
to run in addition to the plugin's own `timeout` setting. If `write_timeout`
elapses first, the subprocess is killed immediately (skipping the normal
SIGTERM-then-SIGKILL grace period used by `timeout`) and reaped in the
background so the write call returns promptly; any metrics not yet sent to a
subprocess when cancellation happens are not written.

[write_timeout]: ../../../docs/CONFIGURATION.md#output-plugins

## Configuration

```toml @sample.conf
# Send metrics to command as input over stdin
[[outputs.exec]]
  ## Command to ingest metrics via stdin.
  command = ["tee", "-a", "/dev/null"]

  ## Environment variables
  ## Array of "key=value" pairs to pass as environment variables
  ## e.g. "KEY=value", "USERNAME=John Doe",
  ## "LD_LIBRARY_PATH=/opt/custom/lib64:/usr/local/libs"
  # environment = []

  ## Timeout for command to complete.
  # timeout = "5s"

  ## Whether the command gets executed once per metric, or once per metric batch
  ## The serializer will also run in batch mode when this is true.
  # use_batch_format = true

  ## Data format to output.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md
  # data_format = "influx"
```
