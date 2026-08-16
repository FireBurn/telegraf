# Azure Event Hubs Output Plugin

This plugin writes metrics to the [Azure Event Hubs][event_hubs] service in any
of the supported [data formats][data_formats]. Metrics are sent as batches with
each message payload containing one metric object, preferably as JSON as this
eases integration with downstream components.

Each patch is sent to a single Event Hub within a namespace. In case no
partition key is specified the batches will be automatically load-balanced
(round-robin) across all the Event Hub partitions.

⭐ Telegraf v1.21.0
🏷️ cloud,datastore
💻 all

[event_hubs]: https://azure.microsoft.com/en-gb/services/event-hubs/
[data_formats]: /docs/DATA_FORMATS_OUTPUT.md

## Global configuration options <!-- @/docs/includes/plugin_config.md -->

Plugins support additional global and plugin configuration settings for tasks
such as modifying metrics, tags, and fields, creating aliases, and configuring
plugin ordering. See [CONFIGURATION.md][CONFIGURATION.md] for more details.

[CONFIGURATION.md]: ../../../docs/CONFIGURATION.md#plugins

## Write timeout support

This plugin implements the optional context-aware output interface and
therefore supports the [`write_timeout`][write_timeout] output option. When
set, the deadline bounds batch creation and the send call(s) made for one
write, in addition to the plugin's own `timeout` setting.

When a write is cancelled, the delivery outcome of the in-flight batch is
unknown: Event Hubs may or may not have received some or all of the events.
Telegraf keeps the batch for retry, so a cancelled write can result in
duplicate events.

[write_timeout]: ../../../docs/CONFIGURATION.md#output-plugins

## Configuration

```toml @sample.conf
# Configuration for Event Hubs output plugin
[[outputs.event_hubs]]
  ## Full connection string to the Event Hub instance. The shared access key
  ## must have "Send" permissions on the target Event Hub.
  connection_string = "Endpoint=sb://namespace.servicebus.windows.net/;SharedAccessKeyName=RootManageSharedAccessKey;SharedAccessKey=superSecret1234=;EntityPath=hubName"

  ## Partition key to use for the event
  ## Metric tag or field name to use for the event partition key. The value of
  ## this tag or field is set as the key for events if it exists. If both, tag
  ## and field, exist the tag is preferred.
  # partition_key = ""

  ## Set the maximum batch message size in bytes
  ## The allowable size depends on the Event Hub tier, see
  ##   https://learn.microsoft.com/azure/event-hubs/event-hubs-quotas#basic-vs-standard-vs-premium-vs-dedicated-tiers
  ## for details. If unset the default size defined by Azure Event Hubs is
  ## used (currently 1,000,000 bytes)
  # max_message_size = "1MB"

  ## Timeout for sending the data
  # timeout = "30s"

  ## Data format to output.
  ## Each data format has its own unique set of configuration options, read
  ## more about them here:
  ## https://github.com/influxdata/telegraf/blob/master/docs/DATA_FORMATS_OUTPUT.md
  data_format = "json"
```
