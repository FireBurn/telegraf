# Apache IoTDB Output Plugin

This plugin writes metrics to an [Apache IoTDB][iotdb] instance, a database
for the Internet of Things, supporting session connection and data insertion.

⭐ Telegraf v1.24.0
🏷️ datastore
💻 all

[iotdb]: https://iotdb.apache.org

## Getting started

Before using this plugin, please configure the IP address, port number,
user name, password and other information of the database server,
as well as some data type conversion, time unit and other configurations.

Please see the [configuration section](#configuration) for an example
configuration.

## Metric Translation

IoTDB uses a different data format for metric data than telegraf. It is
important to note that depending on the metrics being written, the translation
may be lossy. This plugin translates to IoTDB format in the following ways:

### Unsigned Integers

IoTDB currently **DOES NOT support unsigned integer**.
There are three available options of converting uint64, which are specified by
setting `uint64_conversion`.

- `int64_clip`, default option. If an unsigned integer is greater than
`math.MaxInt64`, save it as `int64`; else save `math.MaxInt64`
(9223372036854775807).
- `int64`, force converting an unsigned integer to a`int64`,no matter
what the value it is. This option may lead to exception if the value is
greater than `int64`.
- `text`force converting an unsigned integer to a string, no matter what the
value it is.

### Time Precision

IoTDB supports a variety of time precision. You can specify which precision
you want using the `timestamp_precision` setting. Default is `nanosecond`.
Other options are `second`, `millisecond`, `microsecond`.

### Metadata (tags)

IoTDB uses a tree model for metadata while Telegraf uses a tag model
(see [InfluxDB-Protocol Adapter][InfluxDB-Protocol Adapter]).
There are two available options of converting tags, which are specified by
setting `convert_tags_to`:

- `fields`. Treat Tags as measurements. For each Key:Value in Tag,
convert them into Measurement, Value, DataType, which are supported in IoTDB.
- `device_id`, default option. Treat Tags as part of device id. Tags
constitute a subtree of `Name`.

For example, there is a metric:

```markdown
Name="root.sg.device", Tags={tag1="private", tag2="working"}, Fields={s1=100, s2="hello"}
```

- `fields`, result: `root.sg.device, s1=100, s2="hello", tag1="private", tag2="working"`
- `device_id`, result: `root.sg.device.private.working, s1=100, s2="hello"`

[InfluxDB-Protocol Adapter]: https://iotdb.apache.org/UserGuide/Master/API/InfluxDB-Protocol.html

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

## Configuration

```toml @sample.conf
# Save metrics to an IoTDB Database
[[outputs.iotdb]]
  ## Configuration of IoTDB server connection
  host = "127.0.0.1"
  # port = "6667"

  ## Configuration of authentication
  # user = "root"
  # password = "root"

  ## Timeout to open a new session.
  ## A value of zero means no timeout.
  # timeout = "5s"

  ## Configuration of type conversion for 64-bit unsigned int
  ## IoTDB currently DOES NOT support unsigned integers (version 13.x).
  ## 32-bit unsigned integers are safely converted into 64-bit signed integers by the plugin,
  ## however, this is not true for 64-bit values in general as overflows may occur.
  ## The following setting allows to specify the handling of 64-bit unsigned integers.
  ## Available values are:
  ##   - "int64"       --  convert to 64-bit signed integers and accept overflows
  ##   - "int64_clip"  --  convert to 64-bit signed integers and clip the values on overflow to 9,223,372,036,854,775,807
  ##   - "text"        --  convert to the string representation of the value
  # uint64_conversion = "int64_clip"

  ## Configuration of TimeStamp
  ## TimeStamp is always saved in 64bits int. timestamp_precision specifies the unit of timestamp.
  ## Available value:
  ## "second", "millisecond", "microsecond", "nanosecond"(default)
  # timestamp_precision = "nanosecond"

  ## Handling of tags
  ## Tags are not fully supported by IoTDB.
  ## A guide with suggestions on how to handle tags can be found here:
  ##     https://iotdb.apache.org/UserGuide/Master/API/InfluxDB-Protocol.html
  ##
  ## Available values are:
  ##   - "fields"     --  convert tags to fields in the measurement
  ##   - "device_id"  --  attach tags to the device ID
  ##
  ## For Example, a metric named "root.sg.device" with the tags `tag1: "private"`  and  `tag2: "working"` and
  ##  fields `s1: 100`  and `s2: "hello"` will result in the following representations in IoTDB
  ##   - "fields"     --  root.sg.device, s1=100, s2="hello", tag1="private", tag2="working"
  ##   - "device_id"  --  root.sg.device.private.working, s1=100, s2="hello"
  # convert_tags_to = "device_id"

  ## Handling of unsupported characters
  ## Some characters in different versions of IoTDB are not supported in path name
  ## A guide with suggestions on valid paths can be found here:
  ## for iotdb 0.13.x           -> https://iotdb.apache.org/UserGuide/V0.13.x/Reference/Syntax-Conventions.html#identifiers
  ## for iotdb 1.x.x and above  -> https://iotdb.apache.org/UserGuide/V1.3.x/User-Manual/Syntax-Rule.html#identifier
  ##
  ## Available values are:
  ##   - "1.0", "1.1", "1.2", "1.3"  -- use backticks to enclose tags with forbidden characters
  ##                                    such as @$#:[]{}() and space
  ##   - "0.13"                      -- use backticks to enclose tags with forbidden characters
  ##                                    such as space
  ## Keep this section commented if you don't want to sanitize the path
  # sanitize_tag = "1.3"
```

## Write timeout support

This plugin implements the optional context-aware output interface and
therefore supports the [`write_timeout`][write_timeout] output option, with
important caveats specific to this plugin.

The underlying Apache IoTDB client is a raw Thrift-generated `Session` with
no context support anywhere in its call path and no internal locking: it is
only safe for one goroutine to use a given session at a time. This is
different from `outputs.kafka` and `outputs.nsq`, whose client libraries are
documented as safe to close concurrently with an in-flight call. For IoTDB,
closing a session while a call is still in flight on it risks interleaving
two unsynchronized requests on the same Thrift transport, which can corrupt
the wire framing for both or race on the session's unguarded fields.

Because of this, when `write_timeout` fires:

- The connection attempt or write is abandoned and `write_timeout`/
  `ConnectContext`/`WriteContext` return promptly, as with any other
  context-aware output.
- The abandoned session is **not** closed at that point. It is handed off to
  the still-running goroutine, which closes it once the original call
  itself eventually returns -- successfully, with an error, or (in the
  worst case) never, if the IoTDB server is completely unresponsive and
  never resets the TCP connection. In that worst case the abandoned
  session's goroutine and connection are never reclaimed.
- The number of concurrently abandoned sessions is capped (2, matching
  `outputs.kafka`'s producer-replacement limit) so repeated timeouts fail
  fast with an explicit error instead of accumulating unboundedly, but this
  bounds the *count* of leaked sessions, not the *time* any one of them
  stays open.
- As with the other context-aware outputs, the delivery outcome of a
  cancelled write is unknown -- Telegraf keeps the batch for retry, so a
  cancelled write can result in duplicate inserts.

In short: `write_timeout` reliably bounds how long Telegraf itself waits on
a stuck IoTDB session, and a new session is created for the next write, but
it cannot guarantee the old session's resources are ever forcibly reclaimed
the way it can for outputs backed by libraries with internally
synchronized, concurrency-safe close semantics.

[write_timeout]: ../../../docs/CONFIGURATION.md#output-plugins
