# Exabeam Exporter

This exporter writes logs to the Google Cloud Storage bucket that Exabeam creates for a Cloud Collector, so Exabeam New-Scale SIEM can ingest them. Files use the naming and record format Exabeam expects.

## Supported Pipelines

- Logs

## How It Works

1. The [sending queue](#file-rollover) batches incoming logs. Each batch becomes one file.
2. The exporter turns each log record into one JSON line (see [Record Format](#record-format)) and gzips the result. Processors can set per-record metadata through attributes (see [Per-Record Metadata](#per-record-metadata)).
3. The file is uploaded to the root of the bucket through the GCS S3-compatible API, signed with SigV4 using the Cloud Collector's HMAC keys. See [Object Name](#object-name) for the file name.

## Notes

1. Exabeam gives you a connection string when you create the Cloud Collector. In most cases it is the only setting you need. Any setting it contains can be overridden individually.
2. The bucket only accepts HMAC credentials. Service account authentication is not supported.
3. The credentials can only write objects to the root of the bucket. The exporter never checks for the bucket or tries to create it.
4. Exabeam's parsers expect the original, unmodified log in `message`. If a pipeline parses or rewrites the log body, keep the original text in the `log.record.original` attribute, which the exporter uses by default, or point `raw_log_field` at it.

## Configuration

| Field              | Type   | Default                          | Required | Description                                                                                                                                                                            |
| ------------------ | ------ | -------------------------------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| connection_string  | string |                                  | `false`* | The connection string from the Exabeam Cloud Collector: base64-encoded JSON containing the fields below. Never logged.                                                                 |
| collector_id       | string |                                  | `false`* | The Cloud Collector ID. Used in object names and copied into every record. Overrides the connection string.                                                                            |
| access_key         | string |                                  | `false`* | The HMAC access key. Overrides the connection string. Never logged.                                                                                                                    |
| secret_key         | string |                                  | `false`* | The HMAC secret key. Overrides the connection string. Never logged.                                                                                                                    |
| bucket_name        | string |                                  | `false`* | The bucket Exabeam created for the Cloud Collector. Overrides the connection string.                                                                                                   |
| bucket_location    | string |                                  | `false`  | The bucket location, used as the SigV4 signing region. `auto` is used when it is not set. Overrides the connection string.                                                            |
| sitename           | string |                                  | `false`  | Copied into every record. Overrides the connection string.                                                                                                                             |
| siteid             | string |                                  | `false`  | Copied into every record. Overrides the connection string. `attributes["exabeam_siteid"]` overrides it per record.                                                                   |
| timezone_offset    | string |                                  | `false`  | Copied into every record, in the form `+HHMM` or `-HHMM`. Overrides the connection string. `attributes["exabeam_timezone_offset"]` overrides it per record.                         |
| collector_name     | string |                                  | `false`  | Copied into every record.                                                                                                                                                              |
| hostname           | string | `host.name` resource attribute   | `false`  | Copied into every record. `attributes["exabeam_hostname"]` overrides it per record.                                                                                                   |
| forwarder          | string |                                  | `false`  | Copied into every record. `attributes["exabeam_forwarder"]` overrides it per record.                                                                                                  |
| logtags            | map    |                                  | `false`  | Up to 10 key/value pairs with non-empty names, copied into every record. Merged with `attributes["exabeam_logtags"]`. Exabeam makes each one searchable as `m_c_logtags_<name>`.     |
| raw_log_field      | string |                                  | `false`  | An OTTL expression for the raw log written to `message`, for example `body` or `attributes["raw"]`. Defaults to `attributes["log.record.original"]` when present, otherwise `body`. Maps and lists are written as JSON. If the expression fails for a record, that record uses the default and a warning is logged for the batch. |
| endpoint           | string | `https://storage.googleapis.com` | `false`  | The S3-compatible endpoint to upload to.                                                                                                                                               |
| max_file_size      | int    | `10000000`                       | `false`  | The maximum uncompressed size of a file in bytes. A batch larger than this is split into several files.                                                                               |
| timeout            | string | `30s`                            | `false`  | The timeout for each upload. See [doc](https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/exporterhelper/README.md) for details.                            |
| sending_queue      | map    | see [File Rollover](#file-rollover) | `false` | See [doc](https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/exporterhelper/README.md) for details.                                                      |
| retry_on_failure   | map    |                                  | `false`  | See [doc](https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/exporterhelper/README.md) for details.                                                          |

\* `collector_id`, `access_key`, `secret_key` and `bucket_name` are required, either in `connection_string` or set individually.

### Connection String

The connection string is the base64 encoding of a JSON object like this one:

```json
{
  "collector_id": "b557e4fc-...",
  "access_key": "<access-key>",
  "secret_key": "<secret-key>",
  "bucket_name": "dl-topic-ops-dist-cc-mgrtn-e2e",
  "bucket_location": "US",
  "sitename": "abcorp",
  "siteid": "xdfsdfw32342sfsdf",
  "timezone_offset": "-0700"
}
```

Standard and URL-safe base64 are accepted, with or without padding.

### File Rollover

Exabeam recommends rolling files over at 10 MB, 300 seconds open or 30 seconds idle, with at most 100 files open. The exporter does this with the batching in the exporterhelper sending queue, so the queue, retries and the optional persistent queue cover every file. The defaults are:

```yaml
sending_queue:
  num_consumers: 10   # batches uploading at once
  queue_size: 1000
  batch:
    sizer: bytes
    min_size: 10000000   # 10 MB
    max_size: 10000000   # 10 MB
    flush_timeout: 300s
```

- **Size:** a batch is flushed once it reaches `min_size`, and is never larger than `max_size`. The batch sizer measures the OTLP size of the data. The exporter also splits a batch into several files at `max_file_size` bytes of uncompressed output, so no file is larger than that.
- **Open time:** a batch is flushed at most `flush_timeout` after it opens, so the 300s default matches Exabeam's max open time. Adding data to a batch does not restart the timer.
- **Idle time:** the batcher has no idle timeout. A batch that stops receiving data is flushed when `flush_timeout` expires, so it can wait up to 300s instead of 30s. Lower `flush_timeout` to get data to Exabeam sooner, at the cost of smaller files.
- **Open files:** only one batch is filled at a time, which is within Exabeam's limit of 100 open files. `num_consumers` sets how many full batches can be uploading at once, so up to about `num_consumers` × `max_size` bytes of batches are held in memory while they upload.

A batch is often larger than `max_file_size` once encoded, because each record's metadata is added to it, so it is split into several files. If one of those uploads fails while the collector is running, only the records from that file onward are retried. Files that were already uploaded are not sent again.

Uploads rejected with a 4xx status other than 408 or 429, such as 403 for wrong keys or 404 for a missing bucket, are not retried, because retrying can't fix them. The records in the failed file and any later files of the batch are dropped. Network errors, timeouts, 408, 429 and 5xx responses are retried.

#### Shutdown

When the collector shuts down, retries stop and each buffered batch gets one upload attempt. What happens to a batch that still fails depends on the queue:

- **In-memory queue (default):** every queued batch, including the one being filled, is attempted once. A batch that fails is dropped.
- **Persistent queue (`sending_queue.storage`):** queued batches that haven't been read yet stay in storage. Batches being filled or uploaded are attempted once, and a batch that fails is kept and sent again after the collector restarts. If some of that batch's files were already uploaded, the whole batch is resent, so those files are uploaded twice.

Shutdown waits for these uploads to finish and has no time limit of its own. Each upload is bounded by `timeout`. If the process is killed first, for example when a Kubernetes termination grace period runs out, data that hasn't been uploaded is lost unless it is in a persistent queue.

### Object Name

Objects are written to the root of the bucket and named:

```
exa-cribl-logs_<collector_id>_1_YYYY_MM_DD_<HHMMSS><nanoseconds>_<random>.log.gz
```

The time is the upload time in UTC, and `<random>` is 16 random digits. For example:

```
exa-cribl-logs_a0d78162-e407-4079-b06c-62df4ed0e71a_1_2023_06_29_201820000000126_8520776225844066.log.gz
```

### Record Format

Each file is gzipped newline-delimited JSON, with one line per log record. Exabeam reads the event from the top-level `message` key:

```json
{"message":{"@timestamp":"2023-04-19T11:38:12.501Z","collector_type":"cribl-logs","collector_name":"<collector_name>","collector_id":"<collector_id>","message":"<raw log>","timezone_offset":"-0700","hostname":"<hostname>","forwarder":"<forwarder>","siteid":"<siteid>","sitename":"<sitename>","logtags":{"department":"HR"}}}
```

- `@timestamp` is the time the file is written, in UTC with millisecond precision. It is not the event time.
- `message` is the raw log, from `raw_log_field` when it is set. Otherwise it is the string value of the `log.record.original` attribute when present, or else the log body: string and bytes bodies are written as is, and other body types as JSON.
- `collector_type` is `cribl-logs`, the type Exabeam ingests from Cribl today. It will change once Exabeam defines a collector type for Bindplane.
- Empty fields are left out, except `message`.

### Per-Record Metadata

The configured metadata applies to every record. To set it per record, for example per source in a pipeline, set these log record attributes with a processor:

| Attribute                 | Type   | Effect                                                                         |
| ------------------------- | ------ | ------------------------------------------------------------------------------ |
| `exabeam_hostname`        | string | Replaces `hostname`.                                                           |
| `exabeam_forwarder`       | string | Replaces `forwarder`.                                                          |
| `exabeam_siteid`          | string | Replaces `siteid`.                                                             |
| `exabeam_timezone_offset` | string | Replaces `timezone_offset` when it is in the form `+HHMM` or `-HHMM`.          |
| `exabeam_logtags`         | map    | Merged with `logtags`. On a key conflict the attribute's value wins.           |

Empty and non-string values are ignored. In `exabeam_logtags`, entries with empty names are ignored, and new names are added in name order until there are 10 logtags. The attributes are not otherwise written to Exabeam.

```yaml
processors:
  transform/exabeam:
    log_statements:
      - set(log.attributes["exabeam_logtags"]["source"], "firewall")
      - set(log.attributes["exabeam_siteid"], "site-eu") where resource.attributes["region"] == "eu"
```

## Example Configurations

### Connection String

```yaml
exabeam:
  connection_string: ${env:EXABEAM_CONNECTION_STRING}
  collector_name: bindplane-gateway
  logtags:
    env: prod
```

### Individual Settings with a Persistent Queue

```yaml
extensions:
  file_storage:
    directory: /var/lib/otelcol/storage

exporters:
  exabeam:
    collector_id: b557e4fc-1234-4567-89ab-cdef01234567
    access_key: ${env:EXABEAM_ACCESS_KEY}
    secret_key: ${env:EXABEAM_SECRET_KEY}
    bucket_name: dl-topic-ops-dist-cc-mgrtn-e2e
    bucket_location: US
    sitename: abcorp
    siteid: xdfsdfw32342sfsdf
    timezone_offset: "-0700"
    sending_queue:
      storage: file_storage
```

## Telemetry

The exporter reports the same metrics as the Google Cloud Storage exporter. See [documentation.md](documentation.md).
