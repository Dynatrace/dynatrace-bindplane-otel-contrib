# Exabeam Exporter

This exporter writes logs to the Google Cloud Storage bucket that Exabeam creates for a Cloud Collector, so Exabeam New-Scale SIEM can ingest them. Files use the naming and record format Exabeam expects.

## Supported Pipelines

- Logs

## How It Works

1. The [sending queue](#file-rollover) batches incoming logs. Each batch becomes one file.
2. The exporter turns each log record into one JSON line (see [Record Format](#record-format)) and gzips the result.
3. The file is uploaded to the root of the bucket through the GCS S3-compatible API, signed with SigV4 using the Cloud Collector's HMAC keys. See [Object Name](#object-name) for the file name.

## Notes

1. Exabeam gives you a connection string when you create the Cloud Collector. In most cases it is the only setting you need. Any setting it contains can be overridden individually.
2. The bucket only accepts HMAC credentials. Service account authentication is not supported.
3. The credentials can only write objects to the root of the bucket. The exporter never checks for the bucket or tries to create it.
4. Exabeam's parsers expect the original, unmodified log in `message`. If a pipeline parses or rewrites the log body, keep the original text in the `log.record.original` attribute. The exporter uses that attribute for `message` when it is present.

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
| siteid             | string |                                  | `false`  | Copied into every record. Overrides the connection string.                                                                                                                             |
| timezone_offset    | string |                                  | `false`  | Copied into every record, in the form `+HHMM` or `-HHMM`. Overrides the connection string.                                                                                            |
| collector_name     | string |                                  | `false`  | Copied into every record.                                                                                                                                                              |
| tags               | string |                                  | `false`  | Copied into every record as is, for example `"tag1":"value1", "tag2":"value2"`.                                                                                                        |
| endpoint           | string | `https://storage.googleapis.com` | `false`  | The S3-compatible endpoint to upload to.                                                                                                                                               |
| max_file_size      | int    | `10485760`                       | `false`  | The maximum uncompressed size of a file in bytes. A batch larger than this is split into several files.                                                                               |
| timeout            | string | `5s`                             | `false`  | The timeout for each upload. See [doc](https://github.com/open-telemetry/opentelemetry-collector/blob/main/exporter/exporterhelper/README.md) for details.                            |
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
  num_consumers: 10   # concurrent uploads
  queue_size: 1000
  batch:
    sizer: bytes
    min_size: 10485760   # 10 MiB
    max_size: 10485760   # 10 MiB
    flush_timeout: 30s
```

- **Size:** a batch is flushed once it reaches `min_size`, and is never larger than `max_size`. The batch sizer measures the OTLP size of the data. The exporter also splits a batch into several files at `max_file_size` bytes of uncompressed output, so no file is larger than that.
- **Open and idle time:** a batch is flushed at most `flush_timeout` after it opens. The 30s default keeps files within both of Exabeam's time limits.
- **Open files:** `num_consumers` sets how many files can be uploading at once.

If a batch is split into several files and a later upload fails, the retry uploads the whole batch again, so the earlier files in that batch are duplicated. Batches are normally smaller than `max_file_size`, so this is rare.

Buffered batches are uploaded when the collector shuts down. Configure a persistent queue (`sending_queue.storage`) to keep queued data across restarts.

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

Each file is gzipped newline-delimited JSON, with one record per log record:

```json
{"@timestamp":"2023-04-19T11:38:12.501Z","tags":"<tags>","collector_type":"cribl-logs","collector_name":"<collector_name>","collector_id":"<collector_id>","sitename":"<sitename>","siteid":"<siteid>","timezone_offset":"<timezone_offset>","message":"<raw log>"}
```

- `@timestamp` is the time the file is written, in UTC with millisecond precision. It is not the event time.
- `message` is the string value of the `log.record.original` attribute when present. Otherwise it is the log body: string and bytes bodies are written as is, and other body types as JSON.
- `collector_type` is `cribl-logs`, the type Exabeam ingests from Cribl today. It will change once Exabeam defines a collector type for Bindplane.

## Example Configurations

### Connection String

```yaml
exabeam:
  connection_string: ${env:EXABEAM_CONNECTION_STRING}
  collector_name: bindplane-gateway
  tags: '"env":"prod"'
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
