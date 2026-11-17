# Chronicle Exporter

This exporter facilitates the sending of logs to Chronicle, which is a security analytics platform provided by Google. It is designed to integrate with OpenTelemetry collectors to export telemetry data such as logs to a Chronicle account.

## Supported Pipelines

- Logs

## How It Works

1. The exporter uses the configured credentials to authenticate with the Google Cloud services.
2. It marshals logs into the format expected by Chronicle.
3. It sends the logs to the appropriate Chronicle endpoint over the configured protocol.

## Protocols and APIs

The exporter supports two protocols, each of which targets a different Chronicle ingestion API:

| `protocol` | Target API                                                                                                            | Recommended |
| ---------- | --------------------------------------------------------------------------------------------------------------------- | ----------- |
| `https`    | [Chronicle API](https://docs.cloud.google.com/chronicle/docs/reference/ingestion-methods)                             | Yes         |
| `gRPC`     | [Backstory (Malachite) Ingestion API](https://docs.cloud.google.com/chronicle/docs/reference/ingestion-api)           |             |

**`https` is the recommended protocol.** It targets the newer Chronicle API, supports the `v1alpha`, `v1beta`, and `v1` API versions, and uses improved security practices. The `gRPC` protocol targets the legacy Backstory Ingestion API and is retained for backwards compatibility with existing deployments.

## Configuration

The exporter can be configured using the following fields:

| Field                           | Type              | Default                                | Required | Description                                                                                                                                                                              |
| ------------------------------- | ----------------- | -------------------------------------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `protocol`                      | string            | `gRPC`                                 | `false`  | The protocol used to send logs. Valid values are `gRPC` and `https`. See [Protocols and APIs](#protocols-and-apis).                                                                      |
| `endpoint`                      | string            | `malachiteingestion-pa.googleapis.com` | `false`  | The endpoint for sending to Chronicle. Must not contain a `http://` or `https://` prefix. For `https`, the endpoint is combined with `location` (e.g. `chronicle.googleapis.com`) unless `override_endpoint` is set. |
| `location`                      | string            |                                        | `false`  | The Chronicle region (e.g. `us`, `europe`). Required when `protocol` is `https`.                                                                                                         |
| `project`                       | string            |                                        | `false`  | The Google Cloud project ID. Required when `protocol` is `https`.                                                                                                                        |
| `customer_id`                   | string            |                                        | `false`  | The Chronicle customer (instance) ID used for sending logs. Required when `protocol` is `https`.                                                                                         |
| `api_version`                   | string            | `v1alpha`                              | `false`  | The Chronicle API version to use. Valid values are `v1alpha`, `v1beta`, and `v1`. Only applies to `https` protocol.                                                                             |
| `override_endpoint`             | bool              | `false`                                | `false`  | Whether to ignore the `location` field when constructing the endpoint. Only applies to `https` protocol.                                                                                 |
| `creds_file_path`               | string            |                                        | `false`  | The file path to the Google credentials JSON file. Mutually exclusive with `creds`. If neither is set, the exporter falls back to Google Application Default Credentials.                |
| `creds`                         | string            |                                        | `false`  | The Google credentials JSON. Mutually exclusive with `creds_file_path`. If neither is set, the exporter falls back to Google Application Default Credentials.                            |
| `token_url`                     | string            |                                        | `false`  | Overrides the OAuth2 token endpoint, e.g. a Private Service Connect endpoint. Must be an `https://` URL with no user info, query, or fragment. Requires `creds` or `creds_file_path` with a service account key. Applies to both protocols. See [Private Connectivity](#private-connectivity). |
| `log_type`                      | string            |                                        | `false`  | The default Chronicle log type that will be sent.                                                                                                                                        |
| `override_log_type`             | bool              | `true`                                 | `false`  | Whether or not to override the `log_type` in the config with `attributes["log_type"]`.                                                                                                   |
| `validate_log_types`            | bool              | `false`                                | `false`  | Whether to validate configured log types against Chronicle via an API call on startup. Only applies to `https` protocol.                                                                 |
| `raw_log_field`                 | string            |                                        | `false`  | An OTTL expression for the field that will be used as the raw log payload.                                                                                                               |
| `namespace`                     | string            |                                        | `false`  | User-configured environment namespace to identify the data domain the logs originated from.                                                                                              |
| `compression`                   | string            | `none`                                 | `false`  | The compression type to use when sending logs. Valid values are `none` and `gzip`.                                                                                                       |
| `ingestion_labels`              | map[string]string |                                        | `false`  | Key-value pairs of labels to be applied to the logs when sent to Chronicle.                                                                                                              |
| `rbac_enabled_labels`           | []string          |                                        | `false`  | Ingestion label keys that are eligible for Data RBAC. Labels not listed are sent with `rbac_enabled: false`. Only supported with the `https` protocol; rejected at startup with `gRPC`. |
| `collect_agent_metrics`         | bool              | `true`                                 | `false`  | Enables collecting metrics about the agent's process and log ingestion metrics.                                                                                                          |
| `metrics_interval`              | duration          | `5m`                                   | `false`  | The interval at which agent metrics are collected and sent. Only applies when `collect_agent_metrics` is `true`.                                                                         |
| `batch_request_size_limit_grpc` | int               | `4000000`                              | `false`  | The maximum size, in bytes, allowed for a gRPC batch creation request.                                                                                                                   |
| `batch_request_size_limit_http` | int               | `4000000`                              | `false`  | The maximum size, in bytes, allowed for an HTTPS batch creation request.                                                                                                                 |
| `log_errored_payloads`          | bool              | `false`                                | `false`  | Whether to log payloads that fail to send. Useful for debugging.                                                                                                                         |
| `http_response_header_timeout`  | duration          | `10s`                                  | `false`  | The amount of time to wait for the HTTP response headers after sending requests to Chronicle. Only applies to `https` protocol.                                                          |

### Log Type

If the `attributes["log_type"]` field is present in the log, and maps to a known Chronicle `log_type` the exporter will use the value of that field as the log type. If the `attributes["log_type"]` field is not present, the exporter will use the value of the `log_type` configuration field as the log type.

Currently supported `attributes["log_type"]` values that map to a Chronicle log type are:

- `windows_event.security` → `WINEVTLOG`
- `windows_event.application` → `WINEVTLOG`
- `windows_event.system` → `WINEVTLOG`
- `sql_server` → `MICROSOFT_SQL`

If the `attributes["chronicle_log_type"]` field is present in the log, its value will be used in the payload instead of the automatic detection or the `log_type` in the config.

### Namespace and Ingestion Labels

If the `attributes["chronicle_namespace"]` field is present in the log, its value will be used in the payload instead of the `namespace` in the config.

Any labels defined by key value pairs in a nested map at `attributes["chronicle_ingestion_label"]` are merged with the `ingestion_labels` in the config. When the same label key is set in both places, the value from the attribute takes precedence over that from the config.

### Data RBAC

Google SecOps can restrict access to ingested data based on ingestion labels, but only labels marked as RBAC-eligible at ingestion time are considered. List the label keys that should be eligible in `rbac_enabled_labels`; every other label is sent with `rbac_enabled: false`. The flag is matched by label key, so it also applies to labels supplied through `attributes["chronicle_ingestion_label"]`.

Sending `rbac_enabled: true` is necessary but not sufficient. Data RBAC only takes effect once the SecOps tenant is set up for it: Google must enable ingestion-label based Data RBAC for the tenant (log-level ingestion labels are not generally available and are enabled per tenant), and an administrator must define the [Data RBAC scopes](https://cloud.google.com/chronicle/docs/administration/datarbac-overview) and assign the Restricted Data Access role to the affected users. Until then the flag is carried on the ingested logs but has no visible effect.

This option is only supported with the `https` protocol (v1alpha, v1beta and v1). The legacy `gRPC` protocol has no equivalent field, so the exporter fails validation at startup if `rbac_enabled_labels` is set together with `protocol: gRPC`.

## Credentials

This exporter requires a Google Cloud service account with access to the Chronicle API. The service account must have access to the endpoint specified in the config.

For additional information on accessing Chronicle, see the [Chronicle documentation](https://cloud.google.com/chronicle/docs/reference/ingestion-api#getting_api_authentication_credentials).

### Private Connectivity

When the collector reaches Google Cloud through a private path, such as Cloud Interconnect with a [Private Service Connect endpoint](https://cloud.google.com/vpc/docs/configure-private-service-connect-apis), both the Chronicle API and the OAuth2 token endpoint may need private hostnames:

- Set `endpoint` to the private Chronicle hostname. For `https`, also set `override_endpoint: true`.
- Set `token_url` to the private OAuth2 token endpoint. Access tokens are requested from this URL instead of the `token_uri` in the service account key. The JWT assertion audience remains `https://oauth2.googleapis.com/token`, as Google requires.

The hostnames in this README, such as `oauth2-myendpoint.p.googleapis.com`, are illustrative. Use the hostnames your network team has set up for your Private Service Connect endpoint. Google creates DNS records automatically for only some services, so you may need to create records for these hostnames yourself.

`token_url` must be an `https://` URL with no user info, query, or fragment. It requires a service account key supplied through `creds` or `creds_file_path`. Other credential types, and Application Default Credentials, are not supported with `token_url`.

When `token_url` is set, the exporter requests a token in the background at startup and logs a warning if the request fails. Startup is not blocked. Each token request times out after 30 seconds. If the token endpoint can't be reached, times out, or returns a server error during an export, the export is retried according to `retry_on_failure`, for both protocols.

Requests to both endpoints honor the `HTTPS_PROXY` and `NO_PROXY` environment variables.

## Regional Endpoints

For `gRPC`, regional endpoints for the Backstory Ingestion API are listed [here](https://cloud.google.com/chronicle/docs/reference/ingestion-api#regional_endpoints).

For `https`, regional endpoints for the Chronicle API are listed [here](https://docs.cloud.google.com/chronicle/docs/reference/rest#regional-service-endpoint).

## Log Batch Creation Request Limits

`batch_request_size_limit_grpc` and `batch_request_size_limit_http` are used to ensure log batch creation requests don't exceed Chronicle's backend limits — the former for the `gRPC` protocol, and the latter for the `https` protocol. If a request exceeds the configured size limit, it will be split into multiple requests that adhere to this limit, with each request containing a subset of the logs from the original. Any single log that on its own exceeds the size limit will be dropped.

The `otelcol_exporter_unsplit_payload_size` metric reports the size, in bytes, of each export batch *before* it is split — that is, before it is broken up to satisfy `batch_request_size_limit_http` and the one-log-type-per-request grouping. Compare it against `batch_request_size_limit_http`: when values approach or exceed the limit, batches are being split into multiple HTTP requests. Lowering the `batch` processor's `send_batch_size` until this metric stays below the limit keeps each upstream batch a single HTTP request, reducing overhead.

## Agent Metrics

When `collect_agent_metrics` is `true` (the default), the exporter periodically reports collector process and ingestion stats to Chronicle, surfacing metrics in GCP's Metrics Explorer which can be found nested under the "Chronicle Collector" resource and the "Agent" metric category.

Every `metrics_interval` (default `5m`), the exporter samples process CPU seconds, resident memory (RSS), and uptime, combines them with counts of logs accepted and refused since the last successful upload, and sends them to Chronicle. On success the window is reset; on failure it is retained so the next tick re-attempts with the accumulated counts.

Setting `collect_agent_metrics: false` disables the reporter without affecting log delivery.

## Example Configurations

### Basic Configuration (gRPC / Backstory Ingestion API)

```yaml
chronicle:
  creds_file_path: "/path/to/google/creds.json"
  log_type: "ABSOLUTE"
  customer_id: "customer-123"
```

### Basic Configuration with Regional Endpoint (gRPC)

```yaml
chronicle:
  endpoint: europe-malachiteingestion-pa.googleapis.com
  creds_file_path: "/path/to/google/creds.json"
  log_type: "ONEPASSWORD"
  customer_id: "customer-123"
```

### Basic Configuration (HTTPS / Chronicle API — recommended)

```yaml
chronicle:
  protocol: https
  endpoint: chronicle.googleapis.com
  location: us
  project: my-gcp-project
  customer_id: "customer-123"
  creds_file_path: "/path/to/google/creds.json"
  log_type: "ABSOLUTE"
```

### HTTPS with a Regional Endpoint

The `https` protocol prefixes `endpoint` with the value of `location` to build the regional URL. The example below targets `https://europe-chronicle.googleapis.com/...`:

```yaml
chronicle:
  protocol: https
  endpoint: chronicle.googleapis.com
  location: europe
  project: my-gcp-project
  customer_id: "customer-123"
  creds_file_path: "/path/to/google/creds.json"
  log_type: "ABSOLUTE"
```

### HTTPS with a Custom Endpoint (override_endpoint)

Set `override_endpoint: true` to use the `endpoint` value verbatim instead of prefixing it with `location`. `location` is still required because it is included in the request URL path and API calls.

```yaml
chronicle:
  protocol: https
  endpoint: chronicle.us.rep.googleapis.com
  override_endpoint: true
  location: us
  project: my-gcp-project
  customer_id: "customer-123"
  creds_file_path: "/path/to/google/creds.json"
  log_type: "ABSOLUTE"
```

### HTTPS with Private Service Connect

Send logs and token requests through Private Service Connect endpoints. The hostnames are illustrative; use the ones set up for your network. See [Private Connectivity](#private-connectivity).

```yaml
chronicle:
  protocol: https
  endpoint: chronicle-myendpoint.p.googleapis.com
  override_endpoint: true
  token_url: https://oauth2-myendpoint.p.googleapis.com/token
  location: us
  project: my-gcp-project
  customer_id: "customer-123"
  creds_file_path: "/path/to/google/creds.json"
  log_type: "ABSOLUTE"
```

### HTTPS with the v1beta API Version

```yaml
chronicle:
  protocol: https
  endpoint: chronicle.googleapis.com
  location: us
  project: my-gcp-project
  customer_id: "customer-123"
  api_version: v1beta
  creds_file_path: "/path/to/google/creds.json"
  log_type: "ABSOLUTE"
```

### Configuration with Ingestion Labels

```yaml
chronicle:
  creds_file_path: "/path/to/google/creds.json"
  log_type: ""
  customer_id: "customer-123"
  ingestion_labels:
    env: dev
    zone: USA
```

### Configuration with Data RBAC Ingestion Labels

```yaml
chronicle:
  protocol: https
  location: us
  project: my-project
  creds_file_path: "/path/to/google/creds.json"
  customer_id: "customer-123"
  ingestion_labels:
    env: dev
    zone: USA
  rbac_enabled_labels:
    - zone
```
