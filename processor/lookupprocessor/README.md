# Lookup Processor

This processor enriches telemetry by looking up values from an external data
lookup and adding the resulting fields to the configured `context`.

## Supported pipelines
- Logs
- Metrics
- Traces

## How It Works
1. The processor loads a lookup source. Exactly one of `csv`, `json`, `redis`,
   `api`, or `puppetdb` must be configured.
2. When telemetry is received, the processor checks if the configured `field`
   exists in the configured `context`.
3. If the field exists and the lookup source returns a match, all other key/value
   pairs from the matching row are added to the `context` of the telemetry.
4. For a `redis` or `api` lookup, an optional cache (enabled by default with a
   5-minute TTL) stores recent results. A `csv` or `json` file or a PuppetDB
   inventory is already held in memory, so it is looked up directly and the cache
   does not apply. The cache backend is
   either an OpenTelemetry storage extension (e.g. `file_storage`,
   `redis_storage`) for persistence across restarts, or a per-instance in-memory
   map when no `storage` is configured.

   The in-memory backend is bounded by `cache_max_entries`. Reads never
   mutate the cache; an expired entry is simply reported as a miss, so
   concurrent lookups do not serialize. Reclamation happens on insert: when
   an insert pushes the cache over `cache_max_entries`, expired entries are
   evicted first, then arbitrary entries until the cache is back under the
   limit.

   The storage backend has no such bound; an expired entry is removed only
   on the next read of the same key, and a key that is never looked up again
   remains until the storage extension reclaims it. For high-cardinality
   keys that rarely repeat, prefer a storage extension with bounded
   retention or disable the cache.

## Configuration

### Common
| Field          | Type            | Default | Description |
| ---            | ---             | ---     | --- |
| context        | string          | ` `     | Telemetry context to read/write. One of `attributes`, `body`, `resource.attributes`. |
| field          | string          | ` `     | Field in `context` whose value is used as the lookup key. |
| source_type    | string          | ` `     | Optional. One of `csv`, `json`, `redis`, `api`, `puppetdb`. When unset, the lookup source is inferred from the source block. |
| reload_interval | duration       | `60s`   | How often the lookup source is re-checked for changes. An unchanged file is skipped without re-reading it. A PuppetDB inventory is re-fetched each time. A negative value is rejected. |
| cache_enabled  | bool            | `true`  | Enable TTL caching of lookup results. Not used with a `csv` or `json` file or PuppetDB. |
| cache_ttl      | duration        | `5m`    | Cache entry lifetime. Not used with a `csv` or `json` file or PuppetDB. |
| cache_max_entries | int          | `100000` | Maximum entries held by the in-memory cache. On overflow, expired entries are evicted first, then arbitrary entries. Ignored when `storage` is set, and not used with a `csv` or `json` file or PuppetDB. |
| storage        | component.ID    | `nil`   | Storage extension to back the cache (e.g. `file_storage`). When unset, the cache is in-memory and discarded on restart. Not used with a `csv` or `json` file or PuppetDB. |
| csv            | string          | ` `     | Path to CSV file. See [CSV Lookup Source](#csv-lookup-source). |
| json           | string          | ` `     | Path to JSON file. See [JSON Lookup Source](#json-lookup-source). |
| redis          | object          | `nil`   | Redis config. See [Redis Lookup Source](#redis-lookup-source). |
| api            | object          | `nil`   | API config. See [API Lookup Source](#api-lookup-source). |
| puppetdb       | object          | `nil`   | PuppetDB or OpenVoxDB config. See [PuppetDB Lookup Source](#puppetdb-lookup-source). |

### CSV Lookup Source
| Field | Type   | Default | Description |
| ---   | ---    | ---     | --- |
| csv   | string | ` `     | Filesystem path to a CSV file. The first row is the header. Re-checked on the `reload_interval` cadence (default one minute), and re-read only when it has changed. |

The top-level `field` setting doubles as the CSV column name used to look up
rows. The remaining columns of the matching row are added to the configured
`context`.

The cache settings (`cache_enabled`, `cache_ttl`, `cache_max_entries`, `storage`)
do not apply to a CSV file. The CSV index is already held in memory, so lookups
never go through the cache. Setting any of them alongside `csv` logs a warning at
startup, and will become a configuration error in a future release.

### JSON Lookup Source
| Field | Type   | Default | Description |
| ---   | ---    | ---     | --- |
| json  | string | ` `     | Filesystem path to a JSON file. Re-checked on the `reload_interval` cadence (default one minute), and re-read only when it has changed. |

The file is a JSON object whose keys are lookup keys and whose values are objects
of attributes to add. Unlike a CSV's fixed columns, each key may carry a different
set of fields, so only the fields present for the matched key are added to the
configured `context`. The top-level `field` setting supplies the lookup key; it is
not a column name.

All attribute values are added as strings: a number keeps its exact source text, a
bool becomes `true`/`false`, a nested object or array is added as compact JSON, and
a `null` field is omitted. An entry whose value is not an object is a load error.

```json
{
  "0.0.0.0": {"env": "prod", "region": "us-west"},
  "1.1.1.1": {"env": "dev"}
}
```

The cache settings do not apply to a JSON file, for the same reason as CSV: the
index is already held in memory. Setting any of them alongside `json` logs a
warning at startup, and will become a configuration error in a future release.

### Redis Lookup Source
| Field          | Type     | Default | Description |
| ---            | ---      | ---     | --- |
| address        | string   | ` `     | Redis server address `host:port`. |
| username       | string   | ` `     | Optional username. |
| password       | string   | ` `     | Optional password. |
| db             | int      | `0`     | Redis database index. |
| tls            | bool     | `false` | Use TLS (TLS 1.2+) for the connection. |
| key_prefix     | string   | ` `     | Optional prefix joined to the lookup key with `:`. |
| dial_timeout   | duration | `2s`    | Bounds the initial TCP/TLS dial. Protects against slow DNS or unreachable servers. |
| lookup_timeout | duration | `5s`    | Bounds each Redis call (`HGETALL` + JSON `GET` fallback) so a slow server cannot stall the pipeline. |

The processor first tries `HGETALL` on the resolved key. If no fields are
returned, it falls back to `GET` and decodes the value as JSON `map[string]string`.

On startup, the lookup source performs a `PING` (bounded by `dial_timeout`). A
failed `PING` is logged at `warn` and the processor starts anyway: enrichment
is best-effort, so a Redis restart, network blip, or maintenance window that
coincides with a collector start must not take the telemetry pipeline offline.
Records pass through un-enriched until Redis is reachable; the client
reconnects on its own with no collector restart. Startup behaviour therefore
matches the API source, which never probes its endpoint at start.

While Redis (or an API endpoint) is down, the source logs one `warn` when it
stops answering and one `info` when it answers again. Individual failed
lookups stay at `debug`. After a failure the source is marked down for 5s and
lookups return immediately without touching the network; when the window
elapses a single lookup probes the backend and either clears the mark or opens
a new window. So an outage costs one failed dial (at most `lookup_timeout`)
per 5s rather than one per record, and recovery is noticed within 5s. Entries
already in the cache keep being served throughout.

### API Lookup Source
| Field            | Type              | Default | Description |
| ---              | ---               | ---     | --- |
| url              | string            | ` `     | URL template. `$fieldValue`, `${fieldValue}`, `$key`, or `${key}` are substituted with the URL-encoded lookup key. |
| method           | string            | `GET`   | HTTP method. |
| headers          | map[string]string | `nil`   | Request headers. |
| timeout          | duration          | `10s`   | Per-request HTTP timeout (single attempt). |
| lookup_timeout   | duration          | `5s`    | Overall bound for one `Lookup`, including all retry attempts and backoff sleeps. Prevents a chain of slow requests from exceeding `timeout * max_retries`. |
| max_retries      | int               | `3`     | Total attempts (initial + retries) for transient failures. Non-retryable HTTP statuses (`400`, `401`, `403`, `404`) abort immediately. |
| initial_delay    | duration          | `100ms` | Backoff before the first retry. |
| retry_multiplier | int               | `2`     | Multiplier applied to the backoff between retries. |
| response_mapping | map[string]string | `nil`   | Maps output field names to dotted JSON paths in the response (e.g. `host: data.hostname`). When unset, the top-level response object is flattened. |

Retry policy: `5xx`, `408`, and `429` responses are retried up to `max_retries`
times with exponential backoff. `4xx` responses other than `408`/`429` are
considered deterministic client errors and are not retried. Retry sleeps honor
the caller's context — cancellation aborts a pending retry promptly.

Response bodies are read through an `io.LimitReader` capped at 1 MiB to guard
against misbehaving or hostile endpoints. Bodies embedded in error strings are
further truncated to 256 bytes so a burst of failing lookups cannot bloat
logs.

### PuppetDB Lookup Source

Enriches telemetry with Puppet facts about the host it came from. The lookup
key (`field`) is matched against each node's certname. The same source works
with PuppetDB and OpenVoxDB, which share the `/pdb/query/v4` query API.

| Field                | Type              | Default  | Description |
| ---                  | ---               | ---      | --- |
| endpoint             | string            | ` `      | Base URL of the server, e.g. `https://puppetdb.example.com:8081`. |
| tls                  | object            | ` `      | Client TLS settings (`ca_file`, `cert_file`, `key_file`, `server_name_override`, ...). See [configtls](https://github.com/open-telemetry/opentelemetry-collector/blob/main/config/configtls/README.md). |
| timeout              | duration          | `30s`    | Per-request HTTP timeout. |
| facts                | map[string]string | ` `      | Required. Maps an inventory path to the attribute name it is written as. |
| query                | string            | ` `      | Optional AST query, as a JSON array, that limits which nodes are loaded. It is ANDed with the fact selection, e.g. `["=", "environment", "production"]`. |
| match_short_hostname | bool              | `false`  | Also match the first label of each certname, for telemetry whose host name has no domain. |
| page_size            | int               | `1000`   | Nodes requested per page. |
| max_nodes            | int               | `100000` | Upper bound on nodes held in memory. A larger inventory fails the load and the previous inventory stays in use. |

Each `facts` key is `environment`, or a path starting with `facts.` or
`trusted.` in PuppetDB dot notation, such as `facts.os.family` or
`trusted.extensions.pp_role`. Double-quote a path segment that contains a dot or
slash, such as `facts.mountpoints."/".filesystem`. Array indexes (`[0]`) are not
supported, because PuppetDB rejects them in this query. Strings are written as
is, numbers and booleans as their JSON text, and structured facts as compact
JSON. A fact that a node lacks is left out.

There is no default that copies every fact. Facts can hold sensitive data such
as cloud instance metadata or SSH host keys, so list only the facts you want
attached to telemetry.

The whole selected inventory is fetched on every `reload_interval` and held in
memory, so lookups never touch the network. A failed fetch is logged and the
previous inventory stays in use. Memory grows with node count and the number of
facts selected: about 500 bytes per node for five short facts, so roughly 50 MB
for 100,000 nodes, and up to twice that while a reload is being built. As with
CSV, the cache settings do not apply.

With `match_short_hostname`, a short name that more than one certname shares
(`web01.example.com` and `web01.example.net`) is not matched, and a warning
gives the count. Use full certnames for those hosts.

#### Access and the certificate allowlist

PuppetDB accepts remote queries only over HTTPS with a client certificate
signed by the Puppet CA. The certificate's name must also be listed in the
server's `certificate-allowlist` file. PuppetDB 9 turns the allowlist on by
default and, on new installs, disables the plain HTTP port. A collector on a
Puppet-managed host can usually present that host's agent certificate, which
lives under `/etc/puppetlabs/puppet/ssl/`. The Puppet administrator then adds
the host's certname to the allowlist. A rejected certificate fails the load
with a message that names the allowlist.

Puppet certificates usually carry the certname rather than an IP address. When
the endpoint uses an address or alias that the server certificate does not list,
set `tls.server_name_override` to the server's certname.

#### Supported versions

| Server               | Tested                                                     |
| ---                  | ---                                                        |
| PuppetDB 8           | Yes                                                        |
| OpenVoxDB 8          | Yes                                                        |
| OpenVoxDB 9          | Yes, with default settings and with PuppetDB 9's defaults  |
| PuppetDB 9           | Through OpenVoxDB 9, which serves the same query API       |
| PuppetDB 7 and older | No. These are end of life.                                 |

### Example: CSV
```yaml
receivers:
    otlp:
        protocols:
            grpc:
processors:
    lookup:
        csv: ./example.csv
        context: body
        field: ip
exporters:
    debug:
service:
    pipelines:
        logs:
            receivers: [otlp]
            processors: [lookup]
            exporters: [debug]
```

```csv
ip,host,region,env
0.0.0.0,host-1,us-west,prod
1.1.1.1,host-2,us-east,dev
```

### Example: JSON
```yaml
receivers:
    otlp:
        protocols:
            grpc:
processors:
    lookup:
        json: ./example.json
        context: body
        field: ip
exporters:
    debug:
service:
    pipelines:
        logs:
            receivers: [otlp]
            processors: [lookup]
            exporters: [debug]
```

```json
{
  "0.0.0.0": {"host": "host-1", "region": "us-west", "env": "prod"},
  "1.1.1.1": {"host": "host-2", "env": "dev"}
}
```

### Example: Redis (with shared storage cache)
```yaml
extensions:
    file_storage:
        directory: /var/lib/otelcol/lookup-cache
processors:
    lookup:
        context: attributes
        field: user_id
        cache_ttl: 10m
        storage: file_storage
        redis:
            address: redis.internal:6379
            key_prefix: user
            tls: true
service:
    extensions: [file_storage]
    pipelines:
        logs:
            receivers: [otlp]
            processors: [lookup]
            exporters: [debug]
```

### Example: API
```yaml
processors:
    lookup:
        context: resource.attributes
        field: host.name
        api:
            url: https://cmdb.internal/hosts/${fieldValue}
            method: GET
            headers:
                Authorization: Bearer ${env:CMDB_TOKEN}
            timeout: 2s
            lookup_timeout: 6s
            max_retries: 4
            initial_delay: 200ms
            retry_multiplier: 2
            response_mapping:
                team: data.owner.team
                env:  data.environment
```

### Example: Redis (with custom timeouts)
```yaml
processors:
    lookup:
        context: attributes
        field: user_id
        redis:
            address: redis.internal:6379
            key_prefix: user
            tls: true
            dial_timeout: 3s
            lookup_timeout: 2s
```

### Example: PuppetDB

```yaml
processors:
  lookup:
    context: resource.attributes
    field: host.name
    reload_interval: 15m
    puppetdb:
      endpoint: https://puppetdb.example.com:8081
      tls:
        ca_file: /etc/puppetlabs/puppet/ssl/certs/ca.pem
        cert_file: /etc/puppetlabs/puppet/ssl/certs/collector.example.com.pem
        key_file: /etc/puppetlabs/puppet/ssl/private_keys/collector.example.com.pem
      query: '["=", "environment", "production"]'
      match_short_hostname: true
      facts:
        facts.os.family: puppet.os.family
        trusted.extensions.pp_role: puppet.role
        environment: puppet.environment
```
