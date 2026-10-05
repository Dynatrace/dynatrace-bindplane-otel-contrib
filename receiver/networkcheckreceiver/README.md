# Network Check Receiver

Probes a list of network targets on every collection interval and emits
metrics, and log records for HTTP checks and traceroutes. Each target uses one
probe method:

- **ICMP** sends `ping_count` echo requests and reports round-trip time
  (min/avg/max) and packet loss.
- **HTTP** sends one request and reports whether a response arrived, its
  status code, and the time spent in each phase (DNS lookup, TCP connect, TLS
  handshake, request write, time to first byte, total).
- **DNS** sends one query to a specific DNS server and reports whether it
  answered with a record of the requested type, and how long it took.

Traceroute can be enabled for all targets. It runs every Nth probe of a
target, when an ICMP target's packet loss reaches a threshold, or both.

## Privileges

| Feature | Linux | macOS | Windows |
|---------|-------|-------|---------|
| ICMP ping | `CAP_NET_RAW`, or none when the collector's group ID is inside `net.ipv4.ping_group_range` | None | Administrator |
| UDP traceroute (default) | None | root | None (native API, see below) |
| ICMP traceroute | root or `CAP_NET_RAW`; without it every traced cycle fails with `operation not permitted` | root | None (native API, see below) |
| HTTP probe | None | None | None |
| DNS probe | None | None | None |

At startup the receiver tries a raw ICMP socket first and, if that is not
permitted, an unprivileged datagram ICMP socket. Raw sockets need root or
`CAP_NET_RAW` on Linux, root on macOS, and Administrator on Windows. Datagram
ICMP sockets work without privilege on macOS, and on Linux when the collector's
group ID is inside `net.ipv4.ping_group_range`; Windows has none.

If neither socket can be opened, the receiver logs one warning, `ICMP sockets
unavailable; ICMP targets will report packet_loss 1 until the collector can
open one`, followed by what to grant on Linux and Windows, and every ICMP
target reports `network.ping.packet_loss` = 1. ICMP targets are never probed
over HTTP instead.

### Linux and containers

`net.ipv4.ping_group_range` is a range of group IDs allowed to open datagram
ICMP sockets. The kernel default (`1 0`) allows no group. Many distributions
widen it through systemd; check with `sysctl net.ipv4.ping_group_range`.

Docker sets it to `0 2147483647` inside containers by default, so ICMP ping
works in a Docker container without extra capabilities. Whether a Kubernetes
pod allows it depends on the container runtime and its version, so set it on
the pod. `net.ipv4.ping_group_range` is a safe sysctl and is allowed without
kubelet configuration:

```yaml
spec:
  securityContext:
    sysctls:
      - name: net.ipv4.ping_group_range
        value: "0 2147483647"
```

That covers ICMP ping. The default UDP traceroute needs no privilege on
Linux. ICMP traceroute
(`traceroute.method: icmp`) needs a raw socket, which on Kubernetes means
running the collector as root with `NET_RAW`:

```yaml
containers:
  - name: collector
    securityContext:
      runAsUser: 0
      capabilities:
        add: ["NET_RAW"]
```

Kubernetes does not pass added capabilities to a process running as a non-root
user, so `capabilities.add` alone has no effect for a non-root collector.

### Windows

When the collector runs as a Windows service it runs as `LocalSystem`, which
has the privilege ICMP ping needs. Run from an unelevated shell, it does not,
and ICMP targets report packet loss 1.

Traceroute on Windows ignores `traceroute.method` and uses the IP Helper API
(`IcmpSendEcho`), the mechanism the built-in `tracert.exe` uses. Windows does
not deliver inbound ICMP time-exceeded messages to a raw socket, so the UDP
and ICMP methods cannot work there even with Administrator rights. The native
path needs no elevation.

The system resolver is read with `GetAdaptersAddresses`, the source `ipconfig`
and `Get-DnsClientServerAddress` read, so the `dns.server` attribute is
populated on Windows without configuring `dns_server`.

## Configuration

```yaml
receivers:
  networkcheck:
    # How often a probe cycle runs. Default 60s.
    collection_interval: 60s

    # How many targets are probed at the same time. 0 = default (16).
    # Maximum 256.
    max_concurrent_probes: 16

    # How many targets to probe per cycle, rotating through the list.
    # 0 (default) = every target every cycle. With 10 targets, batch_size 1
    # and a 1m interval, each target is probed once every 10 minutes.
    batch_size: 0

    # Delay each cycle by a random duration in [0, jitter). Spreads probes
    # when many collectors share one configuration. Must be less than
    # collection_interval. Default 0.
    jitter: 0s

    targets:
      - endpoint: "192.0.2.1"      # Bare hostname or IP address.
        method: icmp               # "icmp" (default), "http", or "dns".
        ping_count: 3              # Echo requests per probe, 1-100. Default 3.
        timeout: 5s                # Wait for replies after the last request. Default 5s.
        dns_server: ""             # Resolver for a hostname endpoint, e.g. "192.0.2.53:53".
                                   # Blank = system resolver.

      - endpoint: "https://example.com/health"  # URL. http:// is assumed without a scheme.
        method: http
        http_method: HEAD          # Default HEAD.
        timeout: 10s               # Limit for the whole request. Default 10s.
        # proxy_url: "http://proxy.example:3128"  # Default: HTTPS_PROXY, HTTP_PROXY, NO_PROXY.
        headers:
          X-Probe: networkcheck
        tls:
          insecure_skip_verify: false

      - endpoint: "192.0.2.53"     # DNS server to probe: host or host:port, port 53 if omitted.
        method: dns
        dns_query: "example.com"   # Name to query. Required.
        dns_record_type: A         # A (default), AAAA, CNAME, MX, or TXT.
        timeout: 5s                # Default 5s.

    traceroute:
      enabled: false               # Default false.
      method: udp                  # "udp" (default) or "icmp". Ignored on Windows.
      max_hops: 30                 # 1-255. Default 30.
      timeout: 3s                  # Wait for each probe's reply. Default 3s.
      probes_per_hop: 3            # 1-10. Default 3. Stops at the first reply.
      max_consecutive_timeouts: 5  # Give up after this many silent hops in a row.
                                   # 0 = never give up early. Default 5.

      # Run a traceroute every N probes of a target. 0 (default) = never.
      # Without batch_size, interval 10 with a 1m collection_interval traces
      # each target every 10 minutes.
      interval: 0

      # Run a traceroute when an ICMP target's packet loss is at or above
      # failure_threshold (0.0-1.0, default 0.5).
      on_failure: false
      failure_threshold: 0.5

    logs:
      include_tls_details: true    # Certificate and handshake detail in HTTPS records. Default true.
```

A target `timeout` must be at most 80% of `collection_interval`: a tenth of the
interval short of the 90% probe cycle budget, so a probe that starts at the top
of the cycle times out on its own and is reported as down before the cycle
deadline would skip it. When a target sets no timeout, the default (10 s for
HTTP, 5 s for ICMP and DNS) is clamped to the same ceiling, less the ICMP
packet pacing. `traceroute.timeout` must be at most `collection_interval`.
Configuration errors name the target by its index, for example `target[2]`.

### Targets

The endpoint must have the shape its method uses:

| Method | Endpoint | Rejected |
|--------|----------|----------|
| `icmp` | Hostname or IP address | Scheme, port, path, userinfo |
| `dns` | `host` or `host:port`; an IPv6 address with a port is bracketed: `[2001:db8::53]:53` | Scheme, path, userinfo |
| `http` | `http` or `https` URL; without a scheme `http://` is added | Other schemes, a URL without a host |

Targets embed the collector's standard HTTP client settings, but the probe
honours only `endpoint`, `timeout`, `tls`, `proxy_url` and `headers`. The
remaining keys are rejected with
`target[i]: <key> is not supported by networkcheck targets`:
`auth`, `compression`, `compression_params`, `cookies`, `middlewares`,
`force_attempt_http2`, `http2_read_idle_timeout`, `http2_ping_timeout`,
`keepalive`, `idle_conn_timeout`, `max_idle_conns`, `max_idle_conns_per_host`,
`disable_keep_alives`, `max_conns_per_host`, `read_buffer_size` and
`write_buffer_size`. The collector folds a `keepalive` section into the flat
keys it replaces, so the error names that key, for example `max_idle_conns`.

`tls`, `proxy_url`, `headers` and `http_method` apply to HTTP targets only.

### Probe cycle and deadline

Each collection interval runs one probe cycle over the active targets (all
targets, or the current batch when `batch_size` is set). Up to
`max_concurrent_probes` targets (default 16) are probed at the same time, and
at most 4 traceroutes are in flight at once.

A cycle has a budget of `collection_interval` minus 10%, measured from when
the cycle was requested, so the `jitter` delay counts against it. A target
that has not been probed when the budget runs out, or whose probe returns
after it, is skipped for that cycle: it emits no metrics, no log record and no
error, which leaves a gap in its series rather than reporting it down, and the
skipped probe does not count toward `traceroute.interval`. The probe order
rotates, so targets skipped at the end of one cycle are probed first in the
next.

When targets are skipped, the receiver logs `probe cycle hit its deadline;
some targets were skipped this cycle` with the number skipped, at most once
per minute.

Shutdown cancels the cycle in progress. It waits only as long as a running
probe takes to notice the cancellation.

### Sizing

A probe's worst case is the time it takes when nothing answers:

| Probe | Healthy | Worst case |
|-------|---------|------------|
| ICMP | (`ping_count` − 1) × 200 ms + RTT | (`ping_count` − 1) × 200 ms + `timeout` |
| HTTP | Request time | `timeout` |
| DNS | Query time | `timeout` |
| Traceroute | Sum of the hops' RTTs | Each silent hop costs `probes_per_hop` × `traceroute.timeout` |

When N targets each take time T, a cycle takes about
ceil(N / `max_concurrent_probes`) × T. Compare that with the budget: 54 s for
a 60 s interval. Examples with the default `max_concurrent_probes` of 16:

| Targets | T | Rounds | Cycle |
|---------|---|--------|-------|
| 100 ICMP, `ping_count` 3, all answering, 20 ms RTT | 0.42 s | 7 | about 3 s |
| 100 ICMP, `ping_count` 3, none answering, `timeout` 5s | 5.4 s | 7 | about 38 s |
| 30 HTTP, `timeout` 10s, all down | 10 s | 2 | about 20 s |
| 100 HTTP, `timeout` 10s, all down | 10 s | 7 | about 70 s: 80 targets probed, 20 skipped |

The last row fits with `max_concurrent_probes: 32` (4 rounds, 40 s) or a 5 s
`timeout` (7 rounds, 35 s).

A traceroute to a path that stops answering runs until
`max_consecutive_timeouts` silent hops in a row: 5 × 3 × 3 s = 45 s with the
defaults. With `max_consecutive_timeouts: 0` it walks to `max_hops` and is
stopped when the cycle budget runs out.

At startup the receiver estimates the worst-case cycle as
ceil(targets per cycle / `max_concurrent_probes`) × the slowest target's worst
case from the table above, plus, when traceroute is enabled,
ceil(targets per cycle / 4) × `probes_per_hop` × `traceroute.timeout` ×
`max_consecutive_timeouts` (`max_hops` when that is 0). When the estimate
exceeds the budget it logs `worst-case probe cycle exceeds the collection
interval; targets will be skipped on cycles where probes run to their
timeouts`, with the hint `raise max_concurrent_probes or collection_interval,
or lower per-target timeouts`. The estimate charges every round at the slowest
target's cost, so a configuration that mixes fast and slow targets can trigger
it without ever skipping a target.

### ICMP probes

The probe sends `ping_count` echo requests 200 ms apart and waits up to
`timeout` after the last one. When every reply arrives, it finishes as soon as
the last reply does; when any reply is missing, it waits the full `timeout`.

A hostname endpoint is resolved with `dns_server` when set, otherwise with the
system resolver. When the probe cannot run at all (the hostname does not
resolve, the socket cannot be opened, or the request cannot be sent), the
target reports `network.ping.packet_loss` = 1 and no latency, the same as a
target that does not answer.

### HTTP probes

Each probe sends one request on a new connection, so the DNS, connect and TLS
phases are measured every time. Redirects are not followed: the first
response is the one measured. HTTP/2 is used when an HTTPS server offers it
during the TLS handshake; otherwise HTTP/1.1.

Any response, including 4xx and 5xx, counts as up: `network.http.status` is 1
and the code is in the `http.response.status_code` attribute. Alert on the
status code attribute for application errors. Status 0 means no response:
DNS failure, refused connection, TLS failure, or timeout.

The response body is read and discarded, up to 1 MiB.

Phase timings:

| Phase | Measured from | To |
|-------|---------------|----|
| DNS lookup | Lookup start | Lookup done; 0 for an IP address endpoint |
| Connect | Start of the TCP dial | Connection established |
| TLS handshake | Handshake start | Handshake done; 0 for plain HTTP |
| Request write | Connection ready, after any TLS handshake | Request written |
| Response (time to first byte) | Request written | First response byte |

With a proxy configured, through `proxy_url` or the `HTTPS_PROXY`, `HTTP_PROXY`
and `NO_PROXY` environment variables (read once when the receiver starts), the
DNS and connect phases describe the connection to the proxy, `server.resolved_ip`
is omitted from log records, and for HTTPS the TLS phase is still the handshake
with the origin. A proxy that refuses the `CONNECT` reports `error.type` =
`request`. Loopback targets are never proxied.

Credentials in the endpoint's userinfo are sent as HTTP basic authentication
unless a configured `Authorization` header is present. A configured `Host`
header replaces the request's Host. HTTP/2 is always attempted, so
`force_attempt_http2` has no effect and is rejected like the other unsupported
keys when set.

### DNS probes

The probe sends one query for `dns_query` with exactly `dns_record_type` to the
server in `endpoint`, over UDP, and retries over TCP when the answer is
truncated. The name is queried as fully qualified: no search domains are
appended and `/etc/hosts` is not consulted.

`network.dns.status` is 1 only when the server answers `NOERROR` with at least
one record of the requested type. `NXDOMAIN`, any other response code, an
answer without a record of that type, or no answer before `timeout` is 0.
`network.dns.lookup_duration` is emitted only with status 1.

### Traceroute

Traceroute is IPv4 only. The destination (the host part of the endpoint) is
resolved with the target's resolver, `dns_server` or the system resolver, and
the first IPv4 address is traced; a destination with no IPv4 address fails
with an error. Each target's traceroute uses random probe identifiers, so
concurrent traces and other receivers on the host are unlikely to claim each
other's replies.

A hop that answers none of its probes within `traceroute.timeout` is recorded
with address `*`, `network.traceroute.hop.status` = 0, and no latency. After
`max_consecutive_timeouts` such hops in a row the trace is abandoned rather
than probing up to `max_hops`.

With `on_failure`, a target that stays down is not traced on every cycle: it
is traced on its first failing probe and then, while it stays down, by the
`interval` schedule when `interval` is set, or every 10 failing probes when it
is 0. A passing probe resets the count.

Traceroutes run inside the probe cycle and are stopped when its budget runs
out.

## Probes per hop

Routers rate-limit ICMP time-exceeded generation, so a single probe regularly
goes unanswered on a hop that is working. With one probe per hop, that reads
as a missing hop, and a run of them can trip `max_consecutive_timeouts` and
truncate the trace before it reaches the destination.

Classic traceroute and Windows `tracert` send three probes per hop by default
for this reason. This receiver sends up to `probes_per_hop` but stops at the
first reply, so a healthy path costs one probe per hop and only silent hops
are retried. Sending three probes to every hop on every cycle would worsen the
rate limiting that causes the gaps.

Each hop in a traceroute log record carries `probes`, and the record carries
`traceroute.hops_retried`. A hop with `probes` above 1 answered only after a
retry, which distinguishes a router that is rate-limiting from one that is
silent.

A hop is recorded from its first reply, so its latency is that probe's RTT,
not a best of N.

## Logs

The receiver emits log records as well as metrics. Which signals it produces
is decided by pipeline membership, not configuration: reference the receiver
from a logs pipeline and it emits logs, from a metrics pipeline and it emits
metrics, from both and it emits both.

Two probe types produce records:

- **Traceroute**: one record per trace, with the hops as an ordered array, so
  a route change is a difference between two records.
- **HTTP**: one record per request, with the phases together, so a slow check
  can be attributed to a phase. Separate metric series cannot be correlated
  back to one request.

DNS and ICMP probes emit no records; their results are metrics.

### Sharing one probe cycle

A receiver referenced from both pipelines is instantiated twice by the
collector. The two instances always share probe execution, even when a cycle
runs long, so each target is probed once per cycle and both signals describe
the same observation. Both use the same `collection_interval`.

### HTTP record

```jsonc
{
  "Timestamp": "<request start>",
  "SeverityText": "INFO",                  // ERROR when no response was received
  "Attributes": {
    "server.address": "https://www.cloudflare.com",
    "server.resolved_ip": "104.16.124.96",
    "http.request.method": "GET",
    "http.response.status_code": 200,
    "http.response.size": 1256,
    "network.protocol.version": "HTTP/2.0", // HTTP/1.1 unless the server offers HTTP/2
    "tls.cert.days_remaining": 61.4,
    "dns.server": "192.0.2.53"
  },
  "Body": {
    "phases": {
      "dns_ms": 2.1, "connect_ms": 11.4, "tls_ms": 18.7,
      "write_ms": 0.3, "ttfb_ms": 8.2, "total_ms": 41.2
    },
    "tls": {
      "version": "TLS 1.3",
      "cipher": "TLS_AES_128_GCM_SHA256",
      "negotiated_protocol": "h2",
      "cert": { "issuer": "...", "subject": "...", "not_after": "...", "days_remaining": 61.4 }
    }
  }
}
```

A failed request is `SeverityText: ERROR`, carries `error.type` with the phase
that failed (for example `dns`, `connect`, `tls` or `response`), and adds an
`error` block with that phase and the error message. Its `phases` contain only
the phases that completed, plus `total_ms`. A failed HTTP probe reports status code 0 whatever
the cause, so the record is what separates a DNS failure from a refused
connection or a timeout.

A response with a 4xx or 5xx status is a successful probe and is logged at
`INFO`.

### Traceroute record

```jsonc
{
  "Timestamp": "<trace start>",
  "SeverityText": "INFO",                  // WARN when the destination was not reached
  "Attributes": {
    "server.address": "www.cloudflare.com",
    "server.resolved_ip": "104.16.124.96",
    "traceroute.method": "udp",
    "traceroute.hop_count": 11,
    "traceroute.hops_answered": 8,
    "traceroute.hops_retried": 1,
    "traceroute.reached_dest": true,
    "traceroute.aborted_early": false
  },
  "Body": {
    "hops": [
      { "index": 1, "address": "172.16.1.1",    "timed_out": false, "probes": 1, "rtt_ms": 0.443 },
      { "index": 2, "address": "10.112.162.67", "timed_out": false, "probes": 2, "rtt_ms": 11.34 },
      { "index": 3, "address": "*",             "timed_out": true,  "probes": 3 }
    ]
  }
}
```

Hops that never answered are included with `timed_out: true` and the address
`*`, and carry no `rtt_ms`: the only duration available for them is the
configured timeout, which is not a measurement. `traceroute.aborted_early`
separates a path cut short by `max_consecutive_timeouts` from a short one.
`traceroute.method` is `native` on Windows.

### Security

Request and response headers and bodies are never recorded, and there is no
option to record them. Only the response size is captured.

All userinfo is removed from endpoints, username included, in the
`target.endpoint` resource attribute, the `server.address` log attribute and
error text: `https://user:pass@host/` and `https://token@host/` are both
reported as `https://host/`. Only HTTP endpoints can carry userinfo; ICMP and
DNS endpoints containing `@` are rejected. An `@` in the path, query or
fragment is not userinfo and is left as is. The path and query string are
reported as configured, so do not put secrets in them; send them in `headers`.

### Volume

Records scale with targets and interval, not with hops: one traceroute record
per trace rather than one per hop. At a 60 s interval an HTTP target produces
1,440 records per day. A target traced every `interval` probes adds
1,440 / `interval` traceroute records per day, plus the traces `on_failure`
triggers.

### Configuration

```yaml
receivers:
  networkcheck:
    logs:
      # Include certificate and handshake detail in HTTPS records. Default true.
      include_tls_details: true
```

### Example

```yaml
service:
  pipelines:
    metrics:
      receivers: [networkcheck]
      exporters: [otlphttp]
    logs:
      receivers: [networkcheck]
      exporters: [otlphttp]
```

## Metrics

Each data point is timestamped with the start of its target's probe.

### Failed and skipped probes

A probe that fails emits its outcome metric and no timings:

| Failure | Emitted | Not emitted |
|---------|---------|-------------|
| DNS: no answer with a record of the requested type, or no answer at all | `network.dns.status` = 0 | `network.dns.lookup_duration` |
| HTTP: no response (DNS, connect or TLS failure, timeout) | `network.http.status` = 0 | All six `network.http.*` durations |
| ICMP: no echo reply | `network.ping.packet_loss` = 1 | `network.ping.latency_min`/`avg`/`max` |
| ICMP: hostname did not resolve, socket or send failed, or ICMP unavailable | `network.ping.packet_loss` = 1 | `network.ping.latency_min`/`avg`/`max` |
| Traceroute hop did not answer | `network.traceroute.hop.status` = 0, `traceroute.hop.address` = `*` | `network.traceroute.hop.latency` for that hop |
| Traceroute could not run | Nothing; the reason is in the scrape error | All traceroute metrics for that trace |
| Target skipped when the cycle budget ran out | Nothing, not even an error | Everything for that target in that cycle |

A failed probe has no duration to report. The only figure available is how
long the receiver waited, so publishing it would write the configured timeout
into the latency series as though it were a measurement.

Alert on the status and packet loss metrics, not on durations. A failure
leaves a gap in the timing series rather than a fabricated value, so averages
and percentiles over them stay meaningful. A skipped target leaves a gap in
every series, including status.

A down HTTP target or total ICMP packet loss is data, not an error. Failures
that are errors, such as a traceroute that cannot run, are reported as one
scrape error per scrape and signal, in the form
`N of M targets failed: ping <endpoint>: <error>; ... (+K more)`. It names at
most three targets, and M counts only the targets probed in that cycle, not
skipped ones.

### DNS targets

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `network.dns.status` | Gauge | 1 | 1 = `NOERROR` with a record of the requested type, 0 otherwise |
| `network.dns.lookup_duration` | Gauge | ms | Time for the server to answer; only with status 1 |

Attributes: `dns.query`

### ICMP targets

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `network.ping.packet_loss` | Gauge | 1 | Fraction of echo requests without a reply (0.0-1.0) |
| `network.ping.latency_min` | Gauge | ms | Minimum RTT of the replies received |
| `network.ping.latency_avg` | Gauge | ms | Average RTT of the replies received |
| `network.ping.latency_max` | Gauge | ms | Maximum RTT of the replies received |

Attributes: `ping.method` (always `icmp`), `dns.server`

### HTTP targets

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `network.http.status` | Gauge | 1 | 1 = a response was received (any status code), 0 = no response |
| `network.http.duration` | Gauge | ms | Total request time |
| `network.http.dns_lookup_duration` | Gauge | ms | DNS lookup; 0 for an IP address endpoint |
| `network.http.client_connection_duration` | Gauge | ms | TCP dial start to connection established |
| `network.http.tls_handshake_duration` | Gauge | ms | TLS handshake; 0 for plain HTTP |
| `network.http.request_duration` | Gauge | ms | Request write, excluding the TLS handshake |
| `network.http.response_duration` | Gauge | ms | Request written to first response byte |

Attributes: `http.response.status_code` (on `network.http.status` and
`network.http.duration`), `dns.server`

### Traceroute

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `network.traceroute.hop.status` | Gauge | 1 | 1 = the hop answered one of its probes, 0 = it answered none |
| `network.traceroute.hop.latency` | Gauge | ms | RTT to a hop that answered |

Attributes: `traceroute.hop.index`, `traceroute.hop.address`, `dns.server`

`network.traceroute.hop.status` is a double so that averages are not
truncated: averaged over time, it gives the fraction of traces in which a hop
answered.

### The `dns.server` attribute

`dns.server` is the configured `dns_server` exactly as written, for example
`192.0.2.53:53`. Without `dns_server` it is the system resolver detected at
startup, written as a bare address without a port, for example `192.0.2.53`:
the first `nameserver` in `/etc/resolv.conf` on Linux and macOS, or the first
DNS server of an active network adapter on Windows (IPv4 preferred). It is
empty when no resolver could be detected.

### Resource attributes

Each target produces its own resource with `target.endpoint` set to the
configured `endpoint`, with any userinfo removed.

### Metric volume

Per target and cycle: an ICMP target emits up to 4 data points, an HTTP target
up to 7, a DNS target up to 2. A traceroute adds up to 2 data points per hop.
Traceroute series are keyed by hop index and hop address, so route changes and
load-balanced paths add series over time.
