# Network Check Receiver

One receiver for synthetic network checks. It runs the upstream
OpenTelemetry check receivers as sections of a single configuration and adds
traceroute, which upstream does not have:

| Section | Runs | Configuration |
|---------|------|---------------|
| `http` | [`http_check`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.161.0/receiver/httpcheckreceiver) | The `http_check` configuration |
| `icmp` | [`icmp_check`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.161.0/receiver/icmpcheckreceiver) | The `icmp_check` configuration |
| `dns` | [`dns_check`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.161.0/receiver/dnscheckreceiver) | The `dns_check` configuration |
| `tcp` | [`tcp_check`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.161.0/receiver/tcpcheckreceiver) | The `tcp_check` configuration |
| `traceroute` | This receiver | [Below](#traceroute) |

The `http`, `icmp`, `dns` and `tcp` sections are the upstream receivers'
configurations, verbatim, and their metrics are the upstream metrics, with
the upstream names (`httpcheck.*`, `ping.*`, `dnscheck.*`, `tcpcheck.*`) and
resource attributes. Both track the upstream version this receiver is built
against; see the linked READMEs.

Why a wrapper: Bindplane configures one source instead of four, and
traceroute can follow the checks. A host whose pings start failing is traced
right away, so the path at the moment of the failure is on record without
tracing every host on a schedule.

## Supported pipelines

- Metrics: every section.
- Logs: traceroute only, one record per trace. Without a `traceroute` section
  the receiver can still be placed in a logs pipeline; it emits nothing there.

## Configuration

```yaml
receivers:
  networkcheck:
    # Defaults for every section that does not set its own.
    collection_interval: 60s
    initial_delay: 1s

    # Each of the five sections is optional; at least one is required.
    http:
      targets:
        - endpoint: https://example.com
          method: GET
    icmp:
      targets:
        - host: example.com
          ping_count: 3
    dns:
      dns_servers:
        - endpoint: 192.0.2.53:53
      hostnames:
        - name: example.com
          record_type: A
    tcp:
      collection_interval: 30s     # A section's own setting wins.
      targets:
        - endpoint: example.com:443

    traceroute:
      # Traced on the schedule. host is a bare hostname or IP address;
      # dns_server is host or host:port and defaults to the system resolver.
      targets:
        - host: example.com
          dns_server: ""
      collection_interval: 5m      # Defaults to the top-level collection_interval.
      schedule: true               # false: trace only on failure.
      method: udp                  # udp or icmp. Ignored on Windows.
      max_hops: 30                 # 1-255.
      timeout: 3s                  # Per probe. At most collection_interval when targets are scheduled.
      probes_per_hop: 3            # 1-10. Stops at the first reply.
      max_consecutive_timeouts: 5  # Give up after this many silent hops in a row; 0 = never.
      max_concurrent_traces: 4     # 1-64, scheduled and triggered traces together.
      batch_size: 0                # Targets per cycle, rotating; 0 = all.
      jitter: 0s                   # Random delay per cycle, at most collection_interval / 2.
      on_failure:
        enabled: true              # Default true when an icmp section exists.
        loss_threshold: 50         # ping.loss.ratio (percent) at or above which a check fails.
        retrace_every: 10          # While a host keeps failing, trace every Nth failing check.
        max_hosts: 256             # Failing hosts tracked at once.
        timeout: 60s               # Bound on a triggered trace, including the wait for a slot.
```

The values shown for `traceroute` are the defaults.

### Inheritance

- `http`, `icmp`, `dns` and `tcp` start from the upstream receiver's own
  defaults, so the upstream default metrics stay enabled when a section sets
  only its targets. `collection_interval`, `initial_delay` and `timeout` (the
  scrape deadline) come from the top level unless the section sets them.
- `traceroute` takes `collection_interval` and `initial_delay` from the top
  level unless it sets them. Its `timeout` is the per-probe timeout, not a
  scrape deadline, and is not inherited.

### Validation

Each upstream section is validated by its own rules, and errors carry the
section's key, for example `http::targets::0: ...`. On top of those:

- At least one section is required.
- `traceroute` needs targets, or `on_failure` enabled. `on_failure` needs an
  `icmp` section with its `ping.loss.ratio` metric and `net.peer.name`
  resource attribute enabled.
- `http` targets must not carry credentials in the endpoint URL
  (`https://user:pass@host`): http_check reports the URL in the `http.url`
  attribute unchanged, so they would reach every data point. Use an auth
  extension or a `headers` entry on the target instead.
- Traceroute targets reject a scheme, port, path or userinfo in `host`.
  Bounds are as commented above; `loss_threshold` is 0-100,
  `retrace_every`, `max_hosts` and `timeout` must be positive.

### Telemetry IDs

Each section runs as a receiver of its own type, named after this receiver
and the section, for example `http_check/networkcheck/http` for
`networkcheck`, or `icmp_check/edge/icmp` for `networkcheck/edge`. That ID
labels the section's internal telemetry, for example
`otelcol_receiver_accepted_metric_points{receiver="icmp_check/networkcheck/icmp"}`.
Its log lines carry this receiver's ID and a logger named after the section
(`icmp`).

## Traceroute

Traceroute is IPv4 only. A host is resolved with the target's `dns_server`
or the system resolver and the first IPv4 address is traced.

A hop that answers none of its probes within `timeout` is recorded with
address `*`, `traceroute.hop.status` 0 and no latency. After
`max_consecutive_timeouts` such hops in a row the trace stops early instead
of probing up to `max_hops`.

Cloud networks hide intermediate hops: from an Azure VM every hop up to the
destination is silent (twelve of them to 1.1.1.1, the same as `tracert`
shows), so with the default `max_consecutive_timeouts: 5` a reachable
destination is reported as not reached and `aborted_early`. On such hosts
raise `max_consecutive_timeouts` (15, or 0 to disable the early abort) and
lower `timeout` and `probes_per_hop` to keep the trace inside the cycle
budget, for example `timeout: 1s`, `probes_per_hop: 1`,
`max_consecutive_timeouts: 15`.

### Traces on failure

The `icmp` section's metrics pass through the receiver on their way to the
pipeline, unchanged. For every host in them it reads `ping.loss.ratio`, which
icmp_check reports as a percentage (0-100), and the host as configured
(`net.peer.name`):

- A check with `ping.loss.ratio` at or above `loss_threshold` fails. A
  configured host that is missing from a batch also fails: icmp_check emits
  nothing for a host it could not resolve or open a socket for, and that is
  logged once per host rather than left looking like missing data.
- The first failing check traces the host. While it keeps failing, every
  `retrace_every`-th failing check traces it again: with the default 10, the
  1st, 11th, 21st, ... A passing check resets the count.
- The trace goes to the address icmp_check pinged (`net.peer.ip`), with the
  system resolver only when that address is unknown, so a check and the
  trace it triggers follow the same path even though the `icmp` section has
  no `dns_server` of its own. `dns_server` on a traceroute target applies to
  scheduled traces of that target.
- At most one trace per host is in flight; a trace that is due while the
  previous one runs is not started.
- `batch_size` and `jitter` apply to scheduled traces only. The upstream
  children probe all their targets each cycle and have no spreading of their
  own; to keep sections from firing at the same instant, give each its own
  `initial_delay`.
- Triggered and scheduled traces share `max_concurrent_traces`. A triggered
  trace waits for a slot and, with the wait, is bounded by
  `on_failure.timeout`. A trace that does not finish in time emits nothing and
  logs a warning. At startup the receiver warns when one trace into a path
  that stops answering (`probes_per_hop` × `timeout` ×
  `max_consecutive_timeouts`, or `max_hops` when that is 0) exceeds
  `on_failure.timeout`.
- At most `max_hosts` failing hosts are tracked; further failing hosts are not
  traced until one recovers, and a warning is logged once.

Trace slots are shared between scheduled and triggered traces and are not
assigned fairly under sustained contention: with more failing hosts than
`max_concurrent_traces` can serve inside `on_failure.timeout`, some hosts are
skipped (a rate-limited warning says so). Raise `max_concurrent_traces` or
`on_failure.timeout` if that happens.

Triggered traces are emitted as they finish, with `traceroute.trigger:
ping_failure`, as metrics and, when the receiver is also in a logs pipeline,
as a log record. Scheduled traces carry `traceroute.trigger: scheduled`.

An empty `traceroute:` next to an `icmp` section is enough: no scheduled
targets, failure-triggered traces with the defaults. The `icmp` section runs
only when the receiver is in a metrics pipeline, so triggered traces need
one; their log records also go to the logs pipeline when there is one.

icmp_check reports nothing for a host it could not ping at all, for example
a name that does not resolve or a socket it cannot open (it logs `failed to
ping host` instead), so such a host triggers no trace.

### Scheduled traces and their budget

Each `traceroute.collection_interval` runs one cycle over the targets (or the
current batch when `batch_size` is set). A cycle has a budget of 90% of the
interval, measured from when it was requested, so the `jitter` delay counts
against it. A target not traced when the budget runs out, or whose trace
returns after it, is skipped for that cycle: it emits nothing, not even
`traceroute.reached` 0, leaving a gap rather than reporting the host down.
The trace order rotates, so a target skipped at the end of one cycle goes
first in the next. Skips log `trace cycle hit its deadline; some targets were
skipped this cycle`, at most once a minute.

A trace to a path that stops answering costs `probes_per_hop` × `timeout` per
silent hop, up to `max_consecutive_timeouts` hops: 5 × 3 × 3 s = 45 s with the
defaults. At startup the receiver estimates the worst-case cycle as
ceil(targets per cycle / `max_concurrent_traces`) × that cost + `jitter`, and
logs `worst-case trace cycle exceeds the collection interval` when it exceeds
the budget. Triggered traces occupy the same slots, so while hosts are
failing a cycle can skip targets the estimate says fit.

A receiver in both a metrics and a logs pipeline traces each target once per
cycle; both signals describe the same trace.

Shutdown cancels traces in flight, scheduled and triggered, and waits only
as long as a probe takes to notice. Shutdown also waits for an icmp round already in flight in the
upstream child, up to its `ping_timeout` (5 s by default).

## Privileges

| Check | Linux | macOS | Windows |
|-------|-------|-------|---------|
| `icmp` (icmp_check) | The collector's group ID inside `net.ipv4.ping_group_range`; root is not exempt | None | None |
| UDP traceroute (default) | None | root | None (native API) |
| ICMP traceroute | root or `CAP_NET_RAW` | root | None (native API) |
| `http`, `dns`, `tcp` | None | None | None |

- icmp_check uses unprivileged datagram ICMP sockets on Linux and macOS and
  never falls back to a raw socket. On Linux the kernel allows them only to
  groups in `net.ipv4.ping_group_range`; the kernel default `1 0` allows
  none, many distributions widen it, and Docker sets `0 2147483647` inside
  containers. On Windows it uses a raw ICMP socket, which a standard user can
  open.
- UDP traceroute on Linux reads each probe's ICMP error from the probe
  socket's own error queue (`IP_RECVERR`), the mechanism `tracepath` uses, so
  it needs no privilege.
- Windows ignores `method` and traces with the IP Helper API
  (`IcmpSendEcho`), as `tracert.exe` does: Windows does not deliver inbound
  ICMP time-exceeded messages to raw sockets. `traceroute.method` in log
  records is `native` there.

### Kubernetes

Set the sysctl for the `icmp` section on the pod. It is a safe sysctl and
needs no kubelet configuration:

```yaml
spec:
  securityContext:
    sysctls:
      - name: net.ipv4.ping_group_range
        value: "0 2147483647"
```

UDP traceroute needs nothing. ICMP traceroute needs root with `NET_RAW`;
Kubernetes does not pass added capabilities to a non-root process:

```yaml
containers:
  - name: collector
    securityContext:
      runAsUser: 0
      capabilities:
        add: ["NET_RAW"]
```

## Metrics

Only the traceroute metrics are listed here; for the others see each
upstream receiver's `documentation.md`. Each data point is timestamped with
the start of its trace. All traceroute metrics can be disabled under
`traceroute::metrics`, as with any mdatagen receiver; see
[documentation.md](documentation.md).

| Metric | Type | Unit | Description |
|--------|------|------|-------------|
| `traceroute.reached` | Gauge (int) | 1 | 1 if the destination answered, 0 if not or the trace could not run |
| `traceroute.hops` | Gauge (int) | {hop} | Hops probed; 0 when the trace could not run |
| `traceroute.hop.status` | Gauge (double) | 1 | 1 if the hop answered one of its probes, 0 if none |
| `traceroute.hop.latency` | Gauge (double) | ms | RTT of the hop's first reply; not emitted for silent hops |

| Attribute | On | Value |
|-----------|----|-------|
| `traceroute.trigger` | All | `scheduled` or `ping_failure` |
| `dns.server` | All | The target's `dns_server`; empty for the system resolver and for triggered traces |
| `traceroute.hop.index` | Hop metrics | The hop's TTL |
| `traceroute.hop.address` | Hop metrics | The hop's address, `*` when it did not answer |

Resource attribute: `server.address`, the host as configured in
`traceroute::targets`, or in the `icmp` section for a triggered trace.

A trace that could not run (for example the host has no IPv4 address)
emits `traceroute.reached` 0 and `traceroute.hops` 0 and no hop metrics. For
scheduled traces the reason is in one scrape error per cycle,
`N of M targets failed: traceroute <host>: <error>`; for triggered traces it
is a warning in the collector log.

## Log record

One record per completed trace, with the hops in order:

```jsonc
{
  "Timestamp": "<trace start>",
  "SeverityText": "INFO",                  // WARN when the destination was not reached
  "Resource": { "server.address": "example.com" },
  "Attributes": {
    "server.address": "example.com",
    "server.resolved_ip": "93.184.215.14",
    "traceroute.trigger": "scheduled",     // ping_failure for a triggered trace
    "traceroute.method": "udp",            // native on Windows
    "traceroute.hop_count": 11,
    "traceroute.hops_answered": 8,
    "traceroute.hops_retried": 1,   // hops that answered only after a retry
    "traceroute.reached_dest": true,
    "traceroute.aborted_early": false,
    "dns.server": "192.0.2.53"             // only when the target sets dns_server
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

A hop that answers with an ICMP destination unreachable other than the
destination's own port unreachable (a router reporting the host or network
unreachable or administratively prohibited, or this host for an address with
no route) ends the trace: the record carries `traceroute.unreachable: true`,
`reached_dest` is false and `traceroute.reached` is 0.

Hops that never answered have `timed_out: true`, address `*` and no
`rtt_ms`. `probes` above 1 means the hop answered only after a retry, which
tells a rate-limiting router from a silent one; `traceroute.hops_retried`
counts those hops, not the silent ones. `traceroute.aborted_early`
tells a path cut short by `max_consecutive_timeouts` from a short one. A
trace that could not run produces no record.
