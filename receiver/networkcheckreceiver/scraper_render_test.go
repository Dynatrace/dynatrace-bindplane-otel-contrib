// Copyright Dynatrace LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package networkcheckreceiver

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.opentelemetry.io/collector/scraper/scrapererror"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver/internal/metadata"
)

// renderBoth renders one cycle through both signals. The scrapers are built
// without a prober: render only reads the cycle it is handed.
func renderBoth(t *testing.T, cfg *Config, results ...targetResult) (pmetric.Metrics, error, plog.Logs, error) {
	t.Helper()
	set := receivertest.NewNopSettings(metadata.Type)
	ms := &networkCheckScraper{
		cfg: cfg, settings: set,
		mb: metadata.NewMetricsBuilder(cfg.MetricsBuilderConfig, set),
		rb: metadata.NewResourceBuilder(cfg.MetricsBuilderConfig.ResourceAttributes),
	}
	ls := &networkCheckLogsScraper{
		cfg: cfg, settings: set,
		lb: metadata.NewLogsBuilder(set),
		rb: metadata.NewResourceBuilder(cfg.MetricsBuilderConfig.ResourceAttributes),
	}
	// A cycle-wide timestamp well away from every probe start, so a data
	// point stamped with it is caught.
	cycle := &probeCycle{at: time.Unix(1, 0), requestedAt: time.Unix(1, 0), results: results}
	md, merr := ms.render(cycle)
	ld, lerr := ls.render(cycle)
	return md, merr, ld, lerr
}

func cycleTarget(endpoint, method string) *targetState {
	tc := TargetConfig{Method: method}
	tc.Endpoint = endpoint
	return &targetState{cfg: tc, dnsServer: "9.9.9.9"}
}

type metricPoint struct {
	name     string
	val      float64
	attrs    map[string]any
	endpoint string
	ts       time.Time
}

func flattenMetrics(md pmetric.Metrics) []metricPoint {
	var out []metricPoint
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		rm := md.ResourceMetrics().At(i)
		ep, _ := rm.Resource().Attributes().Get("target.endpoint")
		for j := 0; j < rm.ScopeMetrics().Len(); j++ {
			ms := rm.ScopeMetrics().At(j).Metrics()
			for k := 0; k < ms.Len(); k++ {
				m := ms.At(k)
				dps := m.Gauge().DataPoints()
				for l := 0; l < dps.Len(); l++ {
					dp := dps.At(l)
					v := dp.DoubleValue()
					if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
						v = float64(dp.IntValue())
					}
					out = append(out, metricPoint{m.Name(), v, dp.Attributes().AsRaw(), ep.Str(), dp.Timestamp().AsTime()})
				}
			}
		}
	}
	return out
}

// metricsByEndpoint groups metric names and values per target resource.
func metricsByEndpoint(pts []metricPoint) map[string]map[string]float64 {
	out := map[string]map[string]float64{}
	for _, p := range pts {
		if out[p.endpoint] == nil {
			out[p.endpoint] = map[string]float64{}
		}
		out[p.endpoint][p.name] = p.val
	}
	return out
}

// Data points used to carry the cycle's start time, so every target after the
// first was stamped with a moment before its own probe began.
func TestRender_PerTargetTimestamps(t *testing.T) {
	t0 := time.Now().Truncate(time.Millisecond)
	t1 := t0.Add(7 * time.Second)
	trace := TraceResult{Reached: true, Hops: []HopResult{
		{Index: 1, Address: "10.0.0.1", RTT: time.Millisecond},
		{Index: 2, Address: unansweredHopAddress, TimedOut: true},
	}}

	md, merr, ld, lerr := renderBoth(t, createDefaultConfig().(*Config),
		targetResult{target: cycleTarget("https://a.example", MethodHTTP), startedAt: t0,
			ping: PingResult{Method: MethodHTTP, StatusCode: 200, TotalDuration: time.Millisecond}},
		targetResult{target: cycleTarget("b.example", MethodICMP), startedAt: t1,
			ping: PingResult{Method: MethodICMP, PacketLoss: 0}, traced: true, trace: trace},
	)
	require.NoError(t, merr)
	require.NoError(t, lerr)

	want := map[string]time.Time{"https://a.example": t0, "b.example": t1}
	pts := flattenMetrics(md)
	require.NotEmpty(t, pts)
	for _, p := range pts {
		require.True(t, want[p.endpoint].Equal(p.ts),
			"%s for %s stamped %v, want its probe start %v", p.name, p.endpoint, p.ts, want[p.endpoint])
	}

	require.Equal(t, 2, ld.LogRecordCount())
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		ep, _ := rl.Resource().Attributes().Get("target.endpoint")
		lr := rl.ScopeLogs().At(0).LogRecords().At(0)
		require.True(t, want[ep.Str()].Equal(lr.Timestamp().AsTime()))
	}
}

// A skipped probe never ran. Reporting it as a failure, or emitting a status
// for it, would turn a busy cycle into a false outage.
func TestRender_SkippedEmitsNothing(t *testing.T) {
	md, merr, ld, lerr := renderBoth(t, createDefaultConfig().(*Config),
		targetResult{target: cycleTarget("https://skipped.example", MethodHTTP), skipped: true},
		targetResult{target: cycleTarget("late.example", MethodICMP), skipped: true,
			pingErr: context.DeadlineExceeded, startedAt: time.Now()},
		targetResult{target: cycleTarget("ok.example", MethodDNS), startedAt: time.Now(),
			ping: PingResult{Method: MethodDNS, QuerySuccess: true, QueryName: "q.example"}},
	)
	require.NoError(t, merr, "skipped targets are not failures")
	require.NoError(t, lerr)

	got := metricsByEndpoint(flattenMetrics(md))
	require.Len(t, got, 1, "only the target that ran emits")
	require.Contains(t, got, "ok.example")
	require.Zero(t, ld.LogRecordCount())
}

// One error per failing target was logged at ERROR every interval, per
// signal. The cycle now reports a single partial error naming the first three.
func TestRender_FailureSummary(t *testing.T) {
	cases := []struct {
		name     string
		failing  int
		passing  int
		wantText string
	}{
		{
			name: "one failure", failing: 1, passing: 1,
			wantText: "1 of 2 targets failed: ping https://t0.example: no route to t0",
		},
		{
			name: "three failures", failing: 3, passing: 1,
			wantText: "3 of 4 targets failed: ping https://t0.example: no route to t0; " +
				"ping https://t1.example: no route to t1; traceroute https://t2.example: no route to t2",
		},
		{
			name: "five failures", failing: 5, passing: 0,
			wantText: "5 of 5 targets failed: ping https://t0.example: no route to t0; " +
				"ping https://t1.example: no route to t1; traceroute https://t2.example: no route to t2 (+2 more)",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var results []targetResult
			for i := 0; i < tc.failing; i++ {
				// Credentials in both the endpoint and the error text, which
				// the summary must redact per target.
				res := targetResult{
					target:    cycleTarget(fmt.Sprintf("https://user:pw@t%d.example", i), MethodHTTP),
					startedAt: time.Now(),
				}
				cause := fmt.Errorf("no route to user:pw@t%d", i)
				if i == 2 {
					// A traceroute failure still emits the ping data.
					res.ping = PingResult{Method: MethodHTTP, StatusCode: 200}
					res.traced, res.traceErr = true, cause
				} else {
					res.pingErr = cause
				}
				results = append(results, res)
			}
			for i := 0; i < tc.passing; i++ {
				results = append(results, targetResult{
					target:    cycleTarget(fmt.Sprintf("ok%d.example", i), MethodICMP),
					startedAt: time.Now(),
					ping:      PingResult{Method: MethodICMP},
				})
			}

			_, merr, _, lerr := renderBoth(t, createDefaultConfig().(*Config), results...)
			for _, err := range []error{merr, lerr} {
				require.Error(t, err)
				require.Equal(t, tc.wantText, err.Error())
				require.True(t, scrapererror.IsPartialScrapeError(err), "data must still be emitted")
				var pe scrapererror.PartialScrapeError
				require.ErrorAs(t, err, &pe)
				require.Equal(t, tc.failing, pe.Failed)
			}
		})
	}
}

// The record builders redacted the endpoint they embed, but the resource
// attribute carried the raw one, which put the credential straight back into
// every emitted record one level up.
//
// This used to grep scraper.go and logs_scraper.go for the unredacted call. It
// now renders both signals and checks the serialized payload, so no field
// anywhere can carry the credential, whatever the call site looks like.
func TestResourceEndpointIsRedacted(t *testing.T) {
	trace := TraceResult{Reached: true, Hops: []HopResult{{Index: 1, Address: "10.0.0.1", RTT: time.Millisecond}}}
	md, merr, ld, lerr := renderBoth(t, createDefaultConfig().(*Config),
		targetResult{target: cycleTarget("https://admin:pw7@example.com/health", MethodHTTP), startedAt: time.Now(),
			ping: PingResult{Method: MethodHTTP, StatusCode: 0, ErrPhase: "connect",
				ErrMessage: `Head "https://admin:pw7@example.com/health": connection refused`},
			traced: true, trace: trace},
		targetResult{target: cycleTarget("https://pw7TOKEN@token.example", MethodHTTP), startedAt: time.Now(),
			ping: PingResult{Method: MethodHTTP, StatusCode: 200}},
		targetResult{target: cycleTarget("admin:pw7@10.0.0.9", MethodICMP), startedAt: time.Now(),
			ping: PingResult{Method: MethodICMP, PacketLoss: 1, ErrMessage: "lookup admin:pw7@10.0.0.9: refused"}},
	)
	require.NoError(t, merr)
	require.NoError(t, lerr)

	got := metricsByEndpoint(flattenMetrics(md))
	require.Contains(t, got, "https://example.com/health")
	require.Contains(t, got, "https://token.example")
	require.Contains(t, got, "10.0.0.9")

	mj, err := (&pmetric.JSONMarshaler{}).MarshalMetrics(md)
	require.NoError(t, err)
	require.NotContains(t, string(mj), "pw7")

	lj, err := (&plog.JSONMarshaler{}).MarshalLogs(ld)
	require.NoError(t, err)
	require.NotContains(t, string(lj), "pw7")
	require.Contains(t, string(lj), "https://example.com/health")
}

// The shape each failure mode produces in metrics. A failed probe publishes its
// status and nothing else; timings for phases that never ran would read as
// measurements.
func TestRender_MetricsShape(t *testing.T) {
	now := time.Now()
	md, merr, _, _ := renderBoth(t, createDefaultConfig().(*Config),
		targetResult{target: cycleTarget("dns.example", MethodDNS), startedAt: now,
			ping: PingResult{Method: MethodDNS, QueryName: "q.example", QueryDuration: 5 * time.Second,
				ErrMessage: "i/o timeout"}},
		// Agent-side failure detail: some phases completed before the break.
		targetResult{target: cycleTarget("https://down.example", MethodHTTP), startedAt: now,
			ping: PingResult{Method: MethodHTTP, StatusCode: 0, DNSLookup: 3 * time.Millisecond,
				TotalDuration: 10 * time.Second, ErrMessage: "connection refused", ErrPhase: "connect"}},
		targetResult{target: cycleTarget("192.0.2.1", MethodICMP), startedAt: now,
			ping:   PingResult{Method: MethodICMP, PacketLoss: 1, ErrMessage: "100% loss"},
			traced: true,
			trace: TraceResult{Hops: []HopResult{
				{Index: 1, Address: "10.0.0.1", RTT: 2 * time.Millisecond},
				{Index: 2, Address: unansweredHopAddress, RTT: 3 * time.Second, TimedOut: true},
			}}},
	)
	require.NoError(t, merr, "failed measurements are data, not scrape errors")

	pts := flattenMetrics(md)
	got := metricsByEndpoint(pts)
	require.Equal(t, map[string]float64{"network.dns.status": 0}, got["dns.example"])
	require.Equal(t, map[string]float64{"network.http.status": 0}, got["https://down.example"])
	for _, p := range pts {
		if p.name == "network.http.status" {
			require.EqualValues(t, 0, p.attrs["http.response.status_code"])
		}
	}

	// ICMP total loss: packet_loss plus the hop series, no ping latencies.
	var lossSeen bool
	hopStatus := map[int64]float64{}
	hopLatency := map[int64]float64{}
	for _, p := range pts {
		if p.endpoint != "192.0.2.1" {
			continue
		}
		idx, _ := p.attrs["traceroute.hop.index"].(int64)
		switch p.name {
		case "network.ping.packet_loss":
			lossSeen = true
			require.Equal(t, float64(1), p.val)
		case "network.traceroute.hop.status":
			hopStatus[idx] = p.val
		case "network.traceroute.hop.latency":
			hopLatency[idx] = p.val
		default:
			t.Fatalf("unexpected metric %s for a fully lost ping", p.name)
		}
	}
	require.True(t, lossSeen)
	require.Equal(t, map[int64]float64{1: 1, 2: 0}, hopStatus, "a timed-out hop reports status 0")
	require.Equal(t, map[int64]float64{1: 2}, hopLatency, "a timed-out hop has no latency")
}

// The shape each probe outcome produces in logs. Only HTTP probes and
// traceroutes produce records.
func TestRender_LogsShape(t *testing.T) {
	now := time.Now()
	tlsd := &TLSDetails{Version: "TLS 1.3", CertNotAfter: now.Add(time.Hour), CertDaysLeft: 0.04}
	hop := []HopResult{{Index: 1, Address: "10.0.0.1", RTT: time.Millisecond}}
	silent := []HopResult{{Index: 1, Address: unansweredHopAddress, TimedOut: true}}

	_, _, ld, lerr := renderBoth(t, createDefaultConfig().(*Config),
		targetResult{target: cycleTarget("https://ok.example", MethodHTTP), startedAt: now,
			ping: PingResult{Method: MethodHTTP, StatusCode: 200, TLS: tlsd}},
		targetResult{target: cycleTarget("https://five.example", MethodHTTP), startedAt: now,
			ping: PingResult{Method: MethodHTTP, StatusCode: 503}},
		targetResult{target: cycleTarget("https://down.example", MethodHTTP), startedAt: now,
			ping: PingResult{Method: MethodHTTP, ErrPhase: "connect", ErrMessage: "refused"}},
		targetResult{target: cycleTarget("r.example", MethodICMP), startedAt: now,
			ping: PingResult{Method: MethodICMP}, traced: true, trace: TraceResult{Reached: true, Hops: hop}},
		targetResult{target: cycleTarget("a.example", MethodICMP), startedAt: now,
			ping: PingResult{Method: MethodICMP}, traced: true, trace: TraceResult{AbortedEarly: true, Hops: silent}},
		targetResult{target: cycleTarget("u.example", MethodICMP), startedAt: now,
			ping: PingResult{Method: MethodICMP}, traced: true, trace: TraceResult{MaxHops: 1, Hops: hop}},
		targetResult{target: cycleTarget("d.example", MethodDNS), startedAt: now,
			ping: PingResult{Method: MethodDNS, QuerySuccess: false, ErrMessage: "timeout"}},
		targetResult{target: cycleTarget("i.example", MethodICMP), startedAt: now,
			ping: PingResult{Method: MethodICMP, PacketLoss: 1, ErrMessage: "100% loss"}},
		targetResult{target: cycleTarget("z.example", MethodICMP), startedAt: now,
			ping: PingResult{Method: MethodICMP}, traced: true},
		targetResult{target: cycleTarget("t.example", MethodICMP), startedAt: now,
			ping: PingResult{Method: MethodICMP}, traced: true, traceErr: errors.New("operation not permitted")},
	)
	require.EqualError(t, lerr, "1 of 10 targets failed: traceroute t.example: operation not permitted")

	type shape struct {
		severity string
		status   any
		errType  any
		hasTLS   bool
	}
	got := map[string]shape{}
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		ep, _ := rl.Resource().Attributes().Get("target.endpoint")
		lrs := rl.ScopeLogs().At(0).LogRecords()
		require.Equal(t, 1, lrs.Len(), "one record per target here")
		lr := lrs.At(0)
		attrs := lr.Attributes().AsRaw()
		_, hasTLS := lr.Body().Map().Get("tls")
		got[ep.Str()] = shape{lr.SeverityText(), attrs["http.response.status_code"], attrs["error.type"], hasTLS}
	}
	require.Equal(t, map[string]shape{
		"https://ok.example": {"INFO", int64(200), nil, true},
		// A 5xx is an answer: the endpoint is up and the record is INFO.
		"https://five.example": {"INFO", int64(503), nil, false},
		"https://down.example": {"ERROR", nil, "connect", false},
		"r.example":            {"INFO", nil, nil, false},
		"a.example":            {"WARN", nil, nil, false},
		"u.example":            {"WARN", nil, nil, false},
	}, got, "DNS, ICMP and zero-hop traces emit no records")
}
