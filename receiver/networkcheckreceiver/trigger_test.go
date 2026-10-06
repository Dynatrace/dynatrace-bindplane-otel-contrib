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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver/internal/metadata"
)

var errFake = errors.New("traceroute supports IPv4 destinations only")

// ping is one icmp_check result: the host as configured, the address it
// pinged and the loss percentage.
type ping struct {
	host, ip string
	loss     float64
}

// pingBatch builds a batch shaped like icmp_check's: one resource per host
// with net.peer.name and net.peer.ip, and ping.loss.ratio among its metrics.
func pingBatch(pings ...ping) pmetric.Metrics {
	md := pmetric.NewMetrics()
	for _, p := range pings {
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr(peerNameAttr, p.host)
		rm.Resource().Attributes().PutStr(peerIPAttr, p.ip)
		ms := rm.ScopeMetrics().AppendEmpty().Metrics()
		rtt := ms.AppendEmpty()
		rtt.SetName("ping.rtt.avg")
		rtt.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(1.5)
		loss := ms.AppendEmpty()
		loss.SetName(pingLossMetric)
		loss.SetUnit("%")
		loss.SetEmptyGauge().DataPoints().AppendEmpty().SetDoubleValue(p.loss)
	}
	return md
}

// fakeTracer records the destinations traced. A non-nil block holds every
// trace until it is closed or the trace's context ends.
type fakeTracer struct {
	mu    sync.Mutex
	dests []string
	block chan struct{}
	res   TraceResult
	err   error
}

func (f *fakeTracer) newTrace(dest string) func(context.Context) (TraceResult, error) {
	return func(ctx context.Context) (TraceResult, error) {
		f.mu.Lock()
		f.dests = append(f.dests, dest)
		block := f.block
		f.mu.Unlock()
		if block != nil {
			select {
			case <-block:
			case <-ctx.Done():
			}
		}
		return f.res, f.err
	}
}

func (f *fakeTracer) traced() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.dests...)
}

type triggerFixture struct {
	trig    *triggerConsumer
	tracer  *fakeTracer
	metrics *consumertest.MetricsSink
	logs    *consumertest.LogsSink
	prober  *sharedProber
	warns   *observer.ObservedLogs
}

func newTriggerFixture(t *testing.T, mutate func(*TracerouteConfig)) *triggerFixture {
	t.Helper()
	cfg := defaultTracerouteConfig()
	cfg.CollectionInterval = time.Minute
	cfg.OnFailure.Enabled = true
	if mutate != nil {
		mutate(cfg)
	}
	core, warns := observer.New(zap.WarnLevel)
	set := receivertest.NewNopSettings(metadata.Type)
	set.Logger = zap.New(core)

	f := &triggerFixture{
		tracer:  &fakeTracer{res: TraceResult{Method: "udp", Hops: []HopResult{{Index: 1, Address: unansweredHopAddress, TimedOut: true, Probes: 3}}}},
		metrics: new(consumertest.MetricsSink),
		logs:    new(consumertest.LogsSink),
		warns:   warns,
	}
	f.prober = newSharedProber(cfg, set.Logger)
	f.trig = newTriggerConsumer(f.metrics, f.prober, cfg, set)
	f.trig.newTrace = f.tracer.newTrace
	t.Cleanup(func() {
		f.prober.stop()
		f.trig.close()
	})
	return f
}

// check delivers one batch and waits for the traces it started.
func (f *triggerFixture) check(t *testing.T, pings ...ping) {
	t.Helper()
	require.NoError(t, f.trig.ConsumeMetrics(context.Background(), pingBatch(pings...)))
	f.trig.wg.Wait()
}

func TestTrigger_ForwardsBatchUnchanged(t *testing.T) {
	f := newTriggerFixture(t, nil)
	in := pingBatch(ping{"a.test", "192.0.2.1", 100}, ping{"b.test", "192.0.2.2", 0})
	want := pmetric.NewMetrics()
	in.CopyTo(want)

	require.NoError(t, f.trig.ConsumeMetrics(context.Background(), in))
	f.trig.wg.Wait()

	got := f.metrics.AllMetrics()
	require.NotEmpty(t, got)
	require.Equal(t, want, got[0], "the icmp batch reaches the pipeline as it was")
}

func TestTrigger_Threshold(t *testing.T) {
	for _, tc := range []struct {
		name      string
		threshold float64
		loss      float64
		traced    bool
	}{
		{"below", 50, 49.9, false},
		{"at", 50, 50, true},
		{"above", 50, 100, true},
		{"zero threshold traces every check", 0, 0, true},
		{"full threshold needs total loss", 100, 66.7, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTriggerFixture(t, func(c *TracerouteConfig) { c.OnFailure.LossThreshold = tc.threshold })
			f.check(t, ping{"a.test", "192.0.2.1", tc.loss})
			require.Equal(t, tc.traced, len(f.tracer.traced()) == 1)
		})
	}
}

func TestTrigger_StreakAndRetraceEvery(t *testing.T) {
	f := newTriggerFixture(t, func(c *TracerouteConfig) { c.OnFailure.RetraceEvery = 3 })
	var tracedAt []int
	for i := 1; i <= 8; i++ {
		before := len(f.tracer.traced())
		f.check(t, ping{"a.test", "192.0.2.1", 100})
		if len(f.tracer.traced()) > before {
			tracedAt = append(tracedAt, i)
		}
	}
	require.Equal(t, []int{1, 4, 7}, tracedAt, "the first failing check, then every 3rd")
}

func TestTrigger_PassResetsStreak(t *testing.T) {
	f := newTriggerFixture(t, func(c *TracerouteConfig) { c.OnFailure.RetraceEvery = 10 })
	f.check(t, ping{"a.test", "192.0.2.1", 100}) // traced
	f.check(t, ping{"a.test", "192.0.2.1", 100}) // 2nd failing check: not due
	require.Len(t, f.tracer.traced(), 1)

	f.check(t, ping{"a.test", "192.0.2.1", 0})
	require.Empty(t, f.trig.hosts, "a host that passes is no longer tracked")

	f.check(t, ping{"a.test", "192.0.2.1", 100})
	require.Len(t, f.tracer.traced(), 2, "the first failure after a pass traces again")
}

func TestTrigger_HostsAreIndependent(t *testing.T) {
	f := newTriggerFixture(t, func(c *TracerouteConfig) { c.OnFailure.RetraceEvery = 2 })
	f.check(t, ping{"a.test", "192.0.2.1", 100}, ping{"b.test", "192.0.2.2", 0})
	f.check(t, ping{"a.test", "192.0.2.1", 100}, ping{"b.test", "192.0.2.2", 100})
	require.ElementsMatch(t, []string{"192.0.2.1", "192.0.2.2"}, f.tracer.traced())
}

func TestTrigger_TracesThePingedAddress(t *testing.T) {
	f := newTriggerFixture(t, nil)
	f.check(t, ping{"a.test", "192.0.2.7", 100}, ping{"192.0.2.8", "", 100})
	require.ElementsMatch(t, []string{"192.0.2.7", "192.0.2.8"}, f.tracer.traced(),
		"net.peer.ip when present, else the host as configured")
}

func TestTrigger_OneTraceInFlightPerHost(t *testing.T) {
	f := newTriggerFixture(t, func(c *TracerouteConfig) { c.OnFailure.RetraceEvery = 1 })
	f.tracer.block = make(chan struct{})
	batch := func() {
		require.NoError(t, f.trig.ConsumeMetrics(context.Background(), pingBatch(ping{"a.test", "192.0.2.1", 100})))
	}
	batch()
	require.Eventually(t, func() bool { return len(f.tracer.traced()) == 1 }, 5*time.Second, time.Millisecond)
	batch()
	batch()
	require.Len(t, f.tracer.traced(), 1, "a trace is due on every check, but one is already running")

	close(f.tracer.block)
	f.trig.wg.Wait()
	f.tracer.block = nil
	f.check(t, ping{"a.test", "192.0.2.1", 100})
	require.Len(t, f.tracer.traced(), 2, "once the trace finished the next due check traces again")
}

func TestTrigger_MaxHosts(t *testing.T) {
	f := newTriggerFixture(t, func(c *TracerouteConfig) { c.OnFailure.MaxHosts = 2 })
	f.check(t, ping{"a.test", "192.0.2.1", 100}, ping{"b.test", "192.0.2.2", 100}, ping{"c.test", "192.0.2.3", 100})
	require.ElementsMatch(t, []string{"192.0.2.1", "192.0.2.2"}, f.tracer.traced())
	f.check(t, ping{"d.test", "192.0.2.4", 100})
	require.Equal(t, 1, f.warns.FilterMessageSnippet("max_hosts").Len(), "warned once")

	f.check(t, ping{"a.test", "192.0.2.1", 0})
	f.check(t, ping{"c.test", "192.0.2.3", 100})
	require.Contains(t, f.tracer.traced(), "192.0.2.3", "a recovered host frees its slot")
}

func TestTrigger_NeverBlocksConsumeMetrics(t *testing.T) {
	f := newTriggerFixture(t, func(c *TracerouteConfig) { c.MaxConcurrentTraces = 1 })
	f.tracer.block = make(chan struct{})
	defer close(f.tracer.block)
	// Every trace slot is taken, as by a long scheduled cycle.
	f.prober.sem <- struct{}{}
	defer func() { <-f.prober.sem }()

	start := time.Now()
	for i := range 20 {
		host := string(rune('a'+i)) + ".test"
		require.NoError(t, f.trig.ConsumeMetrics(context.Background(), pingBatch(ping{host, "", 100})))
	}
	require.Less(t, time.Since(start), time.Second, "ConsumeMetrics must not wait for traces or slots")
	require.Len(t, f.metrics.AllMetrics(), 20, "every batch is forwarded")
	require.Empty(t, f.tracer.traced(), "no slot, so no trace has started")
}

func TestTrigger_SlotWaitIsBoundedByTimeout(t *testing.T) {
	f := newTriggerFixture(t, func(c *TracerouteConfig) { c.OnFailure.Timeout = 50 * time.Millisecond })
	for range cap(f.prober.sem) {
		f.prober.sem <- struct{}{}
	}
	f.check(t, ping{"a.test", "192.0.2.1", 100})
	require.Empty(t, f.tracer.traced())
	require.Empty(t, f.metrics.AllMetrics()[1:], "nothing emitted without a slot")
	for range cap(f.prober.sem) {
		<-f.prober.sem
	}
}

func TestTrigger_EmitsMetricsAndLogs(t *testing.T) {
	f := newTriggerFixture(t, nil)
	f.prober.setLogs(f.logs)
	f.tracer.res = TraceResult{
		Method: "udp",
		DestIP: "192.0.2.1",
		Hops: []HopResult{
			{Index: 1, Address: "10.0.0.1", RTT: 2 * time.Millisecond, Probes: 1},
			{Index: 2, Address: unansweredHopAddress, TimedOut: true, Probes: 3},
		},
	}
	f.check(t, ping{"a.test", "192.0.2.1", 100})

	all := f.metrics.AllMetrics()
	require.Len(t, all, 2, "the forwarded icmp batch, then the trace")
	trace := all[1]
	require.Equal(t, 1, trace.ResourceMetrics().Len())
	rm := trace.ResourceMetrics().At(0)
	addr, _ := rm.Resource().Attributes().Get("server.address")
	require.Equal(t, "a.test", addr.Str(), "the host as icmp names it")

	got := map[string][]pcommon.Map{}
	ms := rm.ScopeMetrics().At(0).Metrics()
	for i := range ms.Len() {
		dps := ms.At(i).Gauge().DataPoints()
		for j := range dps.Len() {
			got[ms.At(i).Name()] = append(got[ms.At(i).Name()], dps.At(j).Attributes())
			trig, _ := dps.At(j).Attributes().Get("traceroute.trigger")
			require.Equal(t, "ping_failure", trig.Str(), ms.At(i).Name())
		}
	}
	require.Len(t, got["traceroute.hop.status"], 2)
	require.Len(t, got["traceroute.hop.latency"], 1, "no latency for the silent hop")
	require.Len(t, got["traceroute.reached"], 1)
	require.Len(t, got["traceroute.hops"], 1)

	require.Equal(t, 1, f.logs.LogRecordCount())
	lr := f.logs.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	trig, _ := lr.Attributes().Get("traceroute.trigger")
	require.Equal(t, "ping_failure", trig.Str())
	ip, _ := lr.Attributes().Get("server.resolved_ip")
	require.Equal(t, "192.0.2.1", ip.Str())
	require.Equal(t, "WARN", lr.SeverityText(), "the destination was not reached")
}

func TestTrigger_FailedTraceEmitsReachedZeroAndNoLog(t *testing.T) {
	f := newTriggerFixture(t, nil)
	f.prober.setLogs(f.logs)
	f.tracer.err = errFake
	f.tracer.res = TraceResult{Method: "udp"}
	f.check(t, ping{"a.test", "2001:db8::1", 100})

	all := f.metrics.AllMetrics()
	require.Len(t, all, 2)
	require.Equal(t, 2, all[1].DataPointCount(), "traceroute.reached and traceroute.hops, both 0")
	require.Zero(t, f.logs.LogRecordCount(), "a trace that could not run has no path to record")
	require.Equal(t, 1, f.warns.FilterMessage("triggered traceroute failed").Len())
}

func TestTrigger_CloseStopsTraces(t *testing.T) {
	f := newTriggerFixture(t, nil)
	f.tracer.block = make(chan struct{})
	require.NoError(t, f.trig.ConsumeMetrics(context.Background(), pingBatch(ping{"a.test", "192.0.2.1", 100})))
	require.Eventually(t, func() bool { return len(f.tracer.traced()) == 1 }, 5*time.Second, time.Millisecond)

	start := time.Now()
	f.prober.stop()
	f.trig.close()
	require.Less(t, time.Since(start), time.Second, "close waits only for traces to notice the stop")
	require.Len(t, f.metrics.AllMetrics(), 1, "a trace cut short by the stop emits nothing")
	require.Zero(t, f.warns.FilterMessageSnippet("did not finish").Len(), "stopping is not a timeout")

	require.NoError(t, f.trig.ConsumeMetrics(context.Background(), pingBatch(ping{"b.test", "192.0.2.2", 100})))
	require.Len(t, f.tracer.traced(), 1, "no trace starts after close")
	require.Len(t, f.metrics.AllMetrics(), 2, "batches are still forwarded")
}
