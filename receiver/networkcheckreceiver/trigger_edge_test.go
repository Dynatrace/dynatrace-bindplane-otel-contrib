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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/goleak"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver/internal/metadata"
)

// splitBatches tells forwarded icmp batches from emitted traces. A trace
// goroutine can hand its batch on before ConsumeMetrics forwards the batch
// that started it, so their order in a sink is not fixed.
func splitBatches(all []pmetric.Metrics) (icmp, traces []pmetric.Metrics) {
	for _, md := range all {
		if _, ok := md.ResourceMetrics().At(0).Resource().Attributes().Get("server.address"); ok {
			traces = append(traces, md)
		} else {
			icmp = append(icmp, md)
		}
	}
	return icmp, traces
}

// With the default retrace_every the 1st, 11th and 21st failing checks trace;
// a pass restarts the count, so the next failure traces at once.
func TestTrigger_RetraceEveryDefault(t *testing.T) {
	f := newTriggerFixture(t, nil)
	var tracedAt []int
	for i := 1; i <= 25; i++ {
		before := len(f.tracer.traced())
		f.check(t, ping{"a.test", "192.0.2.1", 100})
		if len(f.tracer.traced()) > before {
			tracedAt = append(tracedAt, i)
		}
	}
	require.Equal(t, []int{1, 11, 21}, tracedAt)

	f.check(t, ping{"a.test", "192.0.2.1", 0})
	f.check(t, ping{"a.test", "192.0.2.1", 100})
	require.Len(t, f.tracer.traced(), 4, "the first failure after a pass traces")
	f.check(t, ping{"a.test", "192.0.2.1", 100})
	require.Len(t, f.tracer.traced(), 4, "the second does not")
}

// Integer data points count, and with several data points in one resource the
// last one decides.
func TestTrigger_DataPointShapes(t *testing.T) {
	batch := func(vals ...any) pmetric.Metrics {
		md := pmetric.NewMetrics()
		rm := md.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr(peerNameAttr, "a.test")
		rm.Resource().Attributes().PutStr(peerIPAttr, "192.0.2.1")
		dps := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		dps.SetName(pingLossMetric)
		g := dps.SetEmptyGauge().DataPoints()
		for _, v := range vals {
			switch v := v.(type) {
			case int:
				g.AppendEmpty().SetIntValue(int64(v))
			case float64:
				g.AppendEmpty().SetDoubleValue(v)
			}
		}
		return md
	}
	for _, tc := range []struct {
		name   string
		md     pmetric.Metrics
		traced bool
	}{
		{"int 100", batch(100), true},
		{"int 49", batch(49), false},
		{"last wins: fail then pass", batch(100.0, 0.0), false},
		{"last wins: pass then fail", batch(0.0, 100.0), true},
		{"no data points", batch(), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTriggerFixture(t, nil)
			require.NoError(t, f.trig.ConsumeMetrics(context.Background(), tc.md))
			f.trig.wg.Wait()
			require.Equal(t, tc.traced, len(f.tracer.traced()) == 1)
		})
	}
}

// A sum named ping.loss.ratio, a resource without net.peer.name and a resource
// with an empty one are ignored; the rest of the batch still counts.
func TestTrigger_IgnoresForeignShapes(t *testing.T) {
	f := newTriggerFixture(t, nil)
	md := pingBatch(ping{"ok.test", "192.0.2.1", 100})

	sum := md.ResourceMetrics().AppendEmpty()
	sum.Resource().Attributes().PutStr(peerNameAttr, "sum.test")
	m := sum.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName(pingLossMetric)
	m.SetEmptySum().DataPoints().AppendEmpty().SetDoubleValue(100)

	noName := md.ResourceMetrics().AppendEmpty()
	pingBatch(ping{"x", "192.0.2.3", 100}).ResourceMetrics().At(0).ScopeMetrics().CopyTo(noName.ScopeMetrics())

	empty := pingBatch(ping{"", "192.0.2.4", 100})
	empty.ResourceMetrics().MoveAndAppendTo(md.ResourceMetrics())

	require.NoError(t, f.trig.ConsumeMetrics(context.Background(), md))
	f.trig.wg.Wait()
	require.Equal(t, []string{"192.0.2.1"}, f.tracer.traced())
}

// Many resources in one batch: each failing host is traced once, passing ones
// are not, and the forwarded batch is the batch that came in.
func TestTrigger_ManyResourcesOneBatch(t *testing.T) {
	f := newTriggerFixture(t, nil)
	var pings []ping
	var want []string
	for i := range 20 {
		loss := 0.0
		if i%3 == 0 {
			loss = 100
			want = append(want, fmt.Sprintf("192.0.2.%d", i))
		}
		pings = append(pings, ping{fmt.Sprintf("h%d.test", i), fmt.Sprintf("192.0.2.%d", i), loss})
	}
	in := pingBatch(pings...)
	want0 := pmetric.NewMetrics()
	in.CopyTo(want0)
	f.check(t, pings...)
	require.ElementsMatch(t, want, f.tracer.traced())
	icmp, _ := splitBatches(f.metrics.AllMetrics())
	require.Equal(t, []pmetric.Metrics{want0}, icmp)
}

// A resource with no net.peer.ip attribute at all (icmp_check's net.peer.ip
// disabled) traces the host as configured.
func TestTrigger_MissingPeerIPAttribute(t *testing.T) {
	f := newTriggerFixture(t, nil)
	md := pingBatch(ping{"a.test", "unused", 100})
	md.ResourceMetrics().At(0).Resource().Attributes().Remove(peerIPAttr)
	require.NoError(t, f.trig.ConsumeMetrics(context.Background(), md))
	f.trig.wg.Wait()
	require.Equal(t, []string{"a.test"}, f.tracer.traced())
}

// icmp_check pings an IPv6 address for an IPv6 literal or a name with only
// AAAA records. The real tracer refuses it without sending anything: the
// trigger emits reached 0 and hops 0, no log record, and a warning.
func TestTrigger_IPv6PeerIPWithRealTracer(t *testing.T) {
	f := newTriggerFixture(t, nil)
	f.prober.setLogs(f.logs)
	cfg := f.prober.cfg
	f.trig.newTrace = func(dest string) func(context.Context) (TraceResult, error) {
		return newTracerouter(cfg, dest, "").trace
	}
	f.check(t, ping{"v6.test", "2001:db8::1", 100})

	_, traces := splitBatches(f.metrics.AllMetrics())
	require.Len(t, traces, 1)
	require.Equal(t, 2, traces[0].DataPointCount(), "reached 0 and hops 0")
	require.Zero(t, f.logs.LogRecordCount())
	w := f.warns.FilterMessage("triggered traceroute failed").All()
	require.Len(t, w, 1)
	require.Contains(t, w[0].ContextMap()["error"], "IPv4 destinations only")
}

// 50 failing batches for one host, delivered concurrently while its trace
// runs, start exactly one trace and never block ConsumeMetrics.
func TestTrigger_BurstDedup(t *testing.T) {
	for _, every := range []int{1, 10} {
		t.Run(fmt.Sprintf("retrace_every=%d", every), func(t *testing.T) {
			f := newTriggerFixture(t, func(c *TracerouteConfig) { c.OnFailure.RetraceEvery = every })
			f.tracer.block = make(chan struct{})
			require.NoError(t, f.trig.ConsumeMetrics(context.Background(), pingBatch(ping{"a.test", "192.0.2.1", 100})))
			require.Eventually(t, func() bool { return len(f.tracer.traced()) == 1 }, 5*time.Second, time.Millisecond)

			start := time.Now()
			var wg sync.WaitGroup
			for range 50 {
				wg.Go(func() {
					require.NoError(t, f.trig.ConsumeMetrics(context.Background(), pingBatch(ping{"a.test", "192.0.2.1", 100})))
				})
			}
			wg.Wait()
			require.Less(t, time.Since(start), time.Second)
			require.Len(t, f.tracer.traced(), 1)
			require.Len(t, f.metrics.AllMetrics(), 51, "every batch forwarded")

			close(f.tracer.block)
			f.trig.wg.Wait()
			f.trig.mu.Lock()
			require.Equal(t, 51, f.trig.hosts["a.test"].streak)
			f.trig.mu.Unlock()
		})
	}
}

// With every slot held and a tracer that would block forever, 50 concurrent
// batches for 50 hosts still return at once.
func TestTrigger_SlotsExhaustedConcurrent(t *testing.T) {
	f := newTriggerFixture(t, func(c *TracerouteConfig) { c.MaxConcurrentTraces = 1 })
	f.tracer.block = make(chan struct{})
	defer close(f.tracer.block)
	f.prober.sem <- struct{}{}
	defer func() { <-f.prober.sem }()

	start := time.Now()
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Go(func() {
			require.NoError(t, f.trig.ConsumeMetrics(context.Background(), pingBatch(ping{fmt.Sprintf("h%d.test", i), "", 100})))
		})
	}
	wg.Wait()
	require.Less(t, time.Since(start), time.Second)
	require.Len(t, f.metrics.AllMetrics(), 50)
}

// ConsumeMetrics forwards the very batch it was given, unmodified.
func TestTrigger_ForwardsSameObject(t *testing.T) {
	f := newTriggerFixture(t, nil)
	var mu sync.Mutex
	var got []pmetric.Metrics
	next, err := consumer.NewMetrics(func(_ context.Context, md pmetric.Metrics) error {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, md)
		return nil
	})
	require.NoError(t, err)
	f.trig.next = next
	in := pingBatch(ping{"a.test", "192.0.2.1", 100})
	want := pmetric.NewMetrics()
	in.CopyTo(want)
	require.NoError(t, f.trig.ConsumeMetrics(context.Background(), in))
	f.trig.wg.Wait()
	icmp, traces := splitBatches(got)
	require.Len(t, traces, 1)
	require.Len(t, icmp, 1)
	require.Equal(t, in, icmp[0], "same object, no copy")
	require.Equal(t, want, icmp[0], "and unchanged")
	require.False(t, f.trig.Capabilities().MutatesData)
}

// A downstream error comes back from ConsumeMetrics; the trace still runs, and
// its own failed hand-off is a warning.
func TestTrigger_DownstreamErrorPropagates(t *testing.T) {
	f := newTriggerFixture(t, nil)
	boom := errors.New("boom")
	f.trig.next = consumertest.NewErr(boom)
	err := f.trig.ConsumeMetrics(context.Background(), pingBatch(ping{"a.test", "192.0.2.1", 100}))
	require.ErrorIs(t, err, boom)
	f.trig.wg.Wait()
	require.Len(t, f.tracer.traced(), 1)
	require.Equal(t, 1, f.warns.FilterMessage("sending triggered traceroute metrics").Len())
}

// Shutdown with triggered traces running and others waiting for a slot ends
// within 200ms and leaves no goroutine behind; later batches start nothing.
func TestTrigger_ShutdownWithInflightTraces(t *testing.T) {
	ignore := goleak.IgnoreCurrent()
	f := newTriggerFixture(t, func(c *TracerouteConfig) { c.MaxConcurrentTraces = 2 })
	f.tracer.block = make(chan struct{}) // only the context ends a trace
	var pings []ping
	for i := range 10 {
		pings = append(pings, ping{fmt.Sprintf("h%d.test", i), fmt.Sprintf("192.0.2.%d", i), 100})
	}
	require.NoError(t, f.trig.ConsumeMetrics(context.Background(), pingBatch(pings...)))
	require.Eventually(t, func() bool { return len(f.tracer.traced()) == 2 }, 5*time.Second, time.Millisecond)

	start := time.Now()
	f.prober.stop()
	f.trig.close()
	require.Less(t, time.Since(start), 200*time.Millisecond)
	require.Len(t, f.metrics.AllMetrics(), 1, "nothing emitted for cut-off traces")
	require.Zero(t, f.warns.Len(), "stopping is not a timeout")

	// A waiter can take a slot freed by a cancelled trace while stop is still
	// cancelling the child contexts, so a trace or two may start, already
	// doomed, during stop. None starts after close.
	n := len(f.tracer.traced())
	require.NoError(t, f.trig.ConsumeMetrics(context.Background(), pingBatch(ping{"late.test", "192.0.2.99", 100})))
	require.Len(t, f.tracer.traced(), n)
	goleak.VerifyNone(t, ignore)
}

// triggerReceiver builds the metrics instance, and optionally the logs
// instance, of an icmp + empty-traceroute receiver whose sections never send a
// packet (initial_delay 1h), starts them, and puts the trigger on trace.
func triggerReceiver(t *testing.T, name string, withLogs bool, trace func(string) func(context.Context) (TraceResult, error)) (*networkCheck, *consumertest.MetricsSink, *consumertest.LogsSink) {
	t.Helper()
	cfg := mustLoad(t, "initial_delay: 1h\nicmp: {targets: [{host: 192.0.2.1}]}\ntraceroute: {}")
	set := receivertest.NewNopSettings(metadata.Type)
	set.ID = component.MustNewIDWithName(metadata.Type.String(), name)
	f := NewFactory()
	msink, lsink := new(consumertest.MetricsSink), new(consumertest.LogsSink)
	mr, err := f.CreateMetrics(context.Background(), set, cfg, msink)
	require.NoError(t, err)
	var lr receiver.Logs
	if withLogs {
		lr, err = f.CreateLogs(context.Background(), set, cfg, lsink)
		require.NoError(t, err)
	}
	nc := mr.(*networkCheck)
	nc.trigger.newTrace = trace
	host := componenttest.NewNopHost()
	require.NoError(t, mr.Start(context.Background(), host))
	if lr != nil {
		require.NoError(t, lr.Start(context.Background(), host))
	}
	t.Cleanup(func() {
		require.NoError(t, mr.Shutdown(context.Background()))
		if lr != nil {
			require.NoError(t, lr.Shutdown(context.Background()))
		}
	})
	return nc, msink, lsink
}

// slowTrace returns after d with a two-hop path to dest.
func slowTrace(d time.Duration) func(string) func(context.Context) (TraceResult, error) {
	return func(dest string) func(context.Context) (TraceResult, error) {
		return func(ctx context.Context) (TraceResult, error) {
			select {
			case <-time.After(d):
			case <-ctx.Done():
			}
			return TraceResult{Method: "udp", DestIP: dest, Reached: true, Hops: []HopResult{
				{Index: 1, Address: "10.0.0.1", RTT: time.Millisecond, Probes: 1},
				{Index: 2, Address: dest, RTT: 2 * time.Millisecond, Probes: 1},
			}}, nil
		}
	}
}

// A triggered trace reaches the metrics pipeline and, when the receiver is in
// a logs pipeline too, the logs pipeline: trigger ping_failure, resource
// server.address the configured host (not the pinged IP), timestamps the trace
// start. Without a logs pipeline only metrics.
func TestTriggeredTraceSignals(t *testing.T) {
	for _, withLogs := range []bool{true, false} {
		t.Run(fmt.Sprintf("logs=%v", withLogs), func(t *testing.T) {
			const delay = 100 * time.Millisecond
			nc, msink, lsink := triggerReceiver(t, fmt.Sprintf("sig%v", withLogs), withLogs, slowTrace(delay))
			before := time.Now()
			require.NoError(t, nc.trigger.ConsumeMetrics(context.Background(), pingBatch(ping{"edge.test", "192.0.2.9", 100})))
			nc.trigger.wg.Wait()

			icmp, traces := splitBatches(msink.AllMetrics())
			require.Len(t, icmp, 1)
			require.Len(t, traces, 1)
			rm := traces[0].ResourceMetrics().At(0)
			addr, _ := rm.Resource().Attributes().Get("server.address")
			require.Equal(t, "edge.test", addr.Str())
			ms := rm.ScopeMetrics().At(0).Metrics()
			for i := range ms.Len() {
				dps := ms.At(i).Gauge().DataPoints()
				for j := range dps.Len() {
					dp := dps.At(j)
					trig, _ := dp.Attributes().Get("traceroute.trigger")
					require.Equal(t, "ping_failure", trig.Str())
					ts := dp.Timestamp().AsTime()
					require.False(t, ts.Before(before), "stamped at the trace start")
					require.Less(t, ts.Sub(before), delay/2, "not at its end")
				}
			}

			if !withLogs {
				require.Zero(t, lsink.LogRecordCount())
				return
			}
			require.Equal(t, 1, lsink.LogRecordCount())
			rl := lsink.AllLogs()[0].ResourceLogs().At(0)
			addr, _ = rl.Resource().Attributes().Get("server.address")
			require.Equal(t, "edge.test", addr.Str())
			lr := rl.ScopeLogs().At(0).LogRecords().At(0)
			trig, _ := lr.Attributes().Get("traceroute.trigger")
			require.Equal(t, "ping_failure", trig.Str())
			ip, _ := lr.Attributes().Get("server.resolved_ip")
			require.Equal(t, "192.0.2.9", ip.Str())
			_, hasDNS := lr.Attributes().Get("dns.server")
			require.False(t, hasDNS)
			require.GreaterOrEqual(t, lr.ObservedTimestamp().AsTime().Sub(lr.Timestamp().AsTime()), delay)
			require.Equal(t, "INFO", lr.SeverityText())
		})
	}
}

// Triggered traces for several hosts finish at the same moment and build
// their telemetry concurrently; run under -race.
func TestTriggeredEmitsConcurrently(t *testing.T) {
	const n = 8
	f := newTriggerFixture(t, func(c *TracerouteConfig) { c.MaxConcurrentTraces = n })
	f.prober.setLogs(f.logs)
	var started sync.WaitGroup
	started.Add(n)
	f.trig.newTrace = func(dest string) func(context.Context) (TraceResult, error) {
		return func(context.Context) (TraceResult, error) {
			started.Done()
			started.Wait() // every trace returns together
			return TraceResult{Method: "udp", DestIP: dest, Reached: true, Hops: []HopResult{{Index: 1, Address: dest, RTT: time.Millisecond, Probes: 1}}}, nil
		}
	}
	var pings []ping
	for i := range n {
		pings = append(pings, ping{fmt.Sprintf("h%d.test", i), fmt.Sprintf("192.0.2.%d", i), 100})
	}
	f.check(t, pings...)
	require.Len(t, f.metrics.AllMetrics(), n+1)
	require.Equal(t, n, f.logs.LogRecordCount())
	hosts := map[string]bool{}
	_, traces := splitBatches(f.metrics.AllMetrics())
	for _, md := range traces {
		require.Equal(t, 1, md.ResourceMetrics().Len(), "one host per emitted batch")
		addr, _ := md.ResourceMetrics().At(0).Resource().Attributes().Get("server.address")
		hosts[addr.Str()] = true
		require.NotZero(t, md.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0).Timestamp())
	}
	require.Len(t, hosts, n)
}

// The README: a triggered trace that does not get a slot within
// on_failure.timeout emits nothing and logs a warning.
func TestTrigger_SlotTimeoutWarns(t *testing.T) {
	f := newTriggerFixture(t, func(c *TracerouteConfig) { c.OnFailure.Timeout = 30 * time.Millisecond })
	for range cap(f.prober.sem) {
		f.prober.sem <- struct{}{}
	}
	defer func() {
		for range cap(f.prober.sem) {
			<-f.prober.sem
		}
	}()
	f.check(t, ping{"a.test", "192.0.2.1", 100})
	require.Equal(t, 1, f.warns.FilterMessageSnippet("on_failure.timeout").Len())
}
