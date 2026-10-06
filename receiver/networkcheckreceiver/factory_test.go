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
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver/internal/metadata"
)

func metricNames(sink *consumertest.MetricsSink) map[string]bool {
	names := map[string]bool{}
	for _, md := range sink.AllMetrics() {
		rms := md.ResourceMetrics()
		for i := range rms.Len() {
			sms := rms.At(i).ScopeMetrics()
			for j := range sms.Len() {
				ms := sms.At(j).Metrics()
				for k := range ms.Len() {
					names[ms.At(k).Name()] = true
				}
			}
		}
	}
	return names
}

func TestChildSettings(t *testing.T) {
	set := receivertest.NewNopSettings(metadata.Type)
	set.ID = component.NewID(metadata.Type)
	httpType := component.MustNewType("http_check")
	require.Equal(t, "http_check/networkcheck/http", childSettings(set, httpType, "http").ID.String())

	set.ID = component.MustNewIDWithName("networkcheck", "edge")
	require.Equal(t, "http_check/edge/http", childSettings(set, httpType, "http").ID.String())
}

// Every check section runs the real upstream receiver against a loopback
// server and its metrics reach the pipeline under their upstream names.
func TestSectionsRunUpstreamReceivers(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	defer srv.Close()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()

	dnsServer := fakeTraceDNS(t, map[string][]net.IP{"probe.test.": {net.IPv4(192, 0, 2, 1)}})

	cfg := mustLoad(t, fmt.Sprintf(`
collection_interval: 200ms
initial_delay: 0s
http:
  targets:
    - endpoint: %s
icmp:
  targets:
    - host: 127.0.0.1
      ping_count: 1
      ping_timeout: 1s
dns:
  dns_servers:
    - endpoint: %s
  hostnames:
    - name: probe.test
tcp:
  targets:
    - endpoint: %s
`, srv.URL, dnsServer, ln.Addr()))
	require.NoError(t, componenttest.CheckConfigStruct(cfg))

	sink := new(consumertest.MetricsSink)
	r, err := NewFactory().CreateMetrics(context.Background(), receivertest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, r.Start(context.Background(), componenttest.NewNopHost()))

	want := []string{"httpcheck.status", "dnscheck.status", "tcpcheck.status"}
	require.Eventually(t, func() bool {
		names := metricNames(sink)
		for _, n := range want {
			if !names[n] {
				return false
			}
		}
		return true
	}, 15*time.Second, 50*time.Millisecond, "metrics from every section")
	require.NoError(t, r.Shutdown(context.Background()))

	// icmp_check emits nothing when it cannot open its socket, which depends
	// on the host (ping_group_range on Linux).
	if names := metricNames(sink); names["ping.loss.ratio"] {
		require.True(t, names["ping.rtt.avg"])
	} else {
		t.Log("icmp_check could not ping loopback here; skipping the icmp assertions")
	}
}

func TestStartFailureNamesTheSection(t *testing.T) {
	cfg := mustLoad(t, `
http:
  targets:
    - endpoint: http://127.0.0.1:1
      auth:
        authenticator: missing
`)
	r, err := NewFactory().CreateMetrics(context.Background(), receivertest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
	require.NoError(t, err)
	err = r.Start(context.Background(), componenttest.NewNopHost())
	require.ErrorContains(t, err, "starting the http section")
	require.NoError(t, r.Shutdown(context.Background()))
}

type fakeSection struct {
	startErr, stopErr error
	started, stopped  bool
}

func (f *fakeSection) Start(context.Context, component.Host) error {
	f.started = true
	return f.startErr
}

func (f *fakeSection) Shutdown(context.Context) error {
	f.stopped = true
	return f.stopErr
}

func TestFanOut(t *testing.T) {
	boomA, boomC := errors.New("a failed"), errors.New("c failed")
	a, b, c := &fakeSection{stopErr: boomA}, &fakeSection{startErr: errors.New("b failed")}, &fakeSection{stopErr: boomC}
	r := &networkCheck{sections: []section{{"a", a}, {"b", b}, {"c", c}}}

	err := r.Start(context.Background(), componenttest.NewNopHost())
	require.EqualError(t, err, "starting the b section: b failed")
	require.True(t, a.started)
	require.False(t, c.started, "start stops at the first failure; the collector then shuts the receiver down")

	err = r.Shutdown(context.Background())
	require.ErrorIs(t, err, boomA)
	require.ErrorIs(t, err, boomC)
	require.ErrorContains(t, err, "stopping the a section")
	require.ErrorContains(t, err, "stopping the c section")
	require.True(t, a.stopped && b.stopped && c.stopped, "shutdown continues past failures")
}

func TestLogsWithoutTracerouteEmitNothing(t *testing.T) {
	cfg := mustLoad(t, "tcp: {targets: [{endpoint: '127.0.0.1:9'}]}")
	sink := new(consumertest.LogsSink)
	r, err := NewFactory().CreateLogs(context.Background(), receivertest.NewNopSettings(metadata.Type), cfg, sink)
	require.NoError(t, err)
	require.NoError(t, r.Start(context.Background(), componenttest.NewNopHost()))
	require.NoError(t, r.Shutdown(context.Background()))
	require.Zero(t, sink.LogRecordCount())
}

func TestICMPSendsThroughTriggerOnlyWithOnFailure(t *testing.T) {
	for _, tc := range []struct {
		yaml    string
		trigger bool
	}{
		{"icmp: {targets: [{host: 127.0.0.1}]}\ntraceroute: {}", true},
		{"icmp: {targets: [{host: 127.0.0.1}]}\ntraceroute: {on_failure: {enabled: false}, targets: [{host: 127.0.0.1}]}", false},
		{"icmp: {targets: [{host: 127.0.0.1}]}", false},
	} {
		r, err := NewFactory().CreateMetrics(context.Background(), receivertest.NewNopSettings(metadata.Type), mustLoad(t, tc.yaml), consumertest.NewNop())
		require.NoError(t, err)
		nc := r.(*networkCheck)
		require.Equal(t, tc.trigger, nc.trigger != nil, tc.yaml)
		require.NoError(t, r.Shutdown(context.Background()))
	}
}

// tracerouteConfig is a receiver with only scheduled traceroute, tracing
// immediately on start.
func tracerouteConfig(interval time.Duration) *Config {
	tc := defaultTracerouteConfig()
	tc.CollectionInterval = interval
	tc.Targets = []TracerouteTarget{{Host: "192.0.2.1", DNSServer: "192.0.2.53"}}
	cfg := createDefaultConfig().(*Config)
	cfg.Traceroute = tc
	return cfg
}

// The metrics and logs instances of one receiver share one prober, so each
// target is traced once per cycle and both signals describe that trace.
func TestSignalsShareScheduledTraces(t *testing.T) {
	cfg := tracerouteConfig(time.Hour)
	set := receivertest.NewNopSettings(metadata.Type)
	f := NewFactory()
	msink, lsink := new(consumertest.MetricsSink), new(consumertest.LogsSink)

	mr, err := f.CreateMetrics(context.Background(), set, cfg, msink)
	require.NoError(t, err)
	lr, err := f.CreateLogs(context.Background(), set, cfg, lsink)
	require.NoError(t, err)
	p := mr.(*networkCheck).prober
	require.Same(t, p, lr.(*networkCheck).prober)

	var calls atomic.Int64
	p.targets[0].trace = func(context.Context) (TraceResult, error) {
		calls.Add(1)
		return TraceResult{Method: "udp", DestIP: "192.0.2.1", Reached: true, Hops: []HopResult{
			{Index: 1, Address: "10.0.0.1", RTT: time.Millisecond, Probes: 1},
			{Index: 2, Address: "192.0.2.1", RTT: 2 * time.Millisecond, Probes: 2},
		}}, nil
	}

	host := componenttest.NewNopHost()
	require.NoError(t, mr.Start(context.Background(), host))
	require.NoError(t, lr.Start(context.Background(), host))
	require.Eventually(t, func() bool {
		return len(msink.AllMetrics()) > 0 && lsink.LogRecordCount() > 0
	}, 10*time.Second, 10*time.Millisecond)
	require.NoError(t, mr.Shutdown(context.Background()))
	require.NoError(t, lr.Shutdown(context.Background()))
	require.EqualValues(t, 1, calls.Load(), "one trace for both signals")

	rm := msink.AllMetrics()[0].ResourceMetrics().At(0)
	addr, _ := rm.Resource().Attributes().Get("server.address")
	require.Equal(t, "192.0.2.1", addr.Str())
	ms := rm.ScopeMetrics().At(0).Metrics()
	for i := range ms.Len() {
		dps := ms.At(i).Gauge().DataPoints()
		for j := range dps.Len() {
			attrs := dps.At(j).Attributes()
			trig, _ := attrs.Get("traceroute.trigger")
			require.Equal(t, "scheduled", trig.Str())
			dns, _ := attrs.Get("dns.server")
			require.Equal(t, "192.0.2.53", dns.Str())
		}
	}

	lr0 := lsink.AllLogs()[0].ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	trig, _ := lr0.Attributes().Get("traceroute.trigger")
	require.Equal(t, "scheduled", trig.Str())
	require.Equal(t, "INFO", lr0.SeverityText())
	hops, _ := lr0.Body().Map().Get("hops")
	require.Equal(t, 2, hops.Slice().Len())
	require.EqualValues(t, 2, hops.Slice().At(1).Map().AsRaw()["probes"])

	proberRegistryMu.Lock()
	defer proberRegistryMu.Unlock()
	require.Empty(t, proberRegistry, "both signals released the prober")
}

// scraperhelper waits for a running scrape before it stops, so Shutdown must
// cancel in-flight traces first or wait for the slowest one.
func TestShutdownDoesNotWaitForInflightTrace(t *testing.T) {
	cfg := tracerouteConfig(time.Hour)
	r, err := NewFactory().CreateMetrics(context.Background(), receivertest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
	require.NoError(t, err)
	blocker := &stubTrace{}
	r.(*networkCheck).prober.targets[0].trace = blocker.trace
	require.NoError(t, r.Start(context.Background(), componenttest.NewNopHost()))
	require.Eventually(t, func() bool { return blocker.running.Load() == 1 }, 10*time.Second, time.Millisecond)

	start := time.Now()
	require.NoError(t, r.Shutdown(context.Background()))
	require.Less(t, time.Since(start), time.Second)
	require.EqualValues(t, 1, blocker.cancelled.Load())
}

// A trace that could not run is data (reached 0) plus one scrape error per
// cycle; a skipped target emits nothing.
func TestScheduledRender(t *testing.T) {
	set := receivertest.NewNopSettings(metadata.Type)
	cfg := tracerouteConfig(time.Minute).Traceroute
	s := newTraceScraper(set, cfg, newSharedProber(cfg, set.Logger))
	start := time.Now().Add(-time.Second)
	cycle := &traceCycle{results: []targetResult{
		{target: &target{host: "ok.test"}, startedAt: start, trace: TraceResult{Reached: true, Hops: []HopResult{{Index: 1, Address: "192.0.2.1", RTT: 1500 * time.Microsecond, Probes: 1}}}},
		{target: &target{host: "v6.test"}, startedAt: start, err: errFake},
		{target: &target{host: "late.test"}, skipped: true},
	}}

	mb := s.mb
	for _, res := range cycle.results {
		if !res.skipped {
			recordTraceMetrics(mb, s.rb, res, metadata.AttributeTracerouteTriggerScheduled)
		}
	}
	md := mb.Emit()
	require.Equal(t, 2, md.ResourceMetrics().Len(), "the skipped target emits nothing")
	byHost := map[string]map[string]float64{}
	for i := range md.ResourceMetrics().Len() {
		rm := md.ResourceMetrics().At(i)
		host, _ := rm.Resource().Attributes().Get("server.address")
		vals := map[string]float64{}
		ms := rm.ScopeMetrics().At(0).Metrics()
		for j := range ms.Len() {
			dp := ms.At(j).Gauge().DataPoints().At(0)
			require.Equal(t, pcommon.NewTimestampFromTime(start), dp.Timestamp(), "stamped with the trace start")
			if dp.ValueType() == pmetric.NumberDataPointValueTypeInt {
				vals[ms.At(j).Name()] = float64(dp.IntValue())
			} else {
				vals[ms.At(j).Name()] = dp.DoubleValue()
			}
		}
		byHost[host.Str()] = vals
	}
	require.Equal(t, map[string]float64{
		"traceroute.reached": 1, "traceroute.hops": 1, "traceroute.hop.status": 1, "traceroute.hop.latency": 1.5,
	}, byHost["ok.test"])
	require.Equal(t, map[string]float64{"traceroute.reached": 0, "traceroute.hops": 0}, byHost["v6.test"])

	err := cycleError(cycle)
	require.EqualError(t, err, "1 of 2 targets failed: traceroute v6.test: "+errFake.Error())
}

func TestCycleErrorNamesAtMostThree(t *testing.T) {
	var results []targetResult
	for i := range 5 {
		results = append(results, targetResult{target: &target{host: fmt.Sprintf("h%d", i)}, err: errFake})
	}
	err := cycleError(&traceCycle{results: results})
	require.ErrorContains(t, err, "5 of 5 targets failed")
	require.ErrorContains(t, err, "(+2 more)")
	require.NotContains(t, err.Error(), "h3")
}
