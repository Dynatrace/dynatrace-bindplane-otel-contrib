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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// triggeredReached returns the traceroute.reached data point of the first
// triggered trace in sink, if any.
func triggeredReached(sink *consumertest.MetricsSink) (pmetric.ResourceMetrics, bool) {
	for _, md := range sink.AllMetrics() {
		for i := range md.ResourceMetrics().Len() {
			rm := md.ResourceMetrics().At(i)
			ms := rm.ScopeMetrics().At(0).Metrics()
			for j := range ms.Len() {
				if ms.At(j).Name() != "traceroute.reached" {
					continue
				}
				trig, _ := ms.At(j).Gauge().DataPoints().At(0).Attributes().Get("traceroute.trigger")
				if trig.Str() == "ping_failure" {
					return rm, true
				}
			}
		}
	}
	return pmetric.ResourceMetrics{}, false
}

// The real icmp_check pings a TEST-NET address that never answers; its
// ping.loss.ratio of 100 (percent) and net.peer.name reach the trigger, which
// traces it. Whether the trace itself can run depends on the platform's
// privileges; either way a triggered trace is emitted for the configured host.
// Skips where icmp_check cannot ping at all (no ICMP socket, no route).
func TestE2E_RealICMPTriggersTrace(t *testing.T) {
	if testing.Short() {
		t.Skip("sends packets")
	}
	cfg := mustLoad(t, `
collection_interval: 1s
initial_delay: 0s
icmp: {targets: [{host: 192.0.2.1, ping_count: 1, ping_timeout: 500ms}]}
traceroute: {max_hops: 2, timeout: 200ms, probes_per_hop: 1, max_consecutive_timeouts: 1, on_failure: {timeout: 5s}}
`)
	core, logs := observer.New(zap.InfoLevel)
	set := settingsFor("e2e")
	set.Logger = zap.New(core)
	sink := new(consumertest.MetricsSink)
	r, err := NewFactory().CreateMetrics(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, r.Start(context.Background(), componenttest.NewNopHost()))
	defer func() { require.NoError(t, r.Shutdown(context.Background())) }()

	var rm pmetric.ResourceMetrics
	ok := false
	deadline := time.Now().Add(10 * time.Second)
	for !ok && time.Now().Before(deadline) {
		if logs.FilterMessage("failed to ping host").Len() > 0 {
			t.Skipf("icmp_check cannot ping here: %v", logs.FilterMessage("failed to ping host").All()[0].ContextMap()["error"])
		}
		time.Sleep(50 * time.Millisecond)
		rm, ok = triggeredReached(sink)
	}
	require.True(t, ok, "a triggered trace within 10s")
	addr, _ := rm.Resource().Attributes().Get("server.address")
	require.Equal(t, "192.0.2.1", addr.Str())

	names := metricNames(sink)
	require.True(t, names["ping.loss.ratio"])
	for _, e := range logs.FilterMessage("triggered traceroute failed").All() {
		t.Logf("trace could not run here: %v", e.ContextMap()["error"])
	}
}
