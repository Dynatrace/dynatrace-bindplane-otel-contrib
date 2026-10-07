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
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/goleak"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver/internal/metadata"
)

func registered(id component.ID) (*sharedProber, int) {
	proberRegistryMu.Lock()
	defer proberRegistryMu.Unlock()
	if e, ok := proberRegistry[id]; ok {
		return e.prober, e.refs
	}
	return nil, 0
}

func settingsFor(name string) receiver.Settings {
	set := receivertest.NewNopSettings(metadata.Type)
	set.ID = component.MustNewIDWithName(metadata.Type.String(), name)
	return set
}

// A section that fails to start, here the third of four, is named; the two
// before it are running and the one after is not. The collector then calls
// Shutdown, which stops the running ones and leaks nothing.
func TestStartFailureInThirdSectionThenShutdown(t *testing.T) {
	ignore := goleak.IgnoreCurrent()
	cfg := mustLoad(t, `
initial_delay: 1h
icmp: {targets: [{host: 192.0.2.1}]}
tcp: {targets: [{endpoint: '127.0.0.1:9'}]}
traceroute: {targets: [{host: 192.0.2.1}]}
`)
	set := settingsFor("startfail")
	r, err := NewFactory().CreateMetrics(context.Background(), set, cfg, consumertest.NewNop())
	require.NoError(t, err)
	nc := r.(*networkCheck)
	var names []string
	for _, s := range nc.sections {
		names = append(names, s.name)
	}
	require.Equal(t, []string{"traceroute", "icmp", "tcp"}, names)
	boom := &fakeSection{startErr: errors.New("boom")}
	nc.sections = append(nc.sections[:2], section{"third", boom}, nc.sections[2])

	err = r.Start(context.Background(), componenttest.NewNopHost())
	require.EqualError(t, err, "starting the third section: boom")

	start := time.Now()
	require.NoError(t, r.Shutdown(context.Background()))
	require.Less(t, time.Since(start), time.Second)
	require.True(t, boom.stopped)
	p, _ := registered(set.ID)
	require.Nil(t, p, "the prober is released")
	goleak.VerifyNone(t, ignore)
}

// Metrics only, logs only and both: start, stop, release the prober, leak
// nothing. Sections hold their first check for an hour.
func TestPipelineCombinations(t *testing.T) {
	const yaml = `
initial_delay: 1h
icmp: {targets: [{host: 192.0.2.1}]}
tcp: {targets: [{endpoint: '127.0.0.1:9'}]}
traceroute: {targets: [{host: 192.0.2.1}]}
`
	for _, tc := range []struct {
		name          string
		metrics, logs bool
	}{
		{"metrics", true, false},
		{"logs", false, true},
		{"both", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ignore := goleak.IgnoreCurrent()
			cfg := mustLoad(t, yaml)
			set := settingsFor("combo-" + tc.name)
			var rs []component.Component
			if tc.metrics {
				r, err := NewFactory().CreateMetrics(context.Background(), set, cfg, consumertest.NewNop())
				require.NoError(t, err)
				rs = append(rs, r)
			}
			if tc.logs {
				r, err := NewFactory().CreateLogs(context.Background(), set, cfg, consumertest.NewNop())
				require.NoError(t, err)
				rs = append(rs, r)
			}
			_, refs := registered(set.ID)
			require.Equal(t, len(rs), refs)
			for _, r := range rs {
				require.NoError(t, r.Start(context.Background(), componenttest.NewNopHost()))
			}
			for _, r := range rs {
				require.NoError(t, r.Shutdown(context.Background()))
			}
			p, _ := registered(set.ID)
			require.Nil(t, p)
			goleak.VerifyNone(t, ignore)
		})
	}
}

// The README says a logs-only receiver gets no triggered traces: it has no
// icmp section to read. It starts and stops cleanly and emits nothing.
func TestLogsOnlyTriggerOnly(t *testing.T) {
	cfg := mustLoad(t, "initial_delay: 1h\nicmp: {targets: [{host: 192.0.2.1}]}\ntraceroute: {}")
	sink := new(consumertest.LogsSink)
	r, err := NewFactory().CreateLogs(context.Background(), settingsFor("logsonly"), cfg, sink)
	require.NoError(t, err)
	nc := r.(*networkCheck)
	require.Nil(t, nc.trigger)
	require.Empty(t, nc.sections)
	require.NoError(t, r.Start(context.Background(), componenttest.NewNopHost()))
	require.NoError(t, r.Shutdown(context.Background()))
	require.Zero(t, sink.LogRecordCount())
}

// networkcheck/a and networkcheck/b: distinct child IDs, distinct probers, and
// stopping one leaves the other tracing.
func TestTwoNamedReceiversAreIndependent(t *testing.T) {
	cfgA := tracerouteConfig(time.Hour)
	cfgB := tracerouteConfig(time.Hour)
	setA, setB := settingsFor("a"), settingsFor("b")
	icmpType := component.MustNewType("icmp_check")
	require.Equal(t, "icmp_check/a/icmp", childSettings(setA, icmpType, "icmp").ID.String())
	require.Equal(t, "icmp_check/b/icmp", childSettings(setB, icmpType, "icmp").ID.String())

	ra, err := NewFactory().CreateMetrics(context.Background(), setA, cfgA, consumertest.NewNop())
	require.NoError(t, err)
	rb, err := NewFactory().CreateMetrics(context.Background(), setB, cfgB, consumertest.NewNop())
	require.NoError(t, err)
	pa, pb := ra.(*networkCheck).prober, rb.(*networkCheck).prober
	require.NotSame(t, pa, pb)
	gotA, _ := registered(setA.ID)
	gotB, _ := registered(setB.ID)
	require.Same(t, pa, gotA)
	require.Same(t, pb, gotB)

	ok := func(context.Context) (TraceResult, error) { return TraceResult{Reached: true}, nil }
	pa.targets[0].trace, pb.targets[0].trace = ok, ok
	host := componenttest.NewNopHost()
	require.NoError(t, ra.Start(context.Background(), host))
	require.NoError(t, rb.Start(context.Background(), host))
	require.NoError(t, ra.Shutdown(context.Background()))
	require.False(t, pb.stopped(), "stopping a does not stop b")
	require.False(t, pb.latestCycle(context.Background(), 0).results[0].skipped, "b still traces")
	require.NoError(t, rb.Shutdown(context.Background()))
}

// A reload shuts the old receiver down, then builds and starts a new one with
// a new configuration under the same ID: the new one gets a new, live prober
// on the new targets.
func TestReloadSameIDNewConfig(t *testing.T) {
	set := settingsFor("reload")
	host := componenttest.NewNopHost()
	build := func(target string) (receiver.Metrics, receiver.Logs, *Config) {
		cfg := tracerouteConfig(time.Hour)
		cfg.Traceroute.Targets = []TracerouteTarget{{Host: target}}
		mr, err := NewFactory().CreateMetrics(context.Background(), set, cfg, consumertest.NewNop())
		require.NoError(t, err)
		lr, err := NewFactory().CreateLogs(context.Background(), set, cfg, consumertest.NewNop())
		require.NoError(t, err)
		return mr, lr, cfg
	}

	m1, l1, _ := build("192.0.2.1")
	old := m1.(*networkCheck).prober
	require.NoError(t, m1.Start(context.Background(), host))
	require.NoError(t, l1.Start(context.Background(), host))
	require.NoError(t, m1.Shutdown(context.Background()))
	require.NoError(t, l1.Shutdown(context.Background()))

	m2, l2, cfg2 := build("192.0.2.2")
	p := m2.(*networkCheck).prober
	require.NotSame(t, old, p)
	require.Same(t, p, l2.(*networkCheck).prober)
	require.Same(t, cfg2.Traceroute, p.cfg)
	require.Equal(t, "192.0.2.2", p.targets[0].host)
	require.False(t, p.stopped())
	var traced atomic.Value
	p.targets[0].trace = func(context.Context) (TraceResult, error) {
		traced.Store(p.targets[0].host)
		return TraceResult{Reached: true}, nil
	}
	require.NoError(t, m2.Start(context.Background(), host))
	require.NoError(t, l2.Start(context.Background(), host))
	require.Eventually(t, func() bool { return traced.Load() == "192.0.2.2" }, 5*time.Second, time.Millisecond)
	require.NoError(t, m2.Shutdown(context.Background()))
	require.NoError(t, l2.Shutdown(context.Background()))
	gone, _ := registered(set.ID)
	require.Nil(t, gone)
}

// Shutdown cancels an in-flight scheduled trace within 200ms for each
// pipeline combination.
func TestShutdownCancelsInflightTraceFast(t *testing.T) {
	for _, tc := range []struct {
		name          string
		metrics, logs bool
	}{
		{"metrics", true, false},
		{"logs", false, true},
		{"both", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tracerouteConfig(time.Hour)
			cfg.Traceroute.InitialDelay = 0
			set := settingsFor("fast-" + tc.name)
			var rs []component.Component
			if tc.metrics {
				r, err := NewFactory().CreateMetrics(context.Background(), set, cfg, consumertest.NewNop())
				require.NoError(t, err)
				rs = append(rs, r)
			}
			if tc.logs {
				r, err := NewFactory().CreateLogs(context.Background(), set, cfg, consumertest.NewNop())
				require.NoError(t, err)
				rs = append(rs, r)
			}
			blocker := &stubTrace{}
			p, _ := registered(set.ID)
			p.targets[0].trace = blocker.trace
			for _, r := range rs {
				require.NoError(t, r.Start(context.Background(), componenttest.NewNopHost()))
			}
			require.Eventually(t, func() bool { return blocker.running.Load() == 1 }, 5*time.Second, time.Millisecond)

			start := time.Now()
			for _, r := range rs {
				require.NoError(t, r.Shutdown(context.Background()))
			}
			require.Less(t, time.Since(start), 200*time.Millisecond)
			require.EqualValues(t, 1, blocker.calls.Load(), "one trace for both signals")
		})
	}
}
