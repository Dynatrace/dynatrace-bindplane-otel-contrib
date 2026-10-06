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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// stubTrace takes d per trace, or returns early when its context ends. A zero
// d blocks until the context ends.
type stubTrace struct {
	d         time.Duration
	calls     atomic.Int64
	running   atomic.Int64
	cancelled atomic.Int64
}

func (s *stubTrace) trace(ctx context.Context) (TraceResult, error) {
	s.calls.Add(1)
	s.running.Add(1)
	defer s.running.Add(-1)
	var wait <-chan time.Time
	if s.d > 0 {
		wait = time.After(s.d)
	}
	select {
	case <-wait:
		return TraceResult{Method: "udp", Reached: true, Hops: []HopResult{{Index: 1, Address: "192.0.2.1", RTT: time.Millisecond, Probes: 1}}}, nil
	case <-ctx.Done():
		s.cancelled.Add(1)
		return TraceResult{}, ctx.Err()
	}
}

// newStubProber builds a started prober whose targets run the stubs.
func newStubProber(t *testing.T, interval time.Duration, limit int, stubs ...*stubTrace) *sharedProber {
	t.Helper()
	cfg := defaultTracerouteConfig()
	cfg.CollectionInterval = interval
	cfg.MaxConcurrentTraces = limit
	p := newSharedProber(cfg, zap.NewNop())
	p.started = true
	for i, s := range stubs {
		p.targets = append(p.targets, &target{host: string(rune('a'+i)) + ".test", trace: s.trace})
	}
	t.Cleanup(p.stop)
	return p
}

// A receiver in both a metrics and a logs pipeline is instantiated twice; the
// two signals must render one cycle rather than trace every target twice.
func TestProber_TwoSignalsShareOneCycle(t *testing.T) {
	s := &stubTrace{d: time.Millisecond}
	p := newStubProber(t, time.Minute, 4, s)

	metricsCycle := p.latestCycle(context.Background(), p.cycleMaxAge())
	logsCycle := p.latestCycle(context.Background(), p.cycleMaxAge())
	require.Same(t, metricsCycle, logsCycle)
	require.EqualValues(t, 1, s.calls.Load())
}

func TestProber_RetracesAfterMaxAge(t *testing.T) {
	s := &stubTrace{d: time.Microsecond}
	p := newStubProber(t, 20*time.Millisecond, 4, s)

	p.latestCycle(context.Background(), p.cycleMaxAge())
	time.Sleep(40 * time.Millisecond)
	p.latestCycle(context.Background(), p.cycleMaxAge())
	require.EqualValues(t, 2, s.calls.Load(), "a stale cycle must trigger a fresh trace")
}

func TestProber_ConcurrentCallersTraceOnce(t *testing.T) {
	s := &stubTrace{d: 10 * time.Millisecond}
	p := newStubProber(t, time.Minute, 4, s)

	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.latestCycle(context.Background(), p.cycleMaxAge())
		}()
	}
	wg.Wait()
	require.EqualValues(t, 1, s.calls.Load())
}

func TestProber_TracesConcurrently(t *testing.T) {
	stubs := make([]*stubTrace, 8)
	for i := range stubs {
		stubs[i] = &stubTrace{d: 100 * time.Millisecond}
	}
	p := newStubProber(t, 10*time.Second, 4, stubs...)

	start := time.Now()
	cycle := p.latestCycle(context.Background(), p.cycleMaxAge())
	// Eight traces at four in flight is two waves of 100ms; sequential is 800ms.
	require.Less(t, time.Since(start), 600*time.Millisecond)
	for i, res := range cycle.results {
		require.Same(t, p.targets[i], res.target, "results stay in target order")
		require.False(t, res.skipped)
	}
}

func TestProber_DeadlineSkipsTailInsteadOfFailingIt(t *testing.T) {
	stubs := make([]*stubTrace, 10)
	for i := range stubs {
		stubs[i] = &stubTrace{d: 100 * time.Millisecond}
	}
	// A 300ms interval is a 270ms budget at one trace at a time: about two
	// traces finish, the rest are skipped.
	p := newStubProber(t, 300*time.Millisecond, 1, stubs...)
	cycle := p.latestCycle(context.Background(), p.cycleMaxAge())

	traced, skipped := 0, 0
	for _, res := range cycle.results {
		if res.skipped {
			skipped++
			require.NoError(t, res.err, "a skipped target carries no error")
			continue
		}
		traced++
		require.True(t, res.trace.Reached, "a trace cut off at the deadline is not reported as a failure")
	}
	require.GreaterOrEqual(t, traced, 1)
	require.GreaterOrEqual(t, skipped, 5)
	require.Nil(t, cycleError(cycle), "skips are not errors")
	require.Equal(t, traced, p.traceStart, "the next cycle starts where this one gave up")
}

func TestProber_StopCutsInflightCycle(t *testing.T) {
	blocker := &stubTrace{}
	p := newStubProber(t, time.Hour, 1, blocker)

	done := make(chan *traceCycle, 1)
	go func() { done <- p.latestCycle(context.Background(), p.cycleMaxAge()) }()
	require.Eventually(t, func() bool { return blocker.running.Load() == 1 }, 5*time.Second, time.Millisecond)

	p.stop()
	var cycle *traceCycle
	select {
	case cycle = <-done:
	case <-time.After(time.Second):
		t.Fatal("latestCycle must return promptly once the prober is stopped")
	}
	require.EqualValues(t, 1, blocker.cancelled.Load())
	require.True(t, cycle.results[0].skipped)

	later := p.latestCycle(context.Background(), 0)
	require.True(t, later.results[0].skipped, "stopped is sticky")
	require.EqualValues(t, 1, blocker.calls.Load(), "no trace runs after stop")
}

func TestProber_JitterInterruptedByStop(t *testing.T) {
	s := &stubTrace{d: time.Millisecond}
	p := newStubProber(t, time.Hour, 1, s)
	p.cfg.Jitter = 10 * time.Second

	done := make(chan *traceCycle, 1)
	go func() { done <- p.latestCycle(context.Background(), p.cycleMaxAge()) }()
	time.Sleep(50 * time.Millisecond)
	p.stop()
	select {
	case c := <-done:
		require.True(t, c.results[0].skipped)
		require.Zero(t, s.calls.Load())
	case <-time.After(time.Second):
		t.Fatal("stop must interrupt the jitter wait")
	}
}

// Freshness is measured from when a cycle was requested, so a jitter delay
// inside the cycle does not make the next tick's cycle look fresh.
func TestProber_JitterDoesNotConsumeFreshnessBudget(t *testing.T) {
	s := &stubTrace{d: time.Microsecond}
	p := newStubProber(t, 200*time.Millisecond, 1, s)
	p.cfg.Jitter = 100 * time.Millisecond

	// Measured from before the first call: if freshness were measured after
	// the jitter wait, the second call would still find the first cycle
	// fresh and reuse it.
	start := time.Now()
	first := p.latestCycle(context.Background(), p.cycleMaxAge())
	time.Sleep(time.Until(start.Add(p.cfg.CollectionInterval + 20*time.Millisecond)))
	second := p.latestCycle(context.Background(), p.cycleMaxAge())
	require.NotSame(t, first, second)
	require.EqualValues(t, 2, s.calls.Load())
}

func TestProber_BatchRotation(t *testing.T) {
	stubs := make([]*stubTrace, 5)
	for i := range stubs {
		stubs[i] = &stubTrace{d: time.Microsecond}
	}
	p := newStubProber(t, time.Hour, 4, stubs...)
	p.cfg.BatchSize = 2

	for range 6 {
		// maxAge 0 forces a fresh cycle per call.
		require.Len(t, p.latestCycle(context.Background(), 0).results, 2)
	}
	var got []int64
	for _, s := range stubs {
		got = append(got, s.calls.Load())
	}
	require.Equal(t, []int64{3, 3, 2, 2, 2}, got)
}

func TestProber_BatchWindowResumesAtSkippedTarget(t *testing.T) {
	stubs := make([]*stubTrace, 6)
	for i := range stubs {
		stubs[i] = &stubTrace{d: time.Microsecond}
	}
	stubs[2] = &stubTrace{} // blocks until the cycle deadline
	p := newStubProber(t, 200*time.Millisecond, 3, stubs...)
	p.cfg.BatchSize = 3

	for range 3 {
		p.latestCycle(context.Background(), 0)
	}
	completed := make([]int64, len(stubs))
	for i, s := range stubs {
		completed[i] = s.calls.Load() - s.cancelled.Load()
	}
	// Cycle 1 traces a, b and skips c; the window resumes at c, so cycles 2
	// and 3 trace c, d, e with c skipped each time. f is not reached yet.
	require.Equal(t, []int64{1, 1, 0, 2, 2, 0}, completed)
}

// Triggered traces take the same slots, so they can push scheduled targets
// past the cycle budget.
func TestProber_SlotsAreSharedWithTriggeredTraces(t *testing.T) {
	s := &stubTrace{d: time.Microsecond}
	p := newStubProber(t, 200*time.Millisecond, 1, s)

	p.sem <- struct{}{} // a triggered trace holds the only slot
	cycle := p.latestCycle(context.Background(), 0)
	require.True(t, cycle.results[0].skipped)
	require.Zero(t, s.calls.Load())

	<-p.sem
	cycle = p.latestCycle(context.Background(), 0)
	require.False(t, cycle.results[0].skipped)
}

func TestProber_ScheduleOffHasNoTargets(t *testing.T) {
	cfg := defaultTracerouteConfig()
	cfg.CollectionInterval = time.Minute
	cfg.Targets = []TracerouteTarget{{Host: "192.0.2.1"}}
	cfg.Schedule = false
	require.Empty(t, newSharedProber(cfg, zap.NewNop()).targets)
	require.Zero(t, worstCaseCycle(cfg))
}

func TestProberRegistry_RefcountsAcrossSignals(t *testing.T) {
	id := component.MustNewIDWithName("networkcheck", "refcount")
	cfg := defaultTracerouteConfig()
	a := acquireProber(id, cfg, zap.NewNop())
	b := acquireProber(id, cfg, zap.NewNop())
	require.Same(t, a, b)

	registered := func() bool {
		proberRegistryMu.Lock()
		defer proberRegistryMu.Unlock()
		_, ok := proberRegistry[id]
		return ok
	}
	releaseProber(id)
	require.True(t, registered(), "the first release keeps the prober for the other signal")
	releaseProber(id)
	require.False(t, registered())
}

func TestProberRegistry_DoesNotHandOutStoppedProber(t *testing.T) {
	id := component.MustNewIDWithName("networkcheck", "stopped")
	cfg := defaultTracerouteConfig()
	first := acquireProber(id, cfg, zap.NewNop())
	first.stop()
	second := acquireProber(id, cfg, zap.NewNop())
	t.Cleanup(func() { releaseProber(id); releaseProber(id) })
	require.NotSame(t, first, second, "a stopped prober would skip every target")
}

func TestProberRegistry_ReplacesStaleUnstartedProber(t *testing.T) {
	id := component.MustNewIDWithName("networkcheck", "stale")
	cfgA, cfgB := defaultTracerouteConfig(), defaultTracerouteConfig()

	// A receiver built with cfgA but never started: the collector abandons
	// components when the service fails to build, and never shuts them down.
	a := acquireProber(id, cfgA, zap.NewNop())
	b := acquireProber(id, cfgB, zap.NewNop())
	t.Cleanup(func() { releaseProber(id); releaseProber(id) })
	require.NotSame(t, a, b)
	require.Same(t, cfgB, b.cfg)

	b.start()
	c := acquireProber(id, cfgA, zap.NewNop())
	require.Same(t, b, c, "a live prober is shared")
}

func TestWorstCaseCycle(t *testing.T) {
	targets := func(n int) []TracerouteTarget { return make([]TracerouteTarget, n) }
	base := func(mutate func(*TracerouteConfig)) *TracerouteConfig {
		c := defaultTracerouteConfig()
		mutate(c)
		return c
	}
	for _, tc := range []struct {
		name string
		cfg  *TracerouteConfig
		want time.Duration
	}{
		{"no targets", base(func(*TracerouteConfig) {}), 0},
		{"one wave at the defaults", base(func(c *TracerouteConfig) { c.Targets = targets(4) }), 5 * 3 * 3 * time.Second},
		{"waves", base(func(c *TracerouteConfig) { c.Targets = targets(9) }), 3 * 5 * 3 * 3 * time.Second},
		{"batch caps the waves", base(func(c *TracerouteConfig) { c.Targets = targets(9); c.BatchSize = 4 }), 5 * 3 * 3 * time.Second},
		{"abort off walks max_hops", base(func(c *TracerouteConfig) {
			c.Targets = targets(1)
			c.MaxConsecutiveTimeouts = 0
			c.MaxHops = 10
			c.ProbesPerHop = 1
			c.Timeout = time.Second
		}), 10 * time.Second},
		{"jitter counts", base(func(c *TracerouteConfig) { c.Targets = targets(1); c.Jitter = time.Second }), 45*time.Second + time.Second},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, worstCaseCycle(tc.cfg))
		})
	}
}

func TestProber_StartWarnsWhenWorstCaseExceedsBudget(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	cfg := defaultTracerouteConfig()
	cfg.CollectionInterval = time.Minute
	cfg.Targets = make([]TracerouteTarget, 5) // two waves of 45s
	p := newSharedProber(cfg, zap.New(core))
	p.start()
	p.start()
	require.Equal(t, 1, logs.FilterMessageSnippet("worst-case trace cycle exceeds").Len(), "once per prober")
}
