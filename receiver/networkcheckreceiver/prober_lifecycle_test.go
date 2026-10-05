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
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver/internal/metadata"
)

// sleepingPinger takes d per probe, or returns early with the context error.
// A zero d blocks until the context ends.
type sleepingPinger struct {
	d         time.Duration
	calls     atomic.Int64
	running   atomic.Int64
	cancelled atomic.Int64
}

func (s *sleepingPinger) ping(ctx context.Context) (PingResult, error) {
	s.calls.Add(1)
	s.running.Add(1)
	defer s.running.Add(-1)
	var wait <-chan time.Time
	if s.d > 0 {
		wait = time.After(s.d)
	}
	select {
	case <-wait:
		return PingResult{Method: MethodHTTP, StatusCode: 200, TotalDuration: s.d}, nil
	case <-ctx.Done():
		s.cancelled.Add(1)
		return PingResult{}, ctx.Err()
	}
}

func newStubProber(t *testing.T, interval time.Duration, limit int, pingers ...*sleepingPinger) *sharedProber {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.CollectionInterval = interval
	cfg.MaxConcurrentProbes = limit
	p := newSharedProber(cfg, receivertest.NewNopSettings(metadata.Type))
	p.started = true
	for _, sp := range pingers {
		p.targets = append(p.targets, &targetState{
			cfg: TargetConfig{Method: MethodHTTP},
			p:   sp,
			tr:  newTracerouter(TracerouteConfig{}, "example.com", ""),
		})
	}
	return p
}

func TestProber_ProbesConcurrently(t *testing.T) {
	pingers := make([]*sleepingPinger, 50)
	for i := range pingers {
		pingers[i] = &sleepingPinger{d: 100 * time.Millisecond}
	}
	p := newStubProber(t, 10*time.Second, 16, pingers...)

	start := time.Now()
	cycle := p.latestCycle(context.Background(), p.cycleMaxAge())
	elapsed := time.Since(start)

	// 50 targets at 16 in flight is four waves of 100ms; sequential would be 5s.
	require.Less(t, elapsed, 600*time.Millisecond, "targets must be probed concurrently")
	require.Len(t, cycle.results, 50)
	for i, res := range cycle.results {
		require.Same(t, p.targets[i], res.target, "results must stay in target order")
		require.False(t, res.skipped)
		require.Equal(t, 200, res.ping.StatusCode)
	}
}

func TestProber_DeadlineSkipsTailInsteadOfFailingIt(t *testing.T) {
	pingers := make([]*sleepingPinger, 20)
	for i := range pingers {
		pingers[i] = &sleepingPinger{d: 100 * time.Millisecond}
	}
	// 300ms interval gives a 270ms budget: two probes finish, the third is cut
	// off, the rest never start.
	p := newStubProber(t, 300*time.Millisecond, 1, pingers...)

	cycle := p.latestCycle(context.Background(), p.cycleMaxAge())

	probed, skipped := 0, 0
	for i, res := range cycle.results {
		if res.skipped {
			skipped++
			require.NoError(t, res.pingErr, "a skipped target carries no error")
			require.Zero(t, p.targets[i].checkCount, "a skipped target is not counted as checked")
			continue
		}
		probed++
		require.Equal(t, 200, res.ping.StatusCode, "a cut-off probe must not be reported as a failure")
		require.Equal(t, 1, p.targets[i].checkCount)
	}
	require.GreaterOrEqual(t, probed, 1)
	require.GreaterOrEqual(t, skipped, 15)
	require.Equal(t, 20, probed+skipped)

	// The next cycle starts where this one gave up, so the tail is not skipped
	// forever.
	require.Equal(t, probed, p.probeStart)
}

func TestProber_StopCutsInflightCycle(t *testing.T) {
	blocker := &sleepingPinger{}
	p := newStubProber(t, time.Hour, 1, blocker)

	var cycle *probeCycle
	done := make(chan struct{})
	go func() {
		cycle = p.latestCycle(context.Background(), p.cycleMaxAge())
		close(done)
	}()
	require.Eventually(t, func() bool { return blocker.running.Load() == 1 }, time.Second, time.Millisecond)

	start := time.Now()
	p.stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("latestCycle must return promptly once the prober is stopped")
	}
	require.Less(t, time.Since(start), 200*time.Millisecond)
	require.EqualValues(t, 1, blocker.cancelled.Load(), "the in-flight probe must see its context cancelled")
	require.True(t, cycle.results[0].skipped)

	// Stopped is sticky: later cycles return at once with everything skipped.
	later := p.latestCycle(context.Background(), 0)
	require.NotSame(t, cycle, later)
	require.True(t, later.results[0].skipped)
	require.EqualValues(t, 1, blocker.calls.Load(), "no probe runs after stop")
}

// scraperhelper waits for the in-flight scrape before it calls the scraper's
// shutdown hook, so the receiver wrapper must cancel the cycle up front or
// Shutdown waits for the slowest probe.
func TestFactory_ShutdownDoesNotWaitForInflightProbe(t *testing.T) {
	entered := make(chan struct{}, 1)
	released := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		<-r.Context().Done()
		close(released)
	}))
	defer srv.Close()

	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.CollectionInterval = time.Hour
	cfg.InitialDelay = 0
	cfg.Targets = []TargetConfig{{Method: MethodHTTP}}
	cfg.Targets[0].Endpoint = srv.URL
	cfg.Targets[0].Timeout = 30 * time.Second

	recv, err := f.CreateMetrics(context.Background(), receivertest.NewNopSettings(metadata.Type), cfg, consumertest.NewNop())
	require.NoError(t, err)
	require.NoError(t, recv.Start(context.Background(), componenttest.NewNopHost()))

	select {
	case <-entered:
	case <-time.After(10 * time.Second):
		t.Fatal("probe never reached the server")
	}

	start := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	require.NoError(t, recv.Shutdown(ctx))
	require.Less(t, time.Since(start), 500*time.Millisecond, "Shutdown must not wait for the blocked probe")

	select {
	case <-released:
	case <-time.After(time.Second):
		t.Fatal("the probe's request must be cancelled by Shutdown")
	}
}

// Two signals ticking on one interval must keep rendering the same cycles when
// a cycle fills the budget, and targets skipped at the deadline must rotate
// rather than always being the same tail.
func TestProber_SharingHoldsUnderOverload(t *testing.T) {
	pingers := []*sleepingPinger{
		{d: 100 * time.Millisecond},
		{d: 100 * time.Millisecond},
		{d: 100 * time.Millisecond},
	}
	// 300ms interval, one probe at a time: each cycle fits two probes and skips one.
	const interval = 300 * time.Millisecond
	p := newStubProber(t, interval, 1, pingers...)

	seen := [2]map[*probeCycle]struct{}{{}, {}}
	var wg sync.WaitGroup
	stop := time.Now().Add(6 * interval)
	for sig := range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for time.Now().Before(stop) {
				c := p.latestCycle(context.Background(), p.cycleMaxAge())
				seen[sig][c] = struct{}{}
				<-ticker.C
			}
		}()
	}
	wg.Wait()

	p.mu.Lock()
	cycles := p.cycles
	p.mu.Unlock()
	require.GreaterOrEqual(t, cycles, 3)

	shared := 0
	for c := range seen[0] {
		if _, ok := seen[1][c]; ok {
			shared++
		}
	}
	require.GreaterOrEqual(t, shared, cycles-1, "every cycle but possibly the last must reach both signals")

	// Count completed probes per target across the distinct cycles: the third
	// probe of a cycle is started and then cut off at the deadline, so the
	// stubs' call counters include attempts that were discarded as skipped.
	completed := make([]int, len(pingers))
	all := map[*probeCycle]struct{}{}
	for _, m := range seen {
		for c := range m {
			all[c] = struct{}{}
		}
	}
	for c := range all {
		for i, res := range c.results {
			if !res.skipped {
				completed[i]++
			}
		}
	}
	total := 0
	for _, n := range completed {
		total += n
	}
	require.Equal(t, 2*len(all), total, "each cycle completes exactly two of three probes")
	for i, n := range completed {
		require.InDelta(t, float64(2*len(all))/3, float64(n), 1.01,
			"target %d must not be the permanently skipped tail", i)
	}
}

func TestProberRegistry_ReplacesStaleUnstartedProber(t *testing.T) {
	id := component.MustNewIDWithName("networkcheck", "stale")
	settings := receivertest.NewNopSettings(metadata.Type)
	cfgA := createDefaultConfig().(*Config)
	cfgB := createDefaultConfig().(*Config)

	// A receiver built with cfgA but never started: the collector abandons
	// components when the service fails to build, and never shuts them down.
	a := acquireProber(id, cfgA, settings)
	b := acquireProber(id, cfgB, settings)
	t.Cleanup(func() { releaseProber(id); releaseProber(id) })
	require.NotSame(t, a, b, "an unstarted prober with a different config is a leftover and must be replaced")
	require.Same(t, cfgB, b.cfg)

	// A live prober is shared even under a different config pointer.
	b.mu.Lock()
	b.started = true
	b.mu.Unlock()
	c := acquireProber(id, cfgA, settings)
	require.Same(t, b, c)
}

func TestProber_StartKeepsICMPTargetsWhenICMPUnavailable(t *testing.T) {
	orig := probeICMPMode
	probeICMPMode = func() (bool, bool) { return false, false }
	t.Cleanup(func() { probeICMPMode = orig })

	core, logs := observer.New(zap.WarnLevel)
	settings := receivertest.NewNopSettings(metadata.Type)
	settings.Logger = zap.New(core)
	cfg := createDefaultConfig().(*Config)
	cfg.Targets = []TargetConfig{{Method: MethodICMP}, {}}
	cfg.Targets[0].Endpoint = "127.0.0.1"
	cfg.Targets[1].Endpoint = "127.0.0.2"

	p := newSharedProber(cfg, settings)
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))

	for _, ts := range p.targets {
		require.IsType(t, &icmpPinger{}, ts.p, "an ICMP target must never be silently turned into an HTTP probe")
	}
	require.Equal(t, 1, logs.FilterMessageSnippet("ICMP sockets unavailable").Len(), "one warning for the host, not one per target")
}

func TestWorstCaseCycle(t *testing.T) {
	icmp := func(n int) []TargetConfig {
		out := make([]TargetConfig, n)
		return out
	}
	httpTargets := func(n int, timeout time.Duration) []TargetConfig {
		out := make([]TargetConfig, n)
		for i := range out {
			out[i].Method = MethodHTTP
			out[i].Timeout = timeout
		}
		return out
	}
	cases := []struct {
		name string
		cfg  Config
		want time.Duration
	}{
		{"no targets", Config{}, 0},
		{"10 icmp fit one wave", Config{Targets: icmp(10)}, 2*200*time.Millisecond + 5*time.Second},
		{"100 icmp at 16 is seven waves", Config{Targets: icmp(100)}, 7 * (2*200*time.Millisecond + 5*time.Second)},
		{"sequential hung http", Config{Targets: httpTargets(30, 2*time.Second), MaxConcurrentProbes: 1}, 60 * time.Second},
		{"batch size caps the wave count", Config{Targets: httpTargets(100, 2*time.Second), MaxConcurrentProbes: 1, BatchSize: 5}, 10 * time.Second},
		{"traceroute adds its silent tail", Config{
			Targets:    httpTargets(8, time.Second),
			Traceroute: TracerouteConfig{Enabled: true, ProbesPerHop: 3, Timeout: 3 * time.Second, MaxConsecutiveTimeouts: 5},
		}, time.Second + 2*(3*5*3*time.Second)},
		{"abort disabled walks max_hops", Config{
			Targets:    httpTargets(1, time.Second),
			Traceroute: TracerouteConfig{Enabled: true, ProbesPerHop: 1, Timeout: time.Second, MaxHops: 10},
		}, time.Second + 10*time.Second},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, worstCaseCycle(&tc.cfg))
		})
	}
}

func TestProber_StartWarnsWhenWorstCaseExceedsInterval(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	settings := receivertest.NewNopSettings(metadata.Type)
	settings.Logger = zap.New(core)
	cfg := createDefaultConfig().(*Config)
	cfg.CollectionInterval = 10 * time.Second
	cfg.MaxConcurrentProbes = 1
	cfg.Targets = []TargetConfig{{Method: MethodDNS, DNSQuery: "example.com"}, {Method: MethodDNS, DNSQuery: "example.com"}, {Method: MethodDNS, DNSQuery: "example.com"}}
	for i := range cfg.Targets {
		cfg.Targets[i].Endpoint = "127.0.0.1"
	}

	p := newSharedProber(cfg, settings)
	require.NoError(t, p.start(context.Background(), componenttest.NewNopHost()))
	require.Equal(t, 1, logs.FilterMessageSnippet("worst-case probe cycle exceeds").Len())
}

func TestProber_JitterInterruptedByStop(t *testing.T) {
	sp := &sleepingPinger{d: time.Millisecond}
	p := newStubProber(t, time.Hour, 1, sp)
	p.cfg.Jitter = 10 * time.Second

	done := make(chan *probeCycle, 1)
	go func() { done <- p.latestCycle(context.Background(), p.cycleMaxAge()) }()
	time.Sleep(50 * time.Millisecond)
	p.stop()

	select {
	case c := <-done:
		require.True(t, c.results[0].skipped)
		require.Zero(t, sp.calls.Load())
	case <-time.After(time.Second):
		t.Fatal("stop must interrupt the jitter sleep")
	}
}

func TestProber_BatchRotation(t *testing.T) {
	pingers := make([]*sleepingPinger, 5)
	for i := range pingers {
		pingers[i] = &sleepingPinger{d: time.Microsecond}
	}
	p := newStubProber(t, time.Hour, 16, pingers...)
	p.cfg.BatchSize = 2

	for range 6 {
		// maxAge 0 forces a fresh cycle per call.
		cycle := p.latestCycle(context.Background(), 0)
		require.Len(t, cycle.results, 2)
	}
	var got []int64
	for _, sp := range pingers {
		got = append(got, sp.calls.Load())
	}
	require.Equal(t, []int64{3, 3, 2, 2, 2}, got)
}

func TestEffectiveTimeoutClampsToCeiling(t *testing.T) {
	interval := 10 * time.Second // budget 9s, ceiling 8s
	require.Equal(t, 8*time.Second, effectiveTimeout(TargetConfig{Method: MethodHTTP}, interval), "default 10s clamped to 80%")
	require.Equal(t, 5*time.Second, effectiveTimeout(TargetConfig{}, interval), "icmp default under the ceiling")
	many := TargetConfig{Method: MethodICMP, PingCount: 30} // 29 × 200ms of pacing
	require.Equal(t, 8*time.Second-29*icmpInterval, effectiveTimeout(many, interval), "pacing comes out of the ceiling")
	explicit := TargetConfig{Method: MethodDNS}
	explicit.Timeout = 3 * time.Second
	require.Equal(t, 3*time.Second, effectiveTimeout(explicit, interval), "explicit timeout under the ceiling kept")
	require.Equal(t, 10*time.Second, effectiveTimeout(TargetConfig{Method: MethodHTTP}, 0), "no interval, no clamp")
	require.Equal(t, 100*time.Millisecond, effectiveTimeout(TargetConfig{PingCount: 100}, time.Second), "floor")
}
