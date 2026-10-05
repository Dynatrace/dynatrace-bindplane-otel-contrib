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

package networkcheckreceiver // import "github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver"

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

// defaultMaxConcurrentProbes is the in-flight probe limit per cycle when
// max_concurrent_probes is left at 0.
const defaultMaxConcurrentProbes = 16

// maxConcurrentTraces bounds traceroutes in flight within a cycle. Each trace
// holds a raw ICMP socket that receives every ICMP packet on the host, so many
// at once multiply that parsing work.
// Deliberate simplification: fixed at 4; make it a setting if anyone measures a need.
const maxConcurrentTraces = 4

// probeICMPMode is checkICMPMode behind a variable so tests can simulate a host
// where no ICMP socket can be opened.
var probeICMPMode = checkICMPMode

// targetResult is one target's outcome for a single probe cycle.
type targetResult struct {
	target *targetState

	// startedAt is when this target's probe began, used as the log record
	// timestamp so a slow check lands at its start rather than its completion.
	startedAt time.Time

	ping    PingResult
	pingErr error

	// skipped is true when the probe never ran: the cycle hit its deadline or
	// the prober was stopped before this target's turn. A skipped target emits
	// nothing for the cycle, as opposed to a failed probe, which emits its
	// status series.
	skipped bool

	// traced is true when traceroute ran for this target on this cycle;
	// shouldRun only fires every Nth check.
	traced   bool
	trace    TraceResult
	traceErr error
}

// probeCycle is one pass over the active batch of targets. Both the metrics and
// the logs path render the same cycle, so the two signals always describe the
// same observation.
type probeCycle struct {
	// at is when probing began, used as the timestamp on emitted telemetry.
	at time.Time

	// requestedAt is when the cycle was asked for, before any jitter delay.
	// Freshness is measured from this: charging the jitter wait against the
	// freshness budget would let a jitter above 10% of the collection interval
	// make the cached cycle look fresh at the next tick, so no probe would run
	// and the previous results would be re-emitted under their original
	// timestamp.
	requestedAt time.Time

	// completedAt is when the last probe returned. A caller that arrived before
	// this was waiting on the cycle while it ran, so the cycle is that caller's
	// observation too. That is what keeps two signals on one cycle when a
	// cycle runs longer than the freshness budget.
	completedAt time.Time

	// results is in target order regardless of the order probes ran in.
	results []targetResult
}

// sharedProber owns the probing state for one receiver instance. A receiver ID
// wired into both a metrics and a logs pipeline is instantiated twice by the
// collector; without sharing, every probe would run twice and the target would
// see double the traffic. Instances are refcounted through acquireProber and
// released on shutdown.
type sharedProber struct {
	cfg      *Config
	logger   *zap.Logger
	settings receiver.Settings

	// stopCtx is cancelled by stop, which the receiver wrappers call first
	// thing in Shutdown. Every probe runs under it, so shutdown cuts an
	// in-flight cycle short instead of waiting for its slowest target.
	stopCtx context.Context
	stopFn  context.CancelFunc

	traceSem chan struct{}

	mu          sync.Mutex
	started     bool
	targets     []*targetState
	systemDNS   string
	batchOffset int

	// probeStart rotates the order targets are probed in within a batch, so
	// targets skipped at a cycle deadline go first on the next cycle instead
	// of being the permanent tail that never gets probed.
	probeStart int

	last *probeCycle

	// inflight is closed when the running cycle completes; nil when idle.
	inflight chan struct{}

	// cycles counts completed probe cycles. Tests assert on it to prove two
	// signals share one cycle rather than probing independently.
	cycles int

	// lastSkipWarn rate-limits the deadline warning; only the cycle runner
	// touches it.
	lastSkipWarn time.Time
}

func newSharedProber(cfg *Config, settings receiver.Settings) *sharedProber {
	ctx, cancel := context.WithCancel(context.Background())
	return &sharedProber{
		cfg:      cfg,
		logger:   settings.Logger,
		settings: settings,
		stopCtx:  ctx,
		stopFn:   cancel,
		traceSem: make(chan struct{}, maxConcurrentTraces),
	}
}

// stop cancels the in-flight cycle, if any, and makes every later cycle return
// immediately with all targets skipped. Idempotent.
func (p *sharedProber) stop() {
	if p.stopFn != nil {
		p.stopFn()
	}
}

func (p *sharedProber) stopped() bool {
	return p.stopCtx != nil && p.stopCtx.Err() != nil
}

func (p *sharedProber) stopContext() context.Context {
	if p.stopCtx == nil {
		return context.Background()
	}
	return p.stopCtx
}

var (
	proberRegistryMu sync.Mutex
	proberRegistry   = map[component.ID]*proberEntry{}
)

type proberEntry struct {
	prober *sharedProber
	refs   int
}

// acquireProber returns the prober for id, creating it on first use. Each call
// must be paired with releaseProber.
func acquireProber(id component.ID, cfg *Config, settings receiver.Settings) *sharedProber {
	proberRegistryMu.Lock()
	defer proberRegistryMu.Unlock()

	if e, ok := proberRegistry[id]; ok {
		if e.prober.cfg == cfg {
			e.refs++
			return e.prober
		}
		// A different Config under the same ID means the entry is a leftover:
		// a receiver that was built but never started (the collector abandons
		// components when the service fails to build) or one already stopped.
		// Reusing it would probe the old targets, so replace it unless it is
		// live.
		e.prober.mu.Lock()
		live := e.prober.started && !e.prober.stopped()
		e.prober.mu.Unlock()
		if live {
			settings.Logger.Warn("receiver already running under this ID with a different configuration; sharing its prober",
				zap.Stringer("id", id))
			e.refs++
			return e.prober
		}
	}
	p := newSharedProber(cfg, settings)
	proberRegistry[id] = &proberEntry{prober: p, refs: 1}
	return p
}

// releaseProber drops a reference and removes the prober once the last signal
// using it has shut down.
func releaseProber(id component.ID) {
	proberRegistryMu.Lock()
	defer proberRegistryMu.Unlock()

	e, ok := proberRegistry[id]
	if !ok {
		return
	}
	e.refs--
	if e.refs <= 0 {
		delete(proberRegistry, id)
	}
}

// start builds probe state. Safe to call once per signal; only the first call
// does work.
func (p *sharedProber) start(ctx context.Context, host component.Host) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return nil
	}

	p.systemDNS = detectSystemDNS()

	icmpAvailable, icmpPrivileged := probeICMPMode()
	switch {
	case !icmpAvailable:
		// ICMP targets stay ICMP: the pinger reports packet_loss 1 with the
		// socket error on every cycle, which keeps the ping series and its
		// alerts intact. Silently probing HTTP instead would move the target
		// to a different metric family without anyone asking for it.
		p.logger.Warn("ICMP sockets unavailable; ICMP targets will report packet_loss 1 until the collector can open one. " +
			"Linux: grant CAP_NET_RAW or include the collector's gid in net.ipv4.ping_group_range. " +
			"Windows: run as a service or from an elevated shell.")
	case !icmpPrivileged:
		p.logger.Info("ICMP running in datagram (unprivileged) mode")
	}

	// Built locally and published only on success: a mid-loop failure that left
	// a partial list on the prober would be appended to a second time when the
	// other signal calls start, and every target already built would be probed
	// twice per cycle.
	var targets []*targetState

	for i, tc := range p.cfg.Targets {
		method := tc.Method
		if method == "" {
			method = MethodICMP
		}

		// Redacted as a matter of course: the string becomes the dns.server
		// attribute as written, and the prober resolves through it unchanged.
		dnsServer := redactEndpoint(tc.DNSServer)
		tc.Timeout = effectiveTimeout(tc, p.cfg.CollectionInterval)
		if dnsServer == "" {
			dnsServer = p.systemDNS
		}

		var pg pinger
		var err error

		switch method {
		case MethodICMP:
			pg = newICMPPinger(tc, icmpPrivileged)
		case MethodDNS:
			pg = newDNSPinger(tc)
		default:
			httpTC := tc
			if !strings.Contains(httpTC.Endpoint, "://") {
				httpTC.Endpoint = "http://" + httpTC.Endpoint
			}
			pg, err = newHTTPPinger(ctx, host, p.settings.TelemetrySettings, httpTC, dnsServer)
			if err != nil {
				return fmt.Errorf("target[%d] HTTP pinger: %w", i, err)
			}
		}

		targets = append(targets, &targetState{
			cfg:       tc,
			p:         pg,
			tr:        newTracerouter(p.cfg.Traceroute, tc.Endpoint, dnsServer),
			dnsServer: dnsServer,
		})
	}

	p.targets = targets
	p.started = true

	if est, budget := worstCaseCycle(p.cfg), p.cycleMaxAge(); budget > 0 && est > budget {
		p.logger.Warn("worst-case probe cycle exceeds the collection interval; targets will be skipped on cycles where probes run to their timeouts",
			zap.Duration("worst_case_cycle", est),
			zap.Duration("cycle_budget", budget),
			zap.Duration("collection_interval", p.cfg.CollectionInterval),
			zap.Int("targets", len(targets)),
			zap.Int("max_concurrent_probes", p.probeLimit()),
			zap.String("hint", skipHint),
		)
	}
	return nil
}

const skipHint = "raise max_concurrent_probes or collection_interval, or lower per-target timeouts"

func (p *sharedProber) probeLimit() int {
	if p.cfg.MaxConcurrentProbes > 0 {
		return p.cfg.MaxConcurrentProbes
	}
	return defaultMaxConcurrentProbes
}

// timeoutCeiling is the longest per-probe timeout a target may run with: 80%
// of the collection interval, a tenth of the interval short of the 90% cycle
// budget. The gap is what lets a probe that starts at the top of the cycle
// time out on its own, and be reported as down, before the cycle deadline
// cuts it and reports it as skipped. Validation applies the same ceiling to
// explicit timeouts.
func timeoutCeiling(interval time.Duration) time.Duration {
	return interval - interval/5
}

// effectiveTimeout is the per-probe timeout a target runs with: its own, or
// the prober's default for the method, clamped to timeoutCeiling. For ICMP the
// packets sent before the last one are paced icmpInterval apart and that time
// comes out of the ceiling too, so the whole probe fits. Without the clamp a
// default 10s HTTP timeout at a 10s interval would be cut by the 9s budget on
// every hung probe and reported as skipped rather than as down.
func effectiveTimeout(tc TargetConfig, interval time.Duration) time.Duration {
	d := tc.Timeout
	if d <= 0 {
		switch tc.Method {
		case MethodHTTP:
			d = defaultHTTPTimeout
		default: // icmp, dns
			d = 5 * time.Second
		}
	}
	if interval <= 0 {
		return d
	}
	ceiling := timeoutCeiling(interval)
	if tc.Method == "" || tc.Method == MethodICMP {
		count := tc.PingCount
		if count <= 0 {
			count = 3
		}
		ceiling -= time.Duration(count-1) * icmpInterval
	}
	ceiling = max(ceiling, 100*time.Millisecond)
	return min(d, ceiling)
}

// worstCaseCycle estimates how long a cycle takes when every probe in the
// batch runs to its timeout, using the timeouts the probers apply. It is the
// sizing aid behind the startup warning, not a bound the prober enforces.
func worstCaseCycle(cfg *Config) time.Duration {
	interval := cfg.CollectionInterval
	n := len(cfg.Targets)
	if cfg.BatchSize > 0 && cfg.BatchSize < n {
		n = cfg.BatchSize
	}
	if n == 0 {
		return 0
	}
	limit := cfg.MaxConcurrentProbes
	if limit <= 0 {
		limit = defaultMaxConcurrentProbes
	}
	orDefault := func(d, def time.Duration) time.Duration {
		if d > 0 {
			return d
		}
		return def
	}

	var slowest time.Duration
	for _, t := range cfg.Targets {
		var d time.Duration
		switch t.Method {
		case MethodHTTP, MethodDNS:
			d = effectiveTimeout(t, interval)
		default:
			count := t.PingCount
			if count <= 0 {
				count = 3
			}
			// The ICMP prober paces packets 200ms apart, then waits one timeout
			// for the last reply.
			d = time.Duration(count-1)*icmpInterval + effectiveTimeout(t, interval)
		}
		if d > slowest {
			slowest = d
		}
	}
	waves := (n + limit - 1) / limit
	est := time.Duration(waves) * slowest

	if tr := cfg.Traceroute; tr.Enabled {
		probes := tr.ProbesPerHop
		if probes <= 0 {
			probes = defaultProbesPerHop
		}
		maxHops := tr.MaxHops
		if maxHops <= 0 {
			maxHops = 30
		}
		silent := tr.MaxConsecutiveTimeouts
		switch {
		case silent < 0:
			silent = defaultMaxConsecutiveTimeouts
		case silent == 0:
			silent = maxHops
		}
		traceWaves := (n + maxConcurrentTraces - 1) / maxConcurrentTraces
		est += time.Duration(traceWaves) * time.Duration(probes*silent) * orDefault(tr.Timeout, 3*time.Second)
	}
	return est
}

// latestCycle returns the probe cycle that describes "now" for the caller.
// A caller that arrives while a cycle is in flight waits for it and receives
// it; a caller that arrives within maxAge of the last cycle's request receives
// that cycle; otherwise the caller runs a new cycle. Both signals of a
// receiver therefore always render the same observation, even when a cycle
// takes longer than the collection interval.
func (p *sharedProber) latestCycle(ctx context.Context, maxAge time.Duration) *probeCycle {
	arrivedAt := time.Now()

	p.mu.Lock()
	for {
		if p.inflight != nil {
			done := p.inflight
			p.mu.Unlock()
			<-done
			p.mu.Lock()
			continue
		}
		if p.last != nil && (p.last.completedAt.After(arrivedAt) || arrivedAt.Sub(p.last.requestedAt) < maxAge) {
			last := p.last
			p.mu.Unlock()
			return last
		}

		done := make(chan struct{})
		p.inflight = done
		batch := p.activeBatch()
		start := 0
		if len(batch) > 0 {
			start = p.probeStart % len(batch)
		}
		if p.cfg.BatchSize > 0 && len(p.targets) > 0 {
			p.batchOffset = (p.batchOffset + len(batch)) % len(p.targets)
		}
		p.mu.Unlock()

		cycle, firstSkipped := p.runCycle(ctx, batch, start, arrivedAt)

		p.mu.Lock()
		p.last = cycle
		p.cycles++
		p.inflight = nil
		if firstSkipped >= 0 {
			p.probeStart = (start + firstSkipped) % len(batch)
		}
		close(done)
		p.mu.Unlock()
		return cycle
	}
}

// cycleContext bounds one cycle by the scrape context, the prober's stop and
// the cycle budget measured from when the cycle was requested.
func (p *sharedProber) cycleContext(ctx context.Context, requestedAt time.Time) (context.Context, context.CancelFunc) {
	cctx, cancel := context.WithCancel(ctx)
	if p.stopped() {
		// AfterFunc below fires asynchronously, which would leave a window in
		// which a probe starts after stop; cancel synchronously instead.
		cancel()
		return cctx, cancel
	}
	unlink := context.AfterFunc(p.stopContext(), cancel)
	if budget := p.cycleMaxAge(); budget > 0 {
		var cancelDeadline context.CancelFunc
		cctx, cancelDeadline = context.WithDeadline(cctx, requestedAt.Add(budget))
		return cctx, func() { cancelDeadline(); unlink(); cancel() }
	}
	return cctx, func() { unlink(); cancel() }
}

// runCycle probes batch with bounded concurrency, starting at probe-order
// position start. It returns the cycle plus the probe-order position of the
// first target that was skipped, or -1 when every target was probed. Results
// are in batch order. Must not be called while holding p.mu.
func (p *sharedProber) runCycle(ctx context.Context, batch []*targetState, start int, requestedAt time.Time) (*probeCycle, int) {
	cctx, cancel := p.cycleContext(ctx, requestedAt)
	defer cancel()

	if p.traceSem == nil {
		// Only the cycle runner reaches here, so this cannot race.
		p.traceSem = make(chan struct{}, maxConcurrentTraces)
	}

	cycle := &probeCycle{requestedAt: requestedAt, results: make([]targetResult, len(batch))}
	for i, ts := range batch {
		cycle.results[i] = targetResult{target: ts, skipped: true}
	}

	if p.cfg.Jitter > 0 {
		delay := time.Duration(rand.Int64N(int64(p.cfg.Jitter))) // #nosec G404 -- jitter is not security-sensitive
		select {
		case <-cctx.Done():
		case <-time.After(delay):
		}
	}
	cycle.at = time.Now()

	n := len(batch)
	sem := make(chan struct{}, p.probeLimit())
	var wg sync.WaitGroup
probing:
	for k := range n {
		select {
		case <-cctx.Done():
			break probing
		case sem <- struct{}{}:
		}
		i := (start + k) % n
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			cycle.results[i] = p.probeTarget(cctx, batch[i])
		}()
	}
	wg.Wait()
	cycle.completedAt = time.Now()

	firstSkipped, skipped := -1, 0
	for k := range n {
		if cycle.results[(start+k)%n].skipped {
			skipped++
			if firstSkipped < 0 {
				firstSkipped = k
			}
		}
	}
	if skipped > 0 && !p.stopped() && time.Since(p.lastSkipWarn) > time.Minute {
		p.lastSkipWarn = time.Now()
		p.logger.Warn("probe cycle hit its deadline; some targets were skipped this cycle",
			zap.Int("skipped", skipped),
			zap.Int("targets", n),
			zap.Duration("cycle_budget", p.cycleMaxAge()),
			zap.String("hint", skipHint),
		)
	}
	return cycle, firstSkipped
}

// probeTarget runs one target's ping and, when due, its traceroute. A probe
// that is still running when ctx ends is reported as skipped rather than as a
// failure: the target did not fail, the cycle ran out of time.
func (p *sharedProber) probeTarget(ctx context.Context, ts *targetState) targetResult {
	tr := targetResult{target: ts, startedAt: time.Now()}
	if ctx.Err() != nil {
		tr.skipped = true
		return tr
	}

	tr.ping, tr.pingErr = ts.p.ping(ctx)
	if ctx.Err() != nil {
		return targetResult{target: ts, startedAt: tr.startedAt, skipped: true}
	}
	if tr.pingErr != nil {
		return tr
	}

	ts.checkCount++
	if !ts.tr.shouldRun(ts.checkCount, tr.ping) {
		return tr
	}
	select {
	case <-ctx.Done():
		// The ping stands; only the trace is dropped for this cycle.
		return tr
	case p.traceSem <- struct{}{}:
	}
	trace, traceErr := ts.tr.trace(ctx)
	<-p.traceSem
	if ctx.Err() != nil {
		// A trace cut off at the deadline is a partial path, not a measurement.
		return tr
	}
	tr.traced = true
	tr.trace, tr.traceErr = trace, traceErr
	return tr
}

// activeBatch returns the slice of targets to probe this cycle. Caller must
// hold p.mu.
func (p *sharedProber) activeBatch() []*targetState {
	size := p.cfg.BatchSize
	if size <= 0 || size >= len(p.targets) {
		return p.targets
	}

	batch := make([]*targetState, 0, size)
	for i := 0; i < size; i++ {
		batch = append(batch, p.targets[(p.batchOffset+i)%len(p.targets)])
	}
	return batch
}

// cycleMaxAge is both how stale a cached cycle may be before a caller triggers
// a new one and the time budget of a cycle. Slightly under the collection
// interval so two signals ticking on the same schedule share a cycle, while a
// single signal still probes every tick and a cycle always ends before the
// next tick.
func (p *sharedProber) cycleMaxAge() time.Duration {
	interval := p.cfg.CollectionInterval
	if interval <= 0 {
		return 0
	}
	return interval - interval/10
}
