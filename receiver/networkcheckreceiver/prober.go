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
	"math/rand/v2"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.uber.org/zap"
)

// target is one configured host. trace is its tracerouter's trace method;
// tests swap in stubs to drive the cycle machinery without sending packets.
type target struct {
	host      string
	dnsServer string
	trace     func(ctx context.Context) (TraceResult, error)
}

// targetResult is one target's outcome for a single cycle.
type targetResult struct {
	target *target

	// startedAt is when the trace began, the timestamp of its telemetry.
	startedAt time.Time

	trace TraceResult
	err   error

	// skipped is true when the trace did not complete: the cycle hit its
	// deadline or the prober was stopped first. A skipped target emits
	// nothing for the cycle, as opposed to a failed trace, which emits
	// traceroute.reached 0.
	skipped bool
}

// traceCycle is one pass over the active batch of targets. Both the metrics and
// the logs signal render the same cycle, so they always describe the same
// traces.
type traceCycle struct {
	// requestedAt is when the cycle was asked for, before any jitter delay.
	// Freshness is measured from this: charging the jitter wait against the
	// freshness budget would let a jitter above 10% of the collection interval
	// make the cached cycle look fresh at the next tick, so no trace would run
	// and the previous results would be emitted again.
	requestedAt time.Time

	// completedAt is when the last trace returned. A caller that arrived before
	// this was waiting on the cycle while it ran, so the cycle is that caller's
	// observation too. That is what keeps two signals on one cycle when a
	// cycle runs longer than the freshness budget.
	completedAt time.Time

	// results is in target order regardless of the order traces ran in.
	results []targetResult
}

// sharedProber owns the tracing state for one receiver ID. A receiver wired
// into both a metrics and a logs pipeline is instantiated twice by the
// collector; without sharing, every target would be traced twice. Instances
// are refcounted through acquireProber and released on shutdown.
type sharedProber struct {
	cfg    *TracerouteConfig
	logger *zap.Logger

	// sem bounds the traces in flight, scheduled and triggered together.
	sem chan struct{}

	// stopCtx is cancelled by stop, which the receiver wrappers call first
	// thing in Shutdown. Every trace runs under it, so shutdown cuts an
	// in-flight cycle short instead of waiting for its slowest target.
	stopCtx context.Context
	stopFn  context.CancelFunc

	mu          sync.Mutex
	started     bool
	targets     []*target
	batchOffset int

	// traceStart rotates the order targets are traced in within a batch, so
	// targets skipped at a cycle deadline go first on the next cycle instead
	// of being the permanent tail that is never traced.
	traceStart int

	last *traceCycle

	// inflight is closed when the running cycle completes; nil when idle.
	inflight chan struct{}

	// cycles counts completed cycles. Tests assert on it to prove two signals
	// share one cycle rather than tracing independently.
	cycles int

	// logs is the logs pipeline of the receiver's logs instance, nil when
	// the receiver is not in a logs pipeline. Triggered traces, which run in
	// the metrics instance, send their log records to it.
	logs consumer.Logs

	// lastSkipWarn rate-limits the deadline warning; only the cycle runner
	// touches it.
	lastSkipWarn time.Time
}

func newSharedProber(cfg *TracerouteConfig, logger *zap.Logger) *sharedProber {
	ctx, cancel := context.WithCancel(context.Background())
	p := &sharedProber{cfg: cfg, logger: logger, stopCtx: ctx, stopFn: cancel, sem: make(chan struct{}, traceLimit(cfg))}
	if !cfg.Schedule {
		return p
	}
	for _, tc := range cfg.Targets {
		p.targets = append(p.targets, &target{
			host:      tc.Host,
			dnsServer: tc.DNSServer,
			trace:     newTracerouter(cfg, tc.Host, tc.DNSServer).trace,
		})
	}
	return p
}

// stop cancels the in-flight cycle, if any, and makes every later cycle return
// immediately with all targets skipped. Idempotent.
func (p *sharedProber) stop() {
	p.stopFn()
}

func (p *sharedProber) stopped() bool {
	return p.stopCtx.Err() != nil
}

// setLogs attaches or, with nil, detaches the logs pipeline.
func (p *sharedProber) setLogs(c consumer.Logs) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.logs = c
}

func (p *sharedProber) logsConsumer() consumer.Logs {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.logs
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
func acquireProber(id component.ID, cfg *TracerouteConfig, logger *zap.Logger) *sharedProber {
	proberRegistryMu.Lock()
	defer proberRegistryMu.Unlock()

	if e, ok := proberRegistry[id]; ok {
		if e.prober.cfg == cfg && !e.prober.stopped() {
			e.refs++
			return e.prober
		}
		// A different Config under the same ID means the entry is a leftover:
		// a receiver that was built but never started (the collector abandons
		// components when the service fails to build) or one already stopped.
		// Reusing it would trace the old targets, so replace it unless it is
		// live.
		e.prober.mu.Lock()
		live := e.prober.started && !e.prober.stopped()
		e.prober.mu.Unlock()
		if live {
			logger.Warn("receiver already running under this ID with a different configuration; sharing its prober",
				zap.Stringer("id", id))
			e.refs++
			return e.prober
		}
	}
	p := newSharedProber(cfg, logger)
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

// start marks the prober live and, once per prober, warns when the worst-case
// cycle does not fit the budget. Safe to call once per signal.
func (p *sharedProber) start() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.started {
		return
	}
	p.started = true

	if of := p.cfg.OnFailure; of.Enabled && worstCaseTrace(p.cfg) > of.Timeout {
		p.logger.Warn("a triggered trace into a path that stops answering cannot finish within on_failure.timeout and will emit nothing",
			zap.Duration("worst_case_trace", worstCaseTrace(p.cfg)),
			zap.Duration("on_failure_timeout", of.Timeout),
			zap.String("hint", "raise on_failure.timeout, or lower timeout, probes_per_hop or max_consecutive_timeouts"),
		)
	}
	if est, budget := worstCaseCycle(p.cfg), p.cycleMaxAge(); budget > 0 && est > budget {
		p.logger.Warn("worst-case trace cycle exceeds the collection interval; targets will be skipped on cycles where traces run into silent hops",
			zap.Duration("worst_case_cycle", est),
			zap.Duration("cycle_budget", budget),
			zap.Duration("collection_interval", p.cfg.CollectionInterval),
			zap.Int("targets", len(p.targets)),
			zap.Int("max_concurrent_traces", traceLimit(p.cfg)),
			zap.String("hint", skipHint),
		)
	}
}

const skipHint = "raise max_concurrent_traces or collection_interval, or lower timeout, probes_per_hop or max_consecutive_timeouts"

func traceLimit(cfg *TracerouteConfig) int {
	if cfg.MaxConcurrentTraces > 0 {
		return cfg.MaxConcurrentTraces
	}
	return defaultMaxConcurrentTraces
}

// worstCaseCycle estimates how long a cycle takes when every trace in the batch
// runs into a path that stops answering: each costs probes_per_hop × timeout
// for every silent hop up to the early abort, or up to max_hops when the abort
// is disabled. It is the sizing aid behind the startup warning, not a bound the
// prober enforces.
func worstCaseCycle(cfg *TracerouteConfig) time.Duration {
	n := len(cfg.Targets)
	if !cfg.Schedule {
		n = 0
	}
	if cfg.BatchSize > 0 && cfg.BatchSize < n {
		n = cfg.BatchSize
	}
	if n == 0 {
		return 0
	}
	limit := traceLimit(cfg)
	waves := (n + limit - 1) / limit
	// The jitter delay is charged against the same budget.
	return time.Duration(waves)*worstCaseTrace(cfg) + cfg.Jitter
}

// worstCaseTrace is how long one trace takes when the path stops answering:
// probes_per_hop × timeout for every silent hop up to the early abort, or up
// to max_hops when the abort is disabled.
func worstCaseTrace(cfg *TracerouteConfig) time.Duration {
	t := newTracerouter(cfg, "", "")
	silent := t.abortAfter()
	if silent == 0 || silent > t.maxHops() {
		silent = t.maxHops()
	}
	return time.Duration(t.probesPerHop()*silent) * t.hopTimeout()
}

// latestCycle returns the cycle that describes "now" for the caller. A caller
// that arrives while a cycle is in flight waits for it and receives it; a
// caller that arrives within maxAge of the last cycle's request receives that
// cycle; otherwise the caller runs a new cycle. Both signals of a receiver
// therefore always render the same traces, even when a cycle takes longer
// than the collection interval.
func (p *sharedProber) latestCycle(ctx context.Context, maxAge time.Duration) *traceCycle {
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
		windowed := p.cfg.BatchSize > 0 && p.cfg.BatchSize < len(p.targets)
		start := 0
		if !windowed && len(batch) > 0 {
			start = p.traceStart % len(batch)
		}
		p.mu.Unlock()

		cycle, firstSkipped := p.runCycle(ctx, batch, start, arrivedAt)

		p.mu.Lock()
		p.last = cycle
		p.cycles++
		p.inflight = nil
		switch {
		case windowed:
			// The window advances past the targets that were traced, so a
			// skipped tail is the head of the next window instead of waiting
			// for the window to come round again.
			advance := len(batch)
			if firstSkipped >= 0 {
				advance = firstSkipped
			}
			p.batchOffset = (p.batchOffset + advance) % len(p.targets)
		case firstSkipped >= 0:
			p.traceStart = (start + firstSkipped) % len(batch)
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
		// which a trace starts after stop; cancel synchronously instead.
		cancel()
		return cctx, cancel
	}
	unlink := context.AfterFunc(p.stopCtx, cancel)
	if budget := p.cycleMaxAge(); budget > 0 {
		var cancelDeadline context.CancelFunc
		cctx, cancelDeadline = context.WithDeadline(cctx, requestedAt.Add(budget))
		return cctx, func() { cancelDeadline(); unlink(); cancel() }
	}
	return cctx, func() { unlink(); cancel() }
}

// runCycle traces batch with bounded concurrency, starting at trace-order
// position start. It returns the cycle plus the trace-order position of the
// first target that was skipped, or -1 when every target completed. Results
// are in batch order. Must not be called while holding p.mu.
func (p *sharedProber) runCycle(ctx context.Context, batch []*target, start int, requestedAt time.Time) (*traceCycle, int) {
	cctx, cancel := p.cycleContext(ctx, requestedAt)
	defer cancel()

	cycle := &traceCycle{requestedAt: requestedAt, results: make([]targetResult, len(batch))}
	for i, t := range batch {
		cycle.results[i] = targetResult{target: t, skipped: true}
	}

	if p.cfg.Jitter > 0 {
		delay := time.Duration(rand.Int64N(int64(p.cfg.Jitter))) // #nosec G404 -- jitter is not security-sensitive
		timer := time.NewTimer(delay)
		select {
		case <-cctx.Done():
			timer.Stop()
		case <-timer.C:
		}
	}

	n := len(batch)
	var wg sync.WaitGroup
tracing:
	for k := range n {
		select {
		case <-cctx.Done():
			break tracing
		case p.sem <- struct{}{}:
		}
		i := (start + k) % n
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-p.sem }()
			cycle.results[i] = traceTarget(cctx, batch[i])
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
		p.logger.Warn("trace cycle hit its deadline; some targets were skipped this cycle",
			zap.Int("skipped", skipped),
			zap.Int("targets", n),
			zap.Duration("cycle_budget", p.cycleMaxAge()),
			zap.String("hint", skipHint),
		)
	}
	return cycle, firstSkipped
}

// traceTarget runs one target's trace. A trace still running when ctx ends is
// reported as skipped rather than as a result: a path cut off at the deadline
// is partial, not a measurement, and the target did not fail.
func traceTarget(ctx context.Context, t *target) targetResult {
	res := targetResult{target: t, startedAt: time.Now()}
	if ctx.Err() != nil {
		res.skipped = true
		return res
	}
	res.trace, res.err = t.trace(ctx)
	// The walk stops on the wall clock, which can be a moment before the
	// context's own timer fires, so the deadline is checked the same way.
	if ctxDone(ctx) {
		return targetResult{target: t, startedAt: res.startedAt, skipped: true}
	}
	return res
}

// activeBatch returns the targets to trace this cycle. Caller must hold p.mu.
func (p *sharedProber) activeBatch() []*target {
	size := p.cfg.BatchSize
	if size <= 0 || size >= len(p.targets) {
		return p.targets
	}

	batch := make([]*target, 0, size)
	for i := range size {
		batch = append(batch, p.targets[(p.batchOffset+i)%len(p.targets)])
	}
	return batch
}

// cycleMaxAge is both how stale a cached cycle may be before a caller triggers
// a new one and the time budget of a cycle. Slightly under the collection
// interval so two signals ticking on the same schedule share a cycle, while a
// single signal still traces every tick and a cycle always ends before the
// next tick.
func (p *sharedProber) cycleMaxAge() time.Duration {
	interval := p.cfg.CollectionInterval
	if interval <= 0 {
		return 0
	}
	return interval - interval/10
}
