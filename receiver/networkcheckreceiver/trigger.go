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
	"sync"
	"time"

	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver/internal/metadata"
)

// What the icmp section reports, as icmp_check names it.
const (
	pingLossMetric = "ping.loss.ratio"
	peerNameAttr   = "net.peer.name"
	peerIPAttr     = "net.peer.ip"
)

// triggerConsumer sits between the icmp section and the metrics pipeline. It
// forwards every batch unchanged and starts a traceroute to a host whose
// ping.loss.ratio reaches on_failure.loss_threshold: on the first failing
// check, then on every retrace_every-th failing check while the host keeps
// failing. A passing check resets the count.
type triggerConsumer struct {
	next     consumer.Metrics
	prober   *sharedProber
	cfg      *TracerouteConfig
	settings receiver.Settings

	// newTrace returns the trace function for a destination; tests swap it.
	newTrace func(dest string) func(context.Context) (TraceResult, error)

	mu     sync.Mutex
	hosts  map[string]*hostState
	closed bool
	wg     sync.WaitGroup

	// warnedFull rate-limits the max_hosts warning to once per receiver.
	warnedFull bool

	// lastSlotWarn rate-limits the no-slot warning to once a minute: under a
	// storm of failing hosts every one of them would otherwise log it.
	lastSlotWarn time.Time
}

// warnNoSlot reports a triggered trace given up because no trace slot came
// free within on_failure.timeout, at most once a minute.
func (t *triggerConsumer) warnNoSlot(host string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if time.Since(t.lastSlotWarn) < time.Minute {
		return
	}
	t.lastSlotWarn = time.Now()
	t.settings.Logger.Warn("no trace slot came free within on_failure.timeout; triggered traceroutes are being skipped",
		zap.String("host", host), zap.Int("max_concurrent_traces", t.cfg.MaxConcurrentTraces),
		zap.Duration("timeout", t.cfg.OnFailure.Timeout))
}

// hostState is a failing host. It is dropped when the host passes and no
// trace is in flight for it, so hosts holds only failing hosts.
type hostState struct {
	streak   int
	inflight bool
}

func newTriggerConsumer(next consumer.Metrics, p *sharedProber, cfg *TracerouteConfig, settings receiver.Settings) *triggerConsumer {
	return &triggerConsumer{
		next:     next,
		prober:   p,
		cfg:      cfg,
		settings: settings,
		hosts:    map[string]*hostState{},
		newTrace: func(dest string) func(context.Context) (TraceResult, error) {
			return newTracerouter(cfg, dest, "").trace
		},
	}
}

func (t *triggerConsumer) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// ConsumeMetrics reads the batch before handing it on: once forwarded, the
// pipeline owns it.
func (t *triggerConsumer) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	rms := md.ResourceMetrics()
	for i := range rms.Len() {
		rm := rms.At(i)
		attrs := rm.Resource().Attributes()
		name, ok := attrs.Get(peerNameAttr)
		if !ok || name.Str() == "" {
			continue
		}
		if loss, ok := lossPercent(rm); ok {
			ip, _ := attrs.Get(peerIPAttr)
			t.observe(name.Str(), ip.Str(), loss >= t.cfg.OnFailure.LossThreshold)
		}
	}
	return t.next.ConsumeMetrics(ctx, md)
}

// lossPercent returns the last ping.loss.ratio data point of a resource.
func lossPercent(rm pmetric.ResourceMetrics) (float64, bool) {
	var loss float64
	found := false
	sms := rm.ScopeMetrics()
	for i := range sms.Len() {
		ms := sms.At(i).Metrics()
		for j := range ms.Len() {
			m := ms.At(j)
			if m.Name() != pingLossMetric || m.Type() != pmetric.MetricTypeGauge {
				continue
			}
			dps := m.Gauge().DataPoints()
			for k := range dps.Len() {
				dp := dps.At(k)
				switch dp.ValueType() {
				case pmetric.NumberDataPointValueTypeDouble:
					loss, found = dp.DoubleValue(), true
				case pmetric.NumberDataPointValueTypeInt:
					loss, found = float64(dp.IntValue()), true
				}
			}
		}
	}
	return loss, found
}

// observe updates host's streak and starts a trace when one is due.
func (t *triggerConsumer) observe(host, ip string, failing bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed {
		return
	}
	st := t.hosts[host]
	if !failing {
		if st != nil {
			st.streak = 0
			if !st.inflight {
				delete(t.hosts, host)
			}
		}
		return
	}
	if st == nil {
		if len(t.hosts) >= t.cfg.OnFailure.MaxHosts {
			if !t.warnedFull {
				t.warnedFull = true
				t.settings.Logger.Warn("on_failure is tracking max_hosts failing hosts; further failing hosts are not traced until one recovers",
					zap.Int("max_hosts", t.cfg.OnFailure.MaxHosts), zap.String("host", host))
			}
			return
		}
		st = &hostState{}
		t.hosts[host] = st
	}
	st.streak++
	if (st.streak-1)%t.cfg.OnFailure.RetraceEvery != 0 {
		return
	}
	if st.inflight {
		t.settings.Logger.Debug("traceroute for failing host already in flight", zap.String("host", host))
		return
	}
	st.inflight = true
	t.wg.Add(1)
	go t.run(host, ip, st)
}

// run traces host and emits the result. It waits for a trace slot, shared
// with the scheduled traces, within on_failure.timeout.
func (t *triggerConsumer) run(host, ip string, st *hostState) {
	defer t.wg.Done()
	defer func() {
		t.mu.Lock()
		st.inflight = false
		if st.streak == 0 && t.hosts[host] == st {
			delete(t.hosts, host)
		}
		t.mu.Unlock()
	}()

	logger := t.settings.Logger.With(zap.String("host", host))
	ctx, cancel := context.WithTimeout(t.prober.stopCtx, t.cfg.OnFailure.Timeout)
	defer cancel()
	select {
	case t.prober.sem <- struct{}{}:
	case <-ctx.Done():
		if !t.prober.stopped() {
			t.warnNoSlot(host)
		}
		return
	}
	defer func() { <-t.prober.sem }()
	// The slot and the deadline can arrive together; a trace with no time
	// left would only log that it did not finish.
	if ctx.Err() != nil {
		if !t.prober.stopped() {
			t.warnNoSlot(host)
		}
		return
	}

	// The address icmp_check pinged, so the trace follows the same path even
	// when the name resolves to several addresses.
	dest := ip
	if dest == "" {
		dest = host
	}
	logger.Debug("ping failed; tracing", zap.String("dest", dest))
	res := traceTarget(ctx, &target{host: host, trace: t.newTrace(dest)})
	switch {
	case res.skipped:
		if !t.prober.stopped() {
			logger.Warn("triggered traceroute did not finish within on_failure.timeout; nothing emitted",
				zap.Duration("timeout", t.cfg.OnFailure.Timeout))
		}
		return
	case res.err != nil:
		logger.Warn("triggered traceroute failed", zap.Error(res.err))
	}
	t.emit(res)
}

// emit sends one triggered trace to the metrics pipeline and, when the
// receiver is also in a logs pipeline, to the logs pipeline. It builds fresh
// builders: emits run concurrently and the builders are not safe for that.
func (t *triggerConsumer) emit(res targetResult) {
	trigger := metadata.AttributeTracerouteTriggerPingFailure
	mb := metadata.NewMetricsBuilder(t.cfg.MetricsBuilderConfig, t.settings)
	recordTraceMetrics(mb, metadata.NewResourceBuilder(t.cfg.ResourceAttributes), res, trigger)
	// The trace is bounded by the stop context; the hand-off is not, so a
	// trace that finished is delivered even while the receiver stops.
	ctx := context.Background()
	if err := t.next.ConsumeMetrics(ctx, mb.Emit()); err != nil {
		t.settings.Logger.Warn("sending triggered traceroute metrics", zap.Error(err))
	}
	if logs := t.prober.logsConsumer(); logs != nil && res.err == nil {
		lb := metadata.NewLogsBuilder(t.settings)
		appendTraceLog(lb, metadata.NewResourceBuilder(t.cfg.ResourceAttributes), res, trigger, time.Now())
		if err := logs.ConsumeLogs(ctx, lb.Emit()); err != nil {
			t.settings.Logger.Warn("sending triggered traceroute log", zap.Error(err))
		}
	}
}

// close stops new triggered traces and waits for those in flight, which end
// promptly once the prober is stopped.
func (t *triggerConsumer) close() {
	t.mu.Lock()
	t.closed = true
	t.mu.Unlock()
	t.wg.Wait()
}
