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
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper/scrapererror"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver/internal/metadata"
)

// traceScraper renders scheduled cycles of the receiver's shared prober as
// metrics or as logs; each signal gets its own instance.
type traceScraper struct {
	prober *sharedProber
	mb     *metadata.MetricsBuilder
	lb     *metadata.LogsBuilder
	rb     *metadata.ResourceBuilder
}

func newTraceScraper(settings receiver.Settings, cfg *TracerouteConfig, p *sharedProber) *traceScraper {
	return &traceScraper{
		prober: p,
		mb:     metadata.NewMetricsBuilder(cfg.MetricsBuilderConfig, settings),
		lb:     metadata.NewLogsBuilder(settings),
		rb:     metadata.NewResourceBuilder(cfg.ResourceAttributes),
	}
}

func (s *traceScraper) scrapeMetrics(ctx context.Context) (pmetric.Metrics, error) {
	cycle := s.prober.latestCycle(ctx, s.prober.cycleMaxAge())
	for _, res := range cycle.results {
		// A skipped target never completed a trace; emitting reached 0 for it
		// would turn a busy cycle into a false outage.
		if !res.skipped {
			recordTraceMetrics(s.mb, s.rb, res, metadata.AttributeTracerouteTriggerScheduled)
		}
	}
	return s.mb.Emit(), cycleError(cycle)
}

func (s *traceScraper) scrapeLogs(ctx context.Context) (plog.Logs, error) {
	cycle := s.prober.latestCycle(ctx, s.prober.cycleMaxAge())
	observed := time.Now()
	for _, res := range cycle.results {
		if !res.skipped {
			appendTraceLog(s.lb, s.rb, res, metadata.AttributeTracerouteTriggerScheduled, observed)
		}
	}
	return s.lb.Emit(), cycleError(cycle)
}

// recordTraceMetrics records one completed trace under its own resource,
// stamped with the trace's start.
func recordTraceMetrics(mb *metadata.MetricsBuilder, rb *metadata.ResourceBuilder, res targetResult, trigger metadata.AttributeTracerouteTrigger) {
	t := res.target
	now := pcommon.NewTimestampFromTime(res.startedAt)

	// A trace that could not run reports reached 0 and no hops; its partial
	// path, if any, is not a measurement.
	var reached, hops int64
	if res.err == nil {
		hops = int64(len(res.trace.Hops))
		if res.trace.Reached {
			reached = 1
		}
		for _, hop := range res.trace.Hops {
			// Double, so an average over time is the fraction of traces in
			// which the hop answered rather than a truncated integer.
			status := 1.0
			if hop.TimedOut {
				status = 0
			}
			mb.RecordTracerouteHopStatusDataPoint(now, status, int64(hop.Index), hop.Address, trigger, t.dnsServer)
			// A silent hop's only duration is the timeout we waited out.
			if !hop.TimedOut {
				mb.RecordTracerouteHopLatencyDataPoint(now, msFloat(hop.RTT), int64(hop.Index), hop.Address, trigger, t.dnsServer)
			}
		}
	}
	mb.RecordTracerouteReachedDataPoint(now, reached, trigger, t.dnsServer)
	mb.RecordTracerouteHopsDataPoint(now, hops, trigger, t.dnsServer)

	rb.SetServerAddress(t.host)
	mb.EmitForResource(metadata.WithResource(rb.Emit()))
}

// maxNamedFailures is how many failures a scrape error spells out.
const maxNamedFailures = 3

// cycleError folds a cycle's failed traces into a single partial scrape
// error. The scraper controller logs every scrape error at ERROR on every
// interval, for each signal, so one error per failing target would flood the
// log. Only the first few are named; the per-target outcome is in
// traceroute.reached.
func cycleError(cycle *traceCycle) error {
	var failures []string
	traced := 0
	for _, res := range cycle.results {
		if res.skipped {
			continue
		}
		traced++
		if res.err != nil {
			failures = append(failures, fmt.Sprintf("traceroute %s: %v", res.target.host, res.err))
		}
	}
	n := len(failures)
	if n == 0 {
		return nil
	}
	msg := fmt.Sprintf("%d of %d targets failed: %s", n, traced, strings.Join(failures[:min(n, maxNamedFailures)], "; "))
	if n > maxNamedFailures {
		msg += fmt.Sprintf(" (+%d more)", n-maxNamedFailures)
	}
	return scrapererror.NewPartialScrapeError(errors.New(msg), n)
}

// msFloat converts a duration to fractional milliseconds, keeping the sub-ms
// precision that time.Duration.Milliseconds() truncates to zero.
func msFloat(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / 1e6
}
