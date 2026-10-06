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

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper/scrapererror"
	"go.uber.org/zap"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver/internal/metadata"
)

// targetState holds runtime state for a single probe target.
type targetState struct {
	cfg        TargetConfig
	p          pinger
	tr         *tracerouter
	checkCount int
	dnsServer  string
}

type networkCheckScraper struct {
	cfg      *Config
	settings receiver.Settings
	logger   *zap.Logger
	mb       *metadata.MetricsBuilder
	rb       *metadata.ResourceBuilder

	// prober owns probe execution and is shared with the logs signal when the
	// same receiver ID appears in both a metrics and a logs pipeline.
	prober *sharedProber
	id     component.ID
}

func newNetworkCheckScraper(settings receiver.Settings, cfg *Config) *networkCheckScraper {
	return &networkCheckScraper{
		cfg:      cfg,
		settings: settings,
		logger:   settings.Logger,
		id:       settings.ID,
		prober:   acquireProber(settings.ID, cfg, settings),
	}
}

func (s *networkCheckScraper) start(ctx context.Context, host component.Host) error {
	s.mb = metadata.NewMetricsBuilder(s.cfg.MetricsBuilderConfig, s.settings)
	s.rb = metadata.NewResourceBuilder(s.cfg.MetricsBuilderConfig.ResourceAttributes)
	return s.prober.start(ctx, host)
}

func (s *networkCheckScraper) shutdown(_ context.Context) error {
	releaseProber(s.id)
	return nil
}

func (s *networkCheckScraper) scrape(ctx context.Context) (pmetric.Metrics, error) {
	return s.render(s.prober.latestCycle(ctx, s.prober.cycleMaxAge()))
}

// render turns one probe cycle into metrics.
func (s *networkCheckScraper) render(cycle *probeCycle) (pmetric.Metrics, error) {
	var failures []string
	probed := 0

	for _, res := range cycle.results {
		// A skipped probe never ran, so there is nothing to report: emitting a
		// status here would turn a busy cycle into a false outage.
		if res.skipped {
			continue
		}
		probed++

		ts := res.target
		// Redacted unconditionally: the resource attribute is attached to every
		// data point, and a target may be configured as https://user:pass@host.
		// The failure text below redacts for the same reason.
		endpoint := redactEndpoint(ts.cfg.Endpoint)
		if res.pingErr != nil {
			failures = append(failures, fmt.Sprintf("ping %s: %v", endpoint, redactErr(res.pingErr)))
			continue
		}

		// Each target is stamped with when its own probe began. Probes run
		// sequentially or in parallel across a cycle that can last tens of
		// seconds, so one cycle-wide timestamp misplaces every target but the
		// first.
		now := pcommon.NewTimestampFromTime(res.startedAt)
		traceNow := pcommon.NewTimestampFromTime(res.traceTime())

		s.rb.SetTargetEndpoint(endpoint)
		s.recordMetrics(now, ts, res.ping)

		if res.traceErr != nil {
			failures = append(failures, fmt.Sprintf("traceroute %s: %v", endpoint, redactErr(res.traceErr)))
		}
		if res.traced {
			for _, hop := range res.trace.Hops {
				// Status is reported for every probed hop, answered or not, so
				// that a hop going dark is visible as a 0 rather than as an
				// absent series indistinguishable from "traceroute never ran".
				// Double rather than int: when configured attributes drop
				// hop.index, several hops reaggregate into one point, and
				// integer arithmetic would truncate a mixed 1 and 0 to 0 —
				// making a partly responsive path read as fully dead.
				status := float64(1)
				if hop.TimedOut {
					status = 0
				}
				s.mb.RecordNetworkTracerouteHopStatusDataPoint(
					traceNow,
					status,
					int64(hop.Index),
					hop.Address,
					ts.dnsServer,
				)

				// A hop that never answered has no latency to report; its RTT
				// is just the probe timeout. Emitting it would look like a
				// real (and very slow) measurement.
				if hop.TimedOut {
					continue
				}
				s.mb.RecordNetworkTracerouteHopLatencyDataPoint(
					traceNow,
					msFloat(hop.RTT),
					int64(hop.Index),
					hop.Address,
					ts.dnsServer,
				)
			}
		}

		s.mb.EmitForResource(metadata.WithResource(s.rb.Emit()))
	}

	return s.mb.Emit(), summarizeFailures(failures, probed)
}

// maxNamedFailures is how many failures a scrape error spells out.
const maxNamedFailures = 3

// summarizeFailures folds a cycle's per-target failures into a single partial
// scrape error. The scraper controller logs every scrape error at ERROR on
// every interval, for each signal; one error per failing target turned a few
// hundred unreachable targets into a log flood that buried everything else.
//
// Deliberate simplification: only the first few failures are named. Per-target
// outcomes are already in the telemetry (status series, log records), so the
// error points at the problem rather than enumerating it. Raise
// maxNamedFailures if operators need more without opening the data.
func summarizeFailures(failures []string, total int) error {
	n := len(failures)
	if n == 0 {
		return nil
	}
	msg := fmt.Sprintf("%d of %d targets failed: %s", n, total, strings.Join(failures[:min(n, maxNamedFailures)], "; "))
	if n > maxNamedFailures {
		msg += fmt.Sprintf(" (+%d more)", n-maxNamedFailures)
	}
	return scrapererror.NewPartialScrapeError(errors.New(msg), n)
}

// recordMetrics writes data points for one completed probe cycle.
func (s *networkCheckScraper) recordMetrics(now pcommon.Timestamp, ts *targetState, r PingResult) {
	// A probe that failed has no timings to report: the only duration available
	// is how long we waited before giving up, and the per-phase timers never
	// fired at all. Publishing those would put the configured timeout into the
	// latency series as though it were a measurement, and report 0ms for phases
	// that never ran. Only the status metric is emitted on failure, which
	// already expresses the outcome; the timing series shows a gap instead.
	dns := ts.dnsServer
	switch r.Method {
	case MethodDNS:
		status := int64(0)
		if r.QuerySuccess {
			status = 1
		}
		s.mb.RecordNetworkDNSStatusDataPoint(now, status, r.QueryName)
		if !r.QuerySuccess {
			return
		}
		s.mb.RecordNetworkDNSLookupDurationDataPoint(now, msFloat(r.QueryDuration), r.QueryName)
	case MethodICMP:
		m := metadata.AttributePingMethodIcmp
		s.mb.RecordNetworkPingPacketLossDataPoint(now, r.PacketLoss, m, dns)
		if r.PacketLoss >= 1.0 {
			// Nothing came back, so min/avg/max are zero values rather than
			// round-trip times. Packet loss of 1.0 is the failure signal.
			return
		}
		s.mb.RecordNetworkPingLatencyMinDataPoint(now, msFloat(r.MinRTT), m, dns)
		s.mb.RecordNetworkPingLatencyAvgDataPoint(now, msFloat(r.AvgRTT), m, dns)
		s.mb.RecordNetworkPingLatencyMaxDataPoint(now, msFloat(r.MaxRTT), m, dns)
	case MethodHTTP:
		code := int64(r.StatusCode)
		up := int64(0)
		if r.StatusCode > 0 {
			up = 1
		}
		s.mb.RecordNetworkHTTPStatusDataPoint(now, up, code, dns)
		if up == 0 {
			return
		}
		s.mb.RecordNetworkHTTPDurationDataPoint(now, msFloat(r.TotalDuration), code, dns)
		s.mb.RecordNetworkHTTPDNSLookupDurationDataPoint(now, msFloat(r.DNSLookup), dns)
		s.mb.RecordNetworkHTTPClientConnectionDurationDataPoint(now, msFloat(r.TCPConnect), dns)
		s.mb.RecordNetworkHTTPTLSHandshakeDurationDataPoint(now, msFloat(r.TLSHandshake), dns)
		s.mb.RecordNetworkHTTPRequestDurationDataPoint(now, msFloat(r.RequestWrite), dns)
		s.mb.RecordNetworkHTTPResponseDurationDataPoint(now, msFloat(r.ResponseRead), dns)
	}
}

// msFloat converts a duration to fractional milliseconds, preserving sub-ms
// precision that time.Duration.Milliseconds() truncates to zero.
func msFloat(d time.Duration) float64 {
	return float64(d.Nanoseconds()) / 1e6
}

// detectSystemDNS is implemented per platform: see systemdns_other.go and
// systemdns_windows.go.
