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
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver/internal/metadata"
)

// networkCheckLogsScraper renders probe cycles as log records. It shares the
// prober with the metrics scraper, so wiring the same receiver into both a
// metrics and a logs pipeline probes each target once, not twice.
type networkCheckLogsScraper struct {
	cfg      *Config
	settings receiver.Settings
	id       component.ID

	lb *metadata.LogsBuilder
	rb *metadata.ResourceBuilder

	prober *sharedProber
}

func newNetworkCheckLogsScraper(settings receiver.Settings, cfg *Config) *networkCheckLogsScraper {
	return &networkCheckLogsScraper{
		cfg:      cfg,
		settings: settings,
		id:       settings.ID,
		prober:   acquireProber(settings.ID, cfg, settings),
	}
}

func (s *networkCheckLogsScraper) start(ctx context.Context, host component.Host) error {
	s.lb = metadata.NewLogsBuilder(s.settings)
	s.rb = metadata.NewResourceBuilder(s.cfg.MetricsBuilderConfig.ResourceAttributes)
	return s.prober.start(ctx, host)
}

func (s *networkCheckLogsScraper) shutdown(_ context.Context) error {
	releaseProber(s.id)
	return nil
}

// scrape renders the latest probe cycle as logs.
func (s *networkCheckLogsScraper) scrape(ctx context.Context) (plog.Logs, error) {
	return s.render(s.prober.latestCycle(ctx, s.prober.cycleMaxAge()))
}

// render turns one probe cycle into log records. Only HTTP probes and
// traceroutes produce records: a DNS or ICMP probe is a scalar sampled on an
// interval, which is a metric, not an event.
func (s *networkCheckLogsScraper) render(cycle *probeCycle) (plog.Logs, error) {
	var failures []string
	probed := 0
	observed := time.Now()

	for _, res := range cycle.results {
		// A skipped probe never ran; see the metrics scraper.
		if res.skipped {
			continue
		}
		probed++

		ts := res.target
		// The record builders redact the endpoint they embed; the resource
		// attribute has to match, or the credential simply moves one level up.
		endpoint := redactEndpoint(ts.cfg.Endpoint)
		if res.pingErr != nil {
			failures = append(failures, fmt.Sprintf("ping %s: %v", endpoint, redactErr(res.pingErr)))
			continue
		}

		s.rb.SetTargetEndpoint(endpoint)

		if res.ping.Method == MethodHTTP {
			rec := plog.NewLogRecord()
			buildHTTPLogRecord(rec, ts, res.ping, res.startedAt, observed, s.cfg.Logs)
			s.lb.AppendLogRecord(rec)
		}

		if res.traceErr != nil {
			failures = append(failures, fmt.Sprintf("traceroute %s: %v", endpoint, redactErr(res.traceErr)))
		}
		if res.traced && len(res.trace.Hops) > 0 {
			rec := plog.NewLogRecord()
			buildTracerouteLogRecord(rec, ts, res.trace, res.traceTime(), observed, s.cfg.Logs)
			s.lb.AppendLogRecord(rec)
		}

		// EmitForResource is a no-op when no records were appended for this
		// target, and always resets the resource builder for the next one.
		s.lb.EmitForResource(metadata.WithLogsResource(s.rb.Emit()))
	}

	return s.lb.Emit(), summarizeFailures(failures, probed)
}
