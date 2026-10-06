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

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/dnscheckreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httpcheckreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/icmpcheckreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/tcpcheckreceiver"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/receiver"
	"go.opentelemetry.io/collector/scraper"
	"go.opentelemetry.io/collector/scraper/scraperhelper"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver/internal/metadata"
)

// NewFactory creates a factory for the networkcheck receiver.
func NewFactory() receiver.Factory {
	return receiver.NewFactory(
		metadata.Type,
		createDefaultConfig,
		receiver.WithMetrics(createMetricsReceiver, metadata.MetricsStability),
		receiver.WithLogs(createLogsReceiver, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{ControllerConfig: scraperhelper.NewDefaultControllerConfig()}
}

// controllerConfig is the scraper controller configuration of scheduled
// traces. Its own timeout (the scrape deadline) stays unset: the cycle budget
// bounds a scrape.
func (c *TracerouteConfig) controllerConfig() *scraperhelper.ControllerConfig {
	cc := scraperhelper.NewDefaultControllerConfig()
	cc.CollectionInterval = c.CollectionInterval
	cc.InitialDelay = c.InitialDelay
	cc.Timeout = 0
	return &cc
}

// createMetricsReceiver builds one upstream receiver per check section, each
// sending to next, plus a scraper controller for scheduled traces. The icmp
// section sends through a triggerConsumer when on_failure is enabled.
func createMetricsReceiver(ctx context.Context, set receiver.Settings, rConf component.Config, next consumer.Metrics) (_ receiver.Metrics, err error) {
	cfg := rConf.(*Config)
	r := &networkCheck{id: set.ID}
	icmpNext := next

	if tc := cfg.Traceroute; tc != nil {
		r.prober = acquireProber(set.ID, tc, set.Logger)
		defer func() {
			if err != nil {
				releaseProber(set.ID)
			}
		}()
		if tc.scheduled() {
			ts := newTraceScraper(set, tc, r.prober)
			s, err := scraper.NewMetrics(ts.scrapeMetrics)
			if err != nil {
				return nil, err
			}
			ctrl, err := scraperhelper.NewMetricsController(tc.controllerConfig(), set, next,
				scraperhelper.AddMetricsScraper(metadata.Type, s))
			if err != nil {
				return nil, err
			}
			r.sections = append(r.sections, section{name: "traceroute", Component: ctrl})
		}
		if tc.OnFailure.Enabled && cfg.ICMP != nil {
			r.trigger = newTriggerConsumer(next, r.prober, tc, set)
			icmpNext = r.trigger
		}
	}

	// A nil section pointer is a non-nil component.Config, so each is checked
	// before it is boxed.
	add := func(name string, f receiver.Factory, c component.Config, next consumer.Metrics) error {
		child, err := f.CreateMetrics(ctx, childSettings(set, f.Type(), name), c, next)
		if err != nil {
			return fmt.Errorf("creating the %s section: %w", name, err)
		}
		r.sections = append(r.sections, section{name: name, Component: child})
		return nil
	}
	if cfg.HTTP != nil {
		err = errors.Join(err, add("http", httpcheckreceiver.NewFactory(), cfg.HTTP, next))
	}
	if cfg.ICMP != nil {
		err = errors.Join(err, add("icmp", icmpcheckreceiver.NewFactory(), cfg.ICMP, icmpNext))
	}
	if cfg.DNS != nil {
		err = errors.Join(err, add("dns", dnscheckreceiver.NewFactory(), cfg.DNS, next))
	}
	if cfg.TCP != nil {
		err = errors.Join(err, add("tcp", tcpcheckreceiver.NewFactory(), cfg.TCP, next))
	}
	if err != nil {
		return nil, err
	}
	return r, nil
}

// createLogsReceiver builds the logs signal. Only traceroute produces logs:
// one record per trace. Without a traceroute section the receiver starts and
// emits nothing.
//
// scraperhelper has a logs controller but no AddLogsScraper helper, so the
// scraper factory is constructed directly and passed through
// AddFactoryWithConfig.
func createLogsReceiver(_ context.Context, set receiver.Settings, rConf component.Config, next consumer.Logs) (_ receiver.Logs, err error) {
	cfg := rConf.(*Config)
	r := &networkCheck{id: set.ID}
	tc := cfg.Traceroute
	if tc == nil {
		return r, nil
	}

	r.prober = acquireProber(set.ID, tc, set.Logger)
	r.logs = next
	if !tc.scheduled() {
		return r, nil
	}
	defer func() {
		if err != nil {
			releaseProber(set.ID)
		}
	}()
	ts := newTraceScraper(set, tc, r.prober)
	s, err := scraper.NewLogs(ts.scrapeLogs)
	if err != nil {
		return nil, err
	}
	f := scraper.NewFactory(metadata.Type, nil,
		scraper.WithLogs(func(context.Context, scraper.Settings, component.Config) (scraper.Logs, error) {
			return s, nil
		}, metadata.LogsStability),
	)
	ctrl, err := scraperhelper.NewLogsController(tc.controllerConfig(), set, next,
		scraperhelper.AddFactoryWithConfig(f, nil))
	if err != nil {
		return nil, err
	}
	r.sections = append(r.sections, section{name: "traceroute", Component: ctrl})
	return r, nil
}

// childSettings derives the settings of a section's upstream receiver: an ID
// of that receiver's type named after this receiver and the section, such as
// http_check/networkcheck/http, so its own telemetry is told apart.
func childSettings(set receiver.Settings, t component.Type, name string) receiver.Settings {
	parent := set.ID.Name()
	if parent == "" {
		parent = metadata.Type.String()
	}
	cs := set
	cs.ID = component.NewIDWithName(t, parent+"/"+name)
	cs.Logger = set.Logger.Named(name)
	return cs
}

// section is one part of the receiver: an upstream check receiver, or the
// scraper controller of scheduled traces.
type section struct {
	name string
	component.Component
}

// networkCheck is the receiver: its sections, started and stopped together.
type networkCheck struct {
	id       component.ID
	sections []section

	// prober is the traceroute state shared with the receiver's other
	// signal; nil without a traceroute section.
	prober *sharedProber

	// trigger is set on the metrics signal when on_failure is enabled.
	trigger *triggerConsumer

	// logs is set on the logs signal; triggered traces send records to it.
	logs consumer.Logs
}

func (r *networkCheck) Start(ctx context.Context, host component.Host) error {
	if r.prober != nil {
		r.prober.start()
		if r.logs != nil {
			r.prober.setLogs(r.logs)
		}
	}
	for _, s := range r.sections {
		if err := s.Start(ctx, host); err != nil {
			return fmt.Errorf("starting the %s section: %w", s.name, err)
		}
	}
	return nil
}

// Shutdown cancels in-flight traces before stopping anything: scraperhelper
// waits for a running scrape before it stops, so without the cancel Shutdown
// would wait for the slowest trace. Every section is stopped even when one
// fails to.
func (r *networkCheck) Shutdown(ctx context.Context) error {
	if r.prober != nil {
		r.prober.stop()
		if r.logs != nil {
			r.prober.setLogs(nil)
		}
	}
	if r.trigger != nil {
		r.trigger.close()
	}
	var errs error
	for _, s := range r.sections {
		if err := s.Shutdown(ctx); err != nil {
			errs = errors.Join(errs, fmt.Errorf("stopping the %s section: %w", s.name, err))
		}
	}
	if r.prober != nil {
		releaseProber(r.id)
		r.prober = nil
	}
	return errs
}
