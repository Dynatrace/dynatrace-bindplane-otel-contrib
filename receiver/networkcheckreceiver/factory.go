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
	"time"

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
	return &Config{
		ControllerConfig: scraperhelper.ControllerConfig{
			CollectionInterval: 60 * time.Second,
			InitialDelay:       time.Second,
		},
		MetricsBuilderConfig: metadata.NewDefaultMetricsBuilderConfig(),
		Logs: LogsConfig{
			IncludeTLSDetails: true,
		},
		Traceroute: TracerouteConfig{
			Method:                 "udp",
			MaxHops:                30,
			FailureThreshold:       0.5,
			Timeout:                3 * time.Second,
			ProbesPerHop:           defaultProbesPerHop,
			MaxConsecutiveTimeouts: defaultMaxConsecutiveTimeouts,
		},
	}
}

var errInvalidConfig = errors.New("config is not a networkcheck receiver config")

func createMetricsReceiver(
	_ context.Context,
	params receiver.Settings,
	rConf component.Config,
	consumer consumer.Metrics,
) (receiver.Metrics, error) {
	cfg, ok := rConf.(*Config)
	if !ok {
		return nil, errInvalidConfig
	}

	ns := newNetworkCheckScraper(params, cfg)
	s, err := scraper.NewMetrics(
		ns.scrape,
		scraper.WithStart(ns.start),
		scraper.WithShutdown(ns.shutdown),
	)
	if err != nil {
		return nil, err
	}

	ctrl, err := scraperhelper.NewMetricsController(
		&cfg.ControllerConfig, params, consumer,
		scraperhelper.AddMetricsScraper(metadata.Type, s),
	)
	if err != nil {
		return nil, err
	}
	return &proberStopper{Component: ctrl, prober: ns.prober}, nil
}

// proberStopper cancels the shared prober's in-flight cycle before the scraper
// controller shuts down. scraperhelper stops its tick loop with close(done)
// followed by wg.Wait() and only then calls the scraper's shutdown hook, so by
// the time the hook runs the controller has already waited for the slowest
// probe. Cancelling first bounds Shutdown to the time one probe takes to notice
// its context.
type proberStopper struct {
	component.Component
	prober *sharedProber
}

func (r *proberStopper) Shutdown(ctx context.Context) error {
	r.prober.stop()
	return r.Component.Shutdown(ctx)
}

// createLogsReceiver builds the logs signal. scraperhelper has a logs
// controller but no AddLogsScraper helper, so the scraper factory is
// constructed directly and passed through AddFactoryWithConfig. Sharing
// ControllerConfig keeps both signals on one collection interval, which is what
// lets them share a probe cycle.
func createLogsReceiver(
	_ context.Context,
	params receiver.Settings,
	rConf component.Config,
	consumer consumer.Logs,
) (receiver.Logs, error) {
	cfg, ok := rConf.(*Config)
	if !ok {
		return nil, errInvalidConfig
	}

	ls := newNetworkCheckLogsScraper(params, cfg)
	s, err := scraper.NewLogs(
		ls.scrape,
		scraper.WithStart(ls.start),
		scraper.WithShutdown(ls.shutdown),
	)
	if err != nil {
		return nil, err
	}

	f := scraper.NewFactory(metadata.Type, nil,
		scraper.WithLogs(func(context.Context, scraper.Settings, component.Config) (scraper.Logs, error) {
			return s, nil
		}, metadata.LogsStability),
	)

	ctrl, err := scraperhelper.NewLogsController(
		&cfg.ControllerConfig, params, consumer,
		scraperhelper.AddFactoryWithConfig(f, nil),
	)
	if err != nil {
		return nil, err
	}
	return &proberStopper{Component: ctrl, prober: ls.prober}, nil
}
