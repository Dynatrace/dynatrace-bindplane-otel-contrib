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

package exabeamexporter // import "github.com/dynatrace/dynatrace-bindplane-otel-contrib/exporter/exabeamexporter"

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/exporter/exabeamexporter/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

const (
	defaultEndpoint = "https://storage.googleapis.com"

	// defaultMaxFileSize and defaultFlushTimeout follow Exabeam's recommended rollover
	// settings: 10 MB max file size and 300s max open time. The batcher's flush timeout
	// starts when a batch opens, so it matches max open time; there is no idle timeout.
	defaultMaxFileSize  = 10_000_000
	defaultFlushTimeout = 300 * time.Second

	// defaultTimeout bounds one upload. Batched files are up to several MB compressed,
	// more than exporterhelper's 5s default allows on a slow link.
	defaultTimeout = 30 * time.Second
)

// NewFactory creates a factory for the Exabeam exporter
func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	batch := exporterhelper.NewDefaultBatchConfig()
	batch.Sizer = exporterhelper.RequestSizerTypeBytes
	batch.MinSize = defaultMaxFileSize
	batch.MaxSize = defaultMaxFileSize
	batch.FlushTimeout = defaultFlushTimeout

	queue := exporterhelper.NewDefaultQueueConfig()
	queue.Batch = configoptional.Some(batch)

	return &Config{
		Endpoint:      defaultEndpoint,
		MaxFileSize:   defaultMaxFileSize,
		TimeoutConfig: exporterhelper.TimeoutConfig{Timeout: defaultTimeout},
		QueueConfig:   configoptional.Some(queue),
		BackOffConfig: configretry.NewDefaultBackOffConfig(),
	}
}

func createLogsExporter(ctx context.Context, params exporter.Settings, config component.Config) (exporter.Logs, error) {
	cfg, ok := config.(*Config)
	if !ok {
		return nil, errors.New("not an Exabeam config")
	}
	exp, err := newExporter(cfg, params)
	if err != nil {
		return nil, fmt.Errorf("new logs exporter: %w", err)
	}
	telemetry, err := metadata.NewTelemetryBuilder(params.TelemetrySettings)
	if err != nil {
		return nil, fmt.Errorf("create telemetry builder: %w", err)
	}
	exp.telemetry = telemetry
	return exporterhelper.NewLogs(
		ctx,
		params,
		config,
		exp.logsDataPusher,
		exporterhelper.WithCapabilities(exp.Capabilities()),
		exporterhelper.WithTimeout(cfg.TimeoutConfig),
		exporterhelper.WithQueue(cfg.QueueConfig),
		exporterhelper.WithRetry(cfg.BackOffConfig),
	)
}
