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
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

// payloadEncoding is the value of the encoding attribute on exporter metrics.
const payloadEncoding = "ndjson/gzip"

// exabeamExporter writes logs to an Exabeam Cloud Collector's bucket
type exabeamExporter struct {
	settings  connectionSettings
	uploader  objectUploader
	encoder   *encoder
	logger    *zap.Logger
	telemetry *metadata.TelemetryBuilder
}

// newExporter creates a new Exabeam exporter
func newExporter(cfg *Config, params exporter.Settings) (*exabeamExporter, error) {
	s, err := cfg.resolve()
	if err != nil {
		return nil, err
	}

	return &exabeamExporter{
		settings: s,
		uploader: newS3Uploader(s, cfg.Endpoint),
		encoder:  newEncoder(cfg, s),
		logger:   params.Logger,
	}, nil
}

// Capabilities lists the exporter's capabilities
func (e *exabeamExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// logsDataPusher writes the logs as one or more files. Each batch from the sending
// queue becomes one file, unless it is larger than max_file_size.
func (e *exabeamExporter) logsDataPusher(ctx context.Context, ld plog.Logs) error {
	files, err := e.encoder.encode(ld)
	if err != nil {
		return consumererror.NewPermanent(fmt.Errorf("encode logs: %w", err))
	}

	for _, file := range files {
		name, err := newObjectName(e.settings.CollectorID, time.Now())
		if err != nil {
			return err
		}
		if err := e.uploadAndRecord(ctx, name, file); err != nil {
			return err
		}
	}
	return nil
}

// uploadAndRecord uploads the file and records telemetry metrics.
func (e *exabeamExporter) uploadAndRecord(ctx context.Context, objectName string, buf []byte) error {
	bucketAttr := attribute.String("bucket", e.settings.BucketName)
	payloadSize := int64(len(buf))

	e.telemetry.ExporterPayloadSize.Record(ctx, payloadSize,
		metric.WithAttributes(
			attribute.String("encoding", payloadEncoding),
			bucketAttr,
		),
	)

	e.telemetry.ExporterUploadInflight.Add(ctx, 1,
		metric.WithAttributes(bucketAttr),
	)

	start := time.Now()
	uploadErr := e.uploader.PutObject(ctx, objectName, buf)

	e.telemetry.ExporterUploadInflight.Add(ctx, -1,
		metric.WithAttributes(bucketAttr),
	)

	e.telemetry.ExporterRequestDuration.Record(ctx, time.Since(start).Milliseconds(),
		metric.WithAttributes(
			attribute.String("error", classifyError(uploadErr)),
			bucketAttr,
			attribute.String("location", e.settings.BucketLocation),
		),
	)

	if uploadErr == nil {
		e.telemetry.ExporterUploadBytesTotal.Add(ctx, payloadSize,
			metric.WithAttributes(bucketAttr),
		)
	} else if errors.Is(uploadErr, context.DeadlineExceeded) {
		e.telemetry.ExporterTimeoutTotal.Add(ctx, 1,
			metric.WithAttributes(bucketAttr),
		)
	}

	return uploadErr
}

// classifyError returns a string classification for the given error.
func classifyError(err error) string {
	if err == nil {
		return "none"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	return "unknown"
}
