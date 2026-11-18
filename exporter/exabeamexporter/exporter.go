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

	enc, err := newEncoder(cfg, s, params.TelemetrySettings)
	if err != nil {
		return nil, err
	}

	return &exabeamExporter{
		settings: s,
		uploader: newS3Uploader(s, cfg.Endpoint),
		encoder:  enc,
		logger:   params.Logger,
	}, nil
}

// Capabilities lists the exporter's capabilities
func (e *exabeamExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

// logsDataPusher writes the logs as one or more files. Each batch from the sending
// queue becomes one file, unless it is larger than max_file_size.
// When an upload fails after earlier files of the batch were uploaded, the error carries
// only the records that weren't uploaded, so the retry doesn't send any record twice.
func (e *exabeamExporter) logsDataPusher(ctx context.Context, ld plog.Logs) error {
	files, err := e.encoder.encode(ctx, ld)
	if err != nil {
		return consumererror.NewPermanent(fmt.Errorf("encode logs: %w", err))
	}
	if len(files) == 0 {
		return nil
	}

	bucketAttr := metric.WithAttributes(attribute.String("bucket", e.settings.BucketName))
	var unsplitSize int64
	for _, file := range files {
		unsplitSize += file.rawSize
	}
	e.telemetry.ExporterUnsplitPayloadSize.Record(ctx, unsplitSize, bucketAttr)

	for i, file := range files {
		err := e.uploadFile(ctx, file)
		if err == nil {
			continue
		}
		e.telemetry.ExporterLogsSendFailed.Add(ctx, 1, bucketAttr)
		if i == 0 {
			return err
		}
		return consumererror.NewLogs(err, logsFrom(ld, file.firstRecord))
	}
	return nil
}

func (e *exabeamExporter) uploadFile(ctx context.Context, file encodedFile) error {
	name, err := newObjectName(e.settings.CollectorID, time.Now())
	if err != nil {
		return err
	}
	return e.uploadAndRecord(ctx, name, file)
}

// logsFrom returns a copy of the log records of ld from index start onward, keeping their
// resource and scope.
func logsFrom(ld plog.Logs, start int) plog.Logs {
	out := plog.NewLogs()
	index := 0
	for _, rl := range ld.ResourceLogs().All() {
		var outRL plog.ResourceLogs
		for _, sl := range rl.ScopeLogs().All() {
			var outSL plog.ScopeLogs
			for _, lr := range sl.LogRecords().All() {
				if index >= start {
					if outRL == (plog.ResourceLogs{}) {
						outRL = out.ResourceLogs().AppendEmpty()
						rl.Resource().CopyTo(outRL.Resource())
						outRL.SetSchemaUrl(rl.SchemaUrl())
					}
					if outSL == (plog.ScopeLogs{}) {
						outSL = outRL.ScopeLogs().AppendEmpty()
						sl.Scope().CopyTo(outSL.Scope())
						outSL.SetSchemaUrl(sl.SchemaUrl())
					}
					lr.CopyTo(outSL.LogRecords().AppendEmpty())
				}
				index++
			}
		}
	}
	return out
}

// uploadAndRecord uploads the file and records telemetry metrics.
func (e *exabeamExporter) uploadAndRecord(ctx context.Context, objectName string, file encodedFile) error {
	bucketAttr := attribute.String("bucket", e.settings.BucketName)
	buf := file.data
	payloadSize := int64(len(buf))

	e.telemetry.ExporterUploadInflight.Add(ctx, 1,
		metric.WithAttributes(bucketAttr),
	)

	start := time.Now()
	uploadErr := e.uploader.PutObject(ctx, objectName, buf)

	e.telemetry.ExporterUploadInflight.Add(ctx, -1,
		metric.WithAttributes(bucketAttr),
	)

	errAttr := attribute.String("error", classifyError(uploadErr))
	e.telemetry.ExporterRequestDuration.Record(ctx, time.Since(start).Milliseconds(),
		metric.WithAttributes(
			errAttr,
			bucketAttr,
			attribute.String("location", e.settings.BucketLocation),
		),
	)
	e.telemetry.ExporterRequestCount.Add(ctx, 1,
		metric.WithAttributes(errAttr, bucketAttr),
	)

	if uploadErr == nil {
		// The size histograms describe uploaded files, so failed attempts and retries aren't recorded.
		e.telemetry.ExporterPayloadSize.Record(ctx, payloadSize,
			metric.WithAttributes(
				attribute.String("encoding", payloadEncoding),
				bucketAttr,
			),
		)
		e.telemetry.ExporterBatchSize.Record(ctx, int64(file.records),
			metric.WithAttributes(bucketAttr),
		)
		e.telemetry.ExporterUploadBytesTotal.Add(ctx, payloadSize,
			metric.WithAttributes(bucketAttr),
		)
		e.telemetry.ExporterRawBytes.Add(ctx, file.rawSize,
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
