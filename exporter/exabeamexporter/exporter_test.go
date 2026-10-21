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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/exporter/exabeamexporter/internal/metadata"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
)

// fakeUploader records uploads and returns the next error from errs, if any.
type fakeUploader struct {
	mu      sync.Mutex
	keys    []string
	bodies  [][]byte
	errs    []error
	blockCh chan struct{}
}

func (f *fakeUploader) PutObject(ctx context.Context, key string, body []byte) error {
	if f.blockCh != nil {
		select {
		case <-f.blockCh:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.keys = append(f.keys, key)
	f.bodies = append(f.bodies, body)
	if len(f.errs) > 0 {
		err := f.errs[0]
		f.errs = f.errs[1:]
		return err
	}
	return nil
}

func newTestTelemetryBuilder(t *testing.T, mp *sdkmetric.MeterProvider) *metadata.TelemetryBuilder {
	t.Helper()
	tb, err := metadata.NewTelemetryBuilder(component.TelemetrySettings{MeterProvider: mp})
	require.NoError(t, err)
	return tb
}

func newTestExporter(t *testing.T, up objectUploader, maxFileSize int64) (*exabeamExporter, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	return &exabeamExporter{
		settings:  testSettings(),
		uploader:  up,
		encoder:   newTestEncoder(maxFileSize),
		logger:    zap.NewNop(),
		telemetry: newTestTelemetryBuilder(t, mp),
	}, reader
}

func testLogs(n int, bodyLen int) plog.Logs {
	ld := plog.NewLogs()
	lrs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for i := range n {
		lrs.AppendEmpty().Body().SetStr(fmt.Sprintf("%04d %s", i, strings.Repeat("x", bodyLen)))
	}
	return ld
}

func collectMetrics(t *testing.T, reader *sdkmetric.ManualReader) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	return rm
}

func findMetric(rm metricdata.ResourceMetrics, name string) *metricdata.Metrics {
	for _, sm := range rm.ScopeMetrics {
		for i := range sm.Metrics {
			if sm.Metrics[i].Name == name {
				return &sm.Metrics[i]
			}
		}
	}
	return nil
}

func sumValue(t *testing.T, m *metricdata.Metrics) int64 {
	t.Helper()
	require.NotNil(t, m)
	sum, ok := m.Data.(metricdata.Sum[int64])
	require.True(t, ok)
	var total int64
	for _, dp := range sum.DataPoints {
		total += dp.Value
	}
	return total
}

func TestLogsDataPusher(t *testing.T) {
	up := &fakeUploader{}
	exp, reader := newTestExporter(t, up, defaultMaxFileSize)

	ld := testLogs(5, 10)
	require.NoError(t, exp.logsDataPusher(context.Background(), ld))

	require.Len(t, up.keys, 1)
	require.Regexp(t, objectNamePattern, up.keys[0])
	require.True(t, strings.HasPrefix(up.keys[0], "exa-cribl-logs_"+testCollectorID+"_1_"))

	lines := gunzipLines(t, up.bodies[0])
	require.Len(t, lines, 5)
	for i, line := range lines {
		require.Equal(t, ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(i).Body().Str(), decodeRecord(t, line)["message"])
	}

	rm := collectMetrics(t, reader)
	require.Equal(t, int64(len(up.bodies[0])), sumValue(t, findMetric(rm, "otelcol_exporter_upload_bytes_total")))
	require.Equal(t, int64(0), sumValue(t, findMetric(rm, "otelcol_exporter_upload_inflight")))

	payload := findMetric(rm, "otelcol_exporter_payload_size")
	require.NotNil(t, payload)
	hist := payload.Data.(metricdata.Histogram[int64])
	require.Len(t, hist.DataPoints, 1)
	enc, ok := hist.DataPoints[0].Attributes.Value(attribute.Key("encoding"))
	require.True(t, ok)
	require.Equal(t, "ndjson/gzip", enc.AsString())
	bucket, ok := hist.DataPoints[0].Attributes.Value(attribute.Key("bucket"))
	require.True(t, ok)
	require.Equal(t, "exabeam-bucket", bucket.AsString())

	dur := findMetric(rm, "otelcol_exporter_request_duration")
	require.NotNil(t, dur)
	durHist := dur.Data.(metricdata.Histogram[int64])
	errAttr, ok := durHist.DataPoints[0].Attributes.Value(attribute.Key("error"))
	require.True(t, ok)
	require.Equal(t, "none", errAttr.AsString())
	loc, ok := durHist.DataPoints[0].Attributes.Value(attribute.Key("location"))
	require.True(t, ok)
	require.Equal(t, "US", loc.AsString())
}

func TestLogsDataPusherNoRecords(t *testing.T) {
	up := &fakeUploader{}
	exp, _ := newTestExporter(t, up, defaultMaxFileSize)
	require.NoError(t, exp.logsDataPusher(context.Background(), plog.NewLogs()))
	require.Empty(t, up.keys)
}

func TestLogsDataPusherSplitsFiles(t *testing.T) {
	up := &fakeUploader{}
	exp, _ := newTestExporter(t, up, 2000)

	require.NoError(t, exp.logsDataPusher(context.Background(), testLogs(50, 100)))
	require.Greater(t, len(up.keys), 1)

	var total int
	names := map[string]bool{}
	for i, body := range up.bodies {
		total += len(gunzipLines(t, body))
		names[up.keys[i]] = true
	}
	require.Equal(t, 50, total)
	require.Len(t, names, len(up.keys), "every file gets a unique name")
}

func TestLogsDataPusherUploadError(t *testing.T) {
	up := &fakeUploader{errs: []error{errors.New("boom")}}
	exp, reader := newTestExporter(t, up, defaultMaxFileSize)

	err := exp.logsDataPusher(context.Background(), testLogs(1, 1))
	require.ErrorContains(t, err, "boom")

	rm := collectMetrics(t, reader)
	require.Nil(t, findMetric(rm, "otelcol_exporter_upload_bytes_total"))
	dur := findMetric(rm, "otelcol_exporter_request_duration").Data.(metricdata.Histogram[int64])
	errAttr, _ := dur.DataPoints[0].Attributes.Value(attribute.Key("error"))
	require.Equal(t, "unknown", errAttr.AsString())
}

func TestLogsDataPusherTimeout(t *testing.T) {
	up := &fakeUploader{blockCh: make(chan struct{})}
	exp, reader := newTestExporter(t, up, defaultMaxFileSize)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := exp.logsDataPusher(ctx, testLogs(1, 1))
	require.ErrorIs(t, err, context.DeadlineExceeded)

	rm := collectMetrics(t, reader)
	require.Equal(t, int64(1), sumValue(t, findMetric(rm, "otelcol_exporter_timeout_total")))
}

func TestClassifyError(t *testing.T) {
	require.Equal(t, "none", classifyError(nil))
	require.Equal(t, "timeout", classifyError(fmt.Errorf("wrapped: %w", context.DeadlineExceeded)))
	require.Equal(t, "unknown", classifyError(errors.New("other")))
}
