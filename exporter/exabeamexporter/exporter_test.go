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
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/exporter/exabeamexporter/internal/metadata"
	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/internal/testutils/retryserver"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// fakeUploader records uploads and returns the next error from errs, if any.
type fakeUploader struct {
	mu        sync.Mutex
	keys      []string
	bodies    [][]byte
	delivered [][]byte
	errs      []error
	blockCh   chan struct{}
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
	var err error
	if len(f.errs) > 0 {
		err, f.errs = f.errs[0], f.errs[1:]
	}
	if err == nil {
		f.delivered = append(f.delivered, body)
	}
	return err
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

// TestUploadRetrySequences checks which upload responses the exporterhelper retries. The SDK
// retries 408, 500, 502, 503 and 504 itself before returning, so the sequences use statuses it
// doesn't retry, and each call sends one request.
func TestUploadRetrySequences(t *testing.T) {
	type callExpectation struct {
		expectErr    bool
		permanentErr bool
	}

	testCases := []struct {
		name     string
		sequence []retryserver.Response
		calls    []callExpectation
	}{
		{
			name: "rate limited then success: 429 → 429 → success",
			sequence: []retryserver.Response{
				{StatusCode: http.StatusTooManyRequests},
				{StatusCode: http.StatusTooManyRequests},
				{StatusCode: http.StatusOK},
			},
			calls: []callExpectation{
				{expectErr: true, permanentErr: false},
				{expectErr: true, permanentErr: false},
				{expectErr: false},
			},
		},
		{
			name: "server error then success: 501 → success",
			sequence: []retryserver.Response{
				{StatusCode: http.StatusNotImplemented},
				{StatusCode: http.StatusOK},
			},
			calls: []callExpectation{
				{expectErr: true, permanentErr: false},
				{expectErr: false},
			},
		},
		{
			name:     "wrong keys: 403",
			sequence: []retryserver.Response{{StatusCode: http.StatusForbidden}},
			calls:    []callExpectation{{expectErr: true, permanentErr: true}},
		},
		{
			name:     "missing bucket: 404",
			sequence: []retryserver.Response{{StatusCode: http.StatusNotFound}},
			calls:    []callExpectation{{expectErr: true, permanentErr: true}},
		},
		{
			name: "retryable then permanent: 429 → 400",
			sequence: []retryserver.Response{
				{StatusCode: http.StatusTooManyRequests},
				{StatusCode: http.StatusBadRequest},
			},
			calls: []callExpectation{
				{expectErr: true, permanentErr: false},
				{expectErr: true, permanentErr: true},
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			srv := retryserver.New(t, tc.sequence)
			exp, _ := newTestExporter(t, newS3Uploader(testSettings(), srv.URL()), defaultMaxFileSize)

			for i, call := range tc.calls {
				err := exp.logsDataPusher(context.Background(), testLogs(1, 10))
				if call.expectErr {
					require.Error(t, err, "call %d should return error", i)
					require.Equal(t, call.permanentErr, consumererror.IsPermanent(err), "call %d permanentErr mismatch", i)
				} else {
					require.NoError(t, err, "call %d should succeed", i)
				}
			}
			require.Equal(t, len(tc.calls), srv.RequestCount(), "one request per call")
		})
	}
}

// TestLogsDataPusherPermanentFailureAfterPartialUpload checks that a permanent error on a later
// file stays permanent, so the exporterhelper drops the unsent records instead of retrying them.
func TestLogsDataPusherPermanentFailureAfterPartialUpload(t *testing.T) {
	up := &fakeUploader{errs: []error{nil, consumererror.NewPermanent(errors.New("forbidden"))}}
	exp, _ := newTestExporter(t, up, 2000)

	err := exp.logsDataPusher(context.Background(), testLogs(50, 100))
	require.True(t, consumererror.IsPermanent(err))
	require.Len(t, up.delivered, 1)
}

func TestClassifyError(t *testing.T) {
	require.Equal(t, "none", classifyError(nil))
	require.Equal(t, "timeout", classifyError(fmt.Errorf("wrapped: %w", context.DeadlineExceeded)))
	require.Equal(t, "unknown", classifyError(errors.New("other")))
}

// deliveredMessages returns the message of every record in files, in order.
func deliveredMessages(t *testing.T, files [][]byte) []string {
	t.Helper()
	var messages []string
	for _, f := range files {
		for _, line := range gunzipLines(t, f) {
			messages = append(messages, decodeRecord(t, line)["message"].(string))
		}
	}
	return messages
}

func logBodies(ld plog.Logs) []string {
	var bodies []string
	for _, rl := range ld.ResourceLogs().All() {
		for _, sl := range rl.ScopeLogs().All() {
			for _, lr := range sl.LogRecords().All() {
				bodies = append(bodies, lr.Body().Str())
			}
		}
	}
	return bodies
}

func TestLogsDataPusherPartialFailureRetriesOnlyUnsentRecords(t *testing.T) {
	up := &fakeUploader{errs: []error{nil, errors.New("boom")}}
	exp, _ := newTestExporter(t, up, 2000)

	// Spread the records over two resources and scopes so the retry data has to keep structure.
	ld := plog.NewLogs()
	for r := range 2 {
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("host.name", fmt.Sprintf("host-%d", r))
		rl.SetSchemaUrl("https://example.com/schema")
		for s := range 2 {
			sl := rl.ScopeLogs().AppendEmpty()
			sl.Scope().SetName(fmt.Sprintf("scope-%d", s))
			for l := range 10 {
				sl.LogRecords().AppendEmpty().Body().SetStr(fmt.Sprintf("%d-%d-%02d %s", r, s, l, strings.Repeat("x", 100)))
			}
		}
	}
	all := logBodies(ld)

	err := exp.logsDataPusher(context.Background(), ld)
	require.ErrorContains(t, err, "boom")
	require.False(t, consumererror.IsPermanent(err))

	var logsErr consumererror.Logs
	require.True(t, errors.As(err, &logsErr), "the error carries only the unsent records")
	retry := logsErr.Data()

	sent := deliveredMessages(t, up.delivered)
	require.Len(t, up.delivered, 1)
	require.NotEmpty(t, sent)
	require.Equal(t, all[len(sent):], logBodies(retry), "retry data is exactly the records after the uploaded file")

	// Resource and scope are kept for the records that are retried.
	last := retry.ResourceLogs().At(retry.ResourceLogs().Len() - 1)
	host, _ := last.Resource().Attributes().Get("host.name")
	require.Equal(t, "host-1", host.Str())
	require.Equal(t, "https://example.com/schema", last.SchemaUrl())
	require.Equal(t, "scope-1", last.ScopeLogs().At(last.ScopeLogs().Len()-1).Scope().Name())

	// Retrying the error's data completes the batch with no record sent twice.
	require.NoError(t, exp.logsDataPusher(context.Background(), retry))
	require.Equal(t, all, deliveredMessages(t, up.delivered))
}

// TestLogsDataPusherPartialFailureWithEmptyContainers checks that the encoder's file boundaries
// and the retry data count records the same way when resources and scopes without records are
// mixed in, for a failure on any file after the first.
func TestLogsDataPusherPartialFailureWithEmptyContainers(t *testing.T) {
	const bodyLen = 100
	lineSize := testLineSize(t, bodyLen)

	// 8 records with 3 per file: files start at records 0, 3 and 6.
	newLogs := func() plog.Logs {
		ld := plog.NewLogs()
		next := 0
		addRecords := func(sl plog.ScopeLogs, n int) {
			for range n {
				sl.LogRecords().AppendEmpty().Body().SetStr(fmt.Sprintf("%04d %s", next, strings.Repeat("x", bodyLen)))
				next++
			}
		}
		ld.ResourceLogs().AppendEmpty() // no scopes
		rl := ld.ResourceLogs().AppendEmpty()
		rl.ScopeLogs().AppendEmpty() // no records
		addRecords(rl.ScopeLogs().AppendEmpty(), 4)
		rl.ScopeLogs().AppendEmpty()
		ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
		rl = ld.ResourceLogs().AppendEmpty()
		addRecords(rl.ScopeLogs().AppendEmpty(), 2)
		rl.ScopeLogs().AppendEmpty()
		addRecords(rl.ScopeLogs().AppendEmpty(), 2)
		ld.ResourceLogs().AppendEmpty()
		return ld
	}

	testCases := []struct {
		desc       string
		errs       []error
		retryStart int
	}{
		{desc: "second file fails", errs: []error{nil, errors.New("boom")}, retryStart: 3},
		{desc: "third file fails", errs: []error{nil, nil, errors.New("boom")}, retryStart: 6},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			up := &fakeUploader{errs: tc.errs}
			exp, _ := newTestExporter(t, up, 3*lineSize)

			ld := newLogs()
			all := logBodies(ld)
			require.Len(t, all, 8)

			err := exp.logsDataPusher(context.Background(), ld)
			require.ErrorContains(t, err, "boom")
			var logsErr consumererror.Logs
			require.True(t, errors.As(err, &logsErr))
			require.Equal(t, all[:tc.retryStart], deliveredMessages(t, up.delivered))
			require.Equal(t, all[tc.retryStart:], logBodies(logsErr.Data()))

			require.NoError(t, exp.logsDataPusher(context.Background(), logsErr.Data()))
			require.Equal(t, all, deliveredMessages(t, up.delivered), "no record is sent twice or skipped")
		})
	}
}

func TestLogsDataPusherFirstFileFailureRetriesEverything(t *testing.T) {
	boom := errors.New("boom")
	up := &fakeUploader{errs: []error{boom}}
	exp, _ := newTestExporter(t, up, 2000)

	err := exp.logsDataPusher(context.Background(), testLogs(50, 100))
	require.ErrorIs(t, err, boom)
	var logsErr consumererror.Logs
	require.False(t, errors.As(err, &logsErr), "nothing was sent, so the whole request is retried as is")
	require.Empty(t, up.delivered)
}

func TestLogsDataPusherRawLogFieldFailureFallsBack(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	up := &fakeUploader{}
	exp, _ := newTestExporter(t, up, defaultMaxFileSize)
	exp.encoder = newTestEncoderWithConfig(t, func(c *Config) { c.RawLogField = failingRawLogField })
	exp.encoder.logger = zap.New(core)

	ld := plog.NewLogs()
	lrs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	good := lrs.AppendEmpty()
	good.Body().SetStr("good body")
	good.Attributes().PutStr("raw", "a,b")
	bad := lrs.AppendEmpty()
	bad.Body().SetStr("bad body")
	bad.Attributes().PutInt("raw", 5)

	require.NoError(t, exp.logsDataPusher(context.Background(), ld), "one bad record doesn't fail the batch")
	require.Equal(t, []string{`["a","b"]`, "bad body"}, deliveredMessages(t, up.delivered))

	warnings := logs.FilterMessage("raw_log_field failed; used the default raw log").All()
	require.Len(t, warnings, 1, "one warning per batch")
	require.Equal(t, int64(1), warnings[0].ContextMap()["records"])
}
