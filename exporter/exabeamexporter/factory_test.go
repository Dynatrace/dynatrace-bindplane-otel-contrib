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
	"net/http"
	"testing"
	"time"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/exporter/exabeamexporter/internal/metadata"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)

	require.Equal(t, "https://storage.googleapis.com", cfg.Endpoint)
	require.Equal(t, int64(10_000_000), cfg.MaxFileSize)
	require.Equal(t, 30*time.Second, cfg.TimeoutConfig.Timeout)
	require.True(t, cfg.BackOffConfig.Enabled)

	require.True(t, cfg.QueueConfig.HasValue())
	queue := cfg.QueueConfig.Get()
	require.True(t, queue.Batch.HasValue())
	batch := queue.Batch.Get()
	require.Equal(t, exporterhelper.RequestSizerTypeBytes, batch.Sizer)
	require.Equal(t, int64(10_000_000), batch.MinSize)
	require.Equal(t, int64(10_000_000), batch.MaxSize)
	require.Equal(t, 300*time.Second, batch.FlushTimeout)

	require.NoError(t, componenttest.CheckConfigStruct(cfg))
}

func TestCreateLogsErrors(t *testing.T) {
	tests := []struct {
		desc        string
		cfg         func(t *testing.T) component.Config
		expectedErr string
	}{
		{
			desc:        "not an Exabeam config",
			cfg:         func(*testing.T) component.Config { return &struct{}{} },
			expectedErr: "not an Exabeam config",
		},
		{
			desc: "invalid connection string",
			cfg: func(*testing.T) component.Config {
				cfg := createDefaultConfig().(*Config)
				cfg.ConnectionString = "not base64!"
				return cfg
			},
			expectedErr: "connection_string is not valid base64",
		},
		{
			desc: "invalid raw_log_field without validation",
			cfg: func(t *testing.T) component.Config {
				cfg := createDefaultConfig().(*Config)
				cfg.ConnectionString = testConnectionStringOpaque(t)
				cfg.RawLogField = `attributes["unterminated`
				return cfg
			},
			expectedErr: "raw_log_field is invalid",
		},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			exp, err := NewFactory().CreateLogs(context.Background(), exportertest.NewNopSettings(metadata.Type), tc.cfg(t))
			require.ErrorContains(t, err, tc.expectedErr)
			require.Nil(t, exp)
		})
	}
}

// startTestExporter creates and starts an exporter against srv with fast flushes and retries.
func startTestExporter(t *testing.T, srv *fakeS3, mutate func(*Config)) exporter.Logs {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.ConnectionString = testConnectionStringOpaque(t)
	cfg.Endpoint = srv.URL
	cfg.QueueConfig.Get().Batch.Get().FlushTimeout = 10 * time.Millisecond
	cfg.BackOffConfig.InitialInterval = 10 * time.Millisecond
	cfg.BackOffConfig.MaxInterval = 20 * time.Millisecond
	cfg.BackOffConfig.RandomizationFactor = 0
	if mutate != nil {
		mutate(cfg)
	}
	require.NoError(t, cfg.Validate())

	exp, err := NewFactory().CreateLogs(context.Background(), exportertest.NewNopSettings(metadata.Type), cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(context.Background(), componenttest.NewNopHost()))
	t.Cleanup(func() { require.NoError(t, exp.Shutdown(context.Background())) })
	return exp
}

// bodiesExcept returns the bodies of the PUTs srv received, leaving out the request at index skip.
func bodiesExcept(srv *fakeS3, skip int) [][]byte {
	var bodies [][]byte
	for i, r := range srv.recorded() {
		if i != skip {
			bodies = append(bodies, r.Body)
		}
	}
	return bodies
}

// TestExporterRetriesTransientFailure checks that a failed upload is retried through the
// sending queue and the data arrives exactly once.
func TestExporterRetriesTransientFailure(t *testing.T) {
	srv := newFakeS3(t)
	// The SDK doesn't retry a 429 without a throttling error code itself, so the exporterhelper retry has to.
	srv.respondNext(http.StatusTooManyRequests)
	exp := startTestExporter(t, srv, nil)

	ld := testLogs(5, 10)
	require.NoError(t, exp.ConsumeLogs(context.Background(), ld))

	require.Eventually(t, func() bool { return len(srv.recorded()) >= 2 }, 5*time.Second, 10*time.Millisecond)
	reqs := srv.recorded()
	require.Len(t, reqs, 2)
	require.Equal(t, deliveredMessages(t, [][]byte{reqs[0].Body}), deliveredMessages(t, [][]byte{reqs[1].Body}), "the same records are retried")
	require.Equal(t, logBodies(ld), deliveredMessages(t, bodiesExcept(srv, 0)))
}

// TestExporterPartialFailureNoDuplicates checks that when a batch is split into several
// files and one upload fails, only the unsent records are retried.
func TestExporterPartialFailureNoDuplicates(t *testing.T) {
	srv := newFakeS3(t)
	srv.respondNext(http.StatusOK, http.StatusTooManyRequests)
	exp := startTestExporter(t, srv, func(c *Config) { c.MaxFileSize = 1500 })

	ld := testLogs(12, 100)
	require.NoError(t, exp.ConsumeLogs(context.Background(), ld))

	expected := logBodies(ld)
	require.Eventually(t, func() bool {
		return len(deliveredMessages(t, bodiesExcept(srv, 1))) >= len(expected)
	}, 5*time.Second, 10*time.Millisecond)
	require.Greater(t, len(srv.recorded()), 3, "the batch was split into several files")
	require.Equal(t, expected, deliveredMessages(t, bodiesExcept(srv, 1)), "every record exactly once, in order")
}

// TestExporterDoesNotRetryPermanentFailure checks that the sending queue drops a batch
// rejected with a permanent error instead of retrying it.
func TestExporterDoesNotRetryPermanentFailure(t *testing.T) {
	srv := newFakeS3(t)
	srv.setStatus(http.StatusForbidden)
	exp := startTestExporter(t, srv, nil)

	require.NoError(t, exp.ConsumeLogs(context.Background(), testLogs(5, 10)))

	require.Eventually(t, func() bool { return len(srv.recorded()) == 1 }, 5*time.Second, 10*time.Millisecond)
	// The test backoff is 10-20ms, so a retry would show up well within this window.
	require.Never(t, func() bool { return len(srv.recorded()) > 1 }, 300*time.Millisecond, 10*time.Millisecond)
}

// TestExporterFlushesAtMinSize checks that a batch is uploaded as soon as it reaches min_size,
// without waiting for the flush timeout, and that the rest is uploaded on shutdown.
func TestExporterFlushesAtMinSize(t *testing.T) {
	srv := newFakeS3(t)

	cfg := createDefaultConfig().(*Config)
	cfg.ConnectionString = testConnectionStringOpaque(t)
	cfg.Endpoint = srv.URL
	batch := cfg.QueueConfig.Get().Batch.Get()
	batch.MinSize = 2000
	batch.MaxSize = 2000
	batch.FlushTimeout = time.Hour
	require.NoError(t, cfg.Validate())

	exp, err := NewFactory().CreateLogs(context.Background(), exportertest.NewNopSettings(metadata.Type), cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(context.Background(), componenttest.NewNopHost()))

	ld := testLogs(50, 100)
	expected := logBodies(ld)
	require.NoError(t, exp.ConsumeLogs(context.Background(), ld))

	// Over 5000 bytes of logs fill at least two batches, which upload before the hour-long flush timeout.
	require.Eventually(t, func() bool { return len(srv.recorded()) >= 2 }, 5*time.Second, 10*time.Millisecond)

	require.NoError(t, exp.Shutdown(context.Background()))

	var bodies [][]byte
	for _, r := range srv.recorded() {
		bodies = append(bodies, r.Body)
	}
	// Batches upload concurrently, so files can arrive in any order.
	require.ElementsMatch(t, expected, deliveredMessages(t, bodies), "every record exactly once")
}

// TestExporterEndToEnd runs the exporter as the collector does, against a fake
// S3 endpoint, and checks that batched data is uploaded on shutdown.
func TestExporterEndToEnd(t *testing.T) {
	srv := newFakeS3(t)

	cfg := createDefaultConfig().(*Config)
	cfg.ConnectionString = configopaque.String(testConnectionString(t, fullConnectionFields()))
	cfg.Endpoint = srv.URL
	cfg.CollectorName = "bindplane-gateway"
	require.NoError(t, cfg.Validate())

	exp, err := NewFactory().CreateLogs(context.Background(), exportertest.NewNopSettings(metadata.Type), cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(context.Background(), componenttest.NewNopHost()))

	require.NoError(t, exp.ConsumeLogs(context.Background(), testLogs(3, 10)))
	require.NoError(t, exp.ConsumeLogs(context.Background(), testLogs(2, 10)))

	// The batch is far below 10 MB and the 300s flush timeout hasn't passed, so nothing is uploaded yet.
	require.Empty(t, srv.recorded())

	require.NoError(t, exp.Shutdown(context.Background()))

	reqs := srv.recorded()
	require.Len(t, reqs, 1, "both pushes are batched into one file")
	require.Equal(t, http.MethodPut, reqs[0].Method)
	key := reqs[0].Path[len("/dl-topic-ops-dist-cc-mgrtn-e2e/"):]
	require.Regexp(t, objectNamePattern, key)

	lines := gunzipLines(t, reqs[0].Body)
	require.Len(t, lines, 5)
	rec := decodeRecord(t, lines[0])
	require.Equal(t, "bindplane-gateway", rec["collector_name"])
	require.Equal(t, "abcorp", rec["sitename"])
	require.Equal(t, "xdfsdfw99999sfsdf", rec["siteid"])
	require.Equal(t, "-0700", rec["timezone_offset"])
}
