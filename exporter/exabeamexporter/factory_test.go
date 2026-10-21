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
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
)

func TestCreateDefaultConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)

	require.Equal(t, "https://storage.googleapis.com", cfg.Endpoint)
	require.Equal(t, int64(10*1024*1024), cfg.MaxFileSize)
	require.Equal(t, exporterhelper.NewDefaultTimeoutConfig(), cfg.TimeoutConfig)
	require.True(t, cfg.BackOffConfig.Enabled)

	require.True(t, cfg.QueueConfig.HasValue())
	queue := cfg.QueueConfig.Get()
	require.True(t, queue.Batch.HasValue())
	batch := queue.Batch.Get()
	require.Equal(t, exporterhelper.RequestSizerTypeBytes, batch.Sizer)
	require.Equal(t, int64(10*1024*1024), batch.MinSize)
	require.Equal(t, int64(10*1024*1024), batch.MaxSize)
	require.Equal(t, 30*time.Second, batch.FlushTimeout)

	require.NoError(t, componenttest.CheckConfigStruct(cfg))
}

func TestCreateLogsInvalidConfig(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.ConnectionString = "not base64!"
	_, err := NewFactory().CreateLogs(context.Background(), exportertest.NewNopSettings(metadata.Type), cfg)
	require.ErrorContains(t, err, "connection_string is not valid base64")
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

	// The batch is far below 10 MiB and the 30s flush timeout hasn't passed, so nothing is uploaded yet.
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
