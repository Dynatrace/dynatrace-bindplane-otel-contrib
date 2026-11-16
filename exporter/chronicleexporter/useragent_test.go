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

package chronicleexporter

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/exporter/chronicleexporter/protos/api"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"golang.org/x/oauth2"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

var testBuildInfo = component.BuildInfo{Command: "otelcol-test", Version: "v1.2.3"}

func testUserAgent() string {
	return "otelcol-test/v1.2.3 (" + runtime.GOOS + "/" + runtime.GOARCH + ")"
}

func userAgentTestLogs() plog.Logs {
	logs := plog.NewLogs()
	logs.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("Test")
	return logs
}

func overrideTokenSource(t *testing.T) {
	t.Helper()
	orig := tokenSource
	t.Cleanup(func() { tokenSource = orig })
	tokenSource = func(context.Context, *Config) (oauth2.TokenSource, error) {
		return &emptyTokenSource{}, nil
	}
}

func TestGRPCExporterUserAgent(t *testing.T) {
	overrideTokenSource(t)

	var (
		mu        sync.Mutex
		userAgent []string
	)
	srv := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		md, _ := metadata.FromIncomingContext(ctx)
		mu.Lock()
		userAgent = md.Get("user-agent")
		mu.Unlock()
		return handler(ctx, req)
	}))
	srv.RegisterService(&api.IngestionServiceV2_ServiceDesc, &mockGRPCServer{
		handler: func(*api.BatchCreateLogsRequest) (*api.BatchCreateLogsResponse, error) {
			return &api.BatchCreateLogsResponse{}, nil
		},
	})
	ln, err := net.Listen("tcp", "localhost:")
	require.NoError(t, err)
	go func() { _ = srv.Serve(ln) }()
	defer srv.GracefulStop()

	// The test override of the dial options does not set a User-Agent; the
	// exporter adds it on top.
	origParams := grpcClientParams
	defer func() { grpcClientParams = origParams }()
	grpcClientParams = func(string, oauth2.TokenSource) (string, []grpc.DialOption) {
		return ln.Addr().String(), []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}

	f := NewFactory()
	cfg := f.CreateDefaultConfig().(*Config)
	cfg.Protocol = protocolGRPC
	cfg.CustomerID = "00000000-1111-2222-3333-444444444444"
	cfg.LogType = "FAKE"
	cfg.CollectAgentMetrics = false
	cfg.QueueBatchConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
	cfg.BackOffConfig.Enabled = false
	require.NoError(t, cfg.Validate())

	ctx := context.Background()
	set := exportertest.NewNopSettings(typ)
	set.BuildInfo = testBuildInfo
	exp, err := f.CreateLogs(ctx, set, cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(ctx, componenttest.NewNopHost()))
	defer func() { require.NoError(t, exp.Shutdown(ctx)) }()

	require.NoError(t, exp.ConsumeLogs(ctx, userAgentTestLogs()))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, userAgent, 1)
	// grpc-go appends its own product after the configured User-Agent.
	require.True(t, strings.HasPrefix(userAgent[0], testUserAgent()+" grpc-go/"), userAgent[0])
}

func TestHTTPExporterUserAgent(t *testing.T) {
	overrideTokenSource(t)

	var (
		mu        sync.Mutex
		userAgent string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		userAgent = r.Header.Get("User-Agent")
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	origEndpoint := httpEndpoint
	defer func() { httpEndpoint = origEndpoint }()
	httpEndpoint = func(_ *Config, logType string) string {
		return fmt.Sprintf("%s/logTypes/%s/logs:import", srv.URL, logType)
	}

	testCases := []struct {
		name      string
		buildInfo component.BuildInfo
		expected  string
	}{
		{
			name:      "build info names the collector",
			buildInfo: testBuildInfo,
			expected:  testUserAgent(),
		},
		{
			name:     "build info is empty",
			expected: "Go-http-client/1.1",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			f := NewFactory()
			cfg := f.CreateDefaultConfig().(*Config)
			cfg.Protocol = protocolHTTPS
			cfg.Location = "us"
			cfg.CustomerID = "00000000-1111-2222-3333-444444444444"
			cfg.Project = "fake"
			cfg.Forwarder = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
			cfg.LogType = "FAKE"
			cfg.CollectAgentMetrics = false
			cfg.QueueBatchConfig = configoptional.None[exporterhelper.QueueBatchConfig]()
			cfg.BackOffConfig.Enabled = false
			require.NoError(t, cfg.Validate())

			ctx := context.Background()
			set := exportertest.NewNopSettings(typ)
			set.BuildInfo = tc.buildInfo
			exp, err := f.CreateLogs(ctx, set, cfg)
			require.NoError(t, err)
			require.NoError(t, exp.Start(ctx, componenttest.NewNopHost()))
			defer func() { require.NoError(t, exp.Shutdown(ctx)) }()

			require.NoError(t, exp.ConsumeLogs(ctx, userAgentTestLogs()))

			mu.Lock()
			defer mu.Unlock()
			require.Equal(t, tc.expected, userAgent)
		})
	}
}
