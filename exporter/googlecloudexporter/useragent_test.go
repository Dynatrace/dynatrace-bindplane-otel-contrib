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

package googlecloudexporter

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/GoogleCloudPlatform/opentelemetry-operations-go/exporter/collector"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// uaServer starts a plaintext gRPC server that records the user-agent of every
// call. No GCP services are registered; every call is answered Unimplemented.
func uaServer(t *testing.T) (string, <-chan string) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	got := make(chan string, 100)
	srv := grpc.NewServer(grpc.UnknownServiceHandler(func(_ any, stream grpc.ServerStream) error {
		md, _ := metadata.FromIncomingContext(stream.Context())
		select {
		case got <- strings.Join(md.Get("user-agent"), ","):
		default:
		}
		return status.Error(codes.Unimplemented, "user agent test server")
	}))
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis.Addr().String(), got
}

func requireUserAgent(t *testing.T, got <-chan string) {
	select {
	case ua := <-got:
		t.Log("user-agent:", ua)
		// grpc-go appends its own " grpc-go/x.y.z" token.
		require.True(t, strings.HasPrefix(ua, "StanzaLogAgent/v1.2.3 "), ua)
	case <-time.After(10 * time.Second):
		t.Fatal("no request reached the test server")
	}
}

// TestUserAgentHeader drives each signal through the wrapper factory against a
// local gRPC server and checks the header actually sent on the wire.
func TestUserAgentHeader(t *testing.T) {
	ctx := context.Background()
	set := exportertest.NewNopSettings(typ)
	set.BuildInfo.Version = "v1.2.3"

	newCfg := func(t *testing.T) (*Config, <-chan string) {
		addr, got := uaServer(t)
		cfg := createDefaultConfig().(*Config)
		c := &cfg.GCPConfig.Config
		c.ProjectID = "test-project"
		for _, cc := range []*collector.ClientConfig{&c.MetricConfig.ClientConfig, &c.LogConfig.ClientConfig, &c.TraceConfig.ClientConfig} {
			cc.Endpoint, cc.UseInsecure = addr, true
		}
		return cfg, got
	}
	now := pcommon.NewTimestampFromTime(time.Now())

	t.Run("metrics", func(t *testing.T) {
		cfg, got := newCfg(t)
		exp, err := NewFactory().CreateMetrics(ctx, set, cfg)
		require.NoError(t, err)
		require.NoError(t, exp.Start(ctx, componenttest.NewNopHost()))
		defer func() { _ = exp.Shutdown(ctx) }()

		md := pmetric.NewMetrics()
		m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
		m.SetName("test.metric")
		dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
		dp.SetIntValue(1)
		dp.SetTimestamp(now)
		require.NoError(t, exp.ConsumeMetrics(ctx, md))
		requireUserAgent(t, got)
	})

	t.Run("logs", func(t *testing.T) {
		cfg, got := newCfg(t)
		exp, err := NewFactory().CreateLogs(ctx, set, cfg)
		require.NoError(t, err)
		require.NoError(t, exp.Start(ctx, componenttest.NewNopHost()))
		defer func() { _ = exp.Shutdown(ctx) }()

		ld := plog.NewLogs()
		ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetStr("hello")
		require.NoError(t, exp.ConsumeLogs(ctx, ld))
		requireUserAgent(t, got)
	})

	t.Run("traces", func(t *testing.T) {
		cfg, got := newCfg(t)
		exp, err := NewFactory().CreateTraces(ctx, set, cfg)
		require.NoError(t, err)
		require.NoError(t, exp.Start(ctx, componenttest.NewNopHost()))
		defer func() { _ = exp.Shutdown(ctx) }()

		td := ptrace.NewTraces()
		span := td.ResourceSpans().AppendEmpty().ScopeSpans().AppendEmpty().Spans().AppendEmpty()
		span.SetName("test")
		span.SetTraceID(pcommon.TraceID{1})
		span.SetSpanID(pcommon.SpanID{1})
		span.SetStartTimestamp(now)
		span.SetEndTimestamp(now)
		require.NoError(t, exp.ConsumeTraces(ctx, td))
		requireUserAgent(t, got)
	})
}
