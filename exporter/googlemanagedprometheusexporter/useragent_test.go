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

package googlemanagedprometheusexporter

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter/exportertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestUserAgentHeader drives the wrapper factory against a local gRPC server
// and checks the header actually sent on the wire.
func TestUserAgentHeader(t *testing.T) {
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
	defer srv.Stop()

	ctx := context.Background()
	set := exportertest.NewNopSettings(component.MustNewType("googlemanagedprometheus"))
	set.BuildInfo.Version = "v1.2.3"

	cfg := createDefaultConfig().(*Config)
	cfg.GMPConfig.GMPConfig.ProjectID = "test-project"
	cfg.GMPConfig.GMPConfig.MetricConfig.ClientConfig.Endpoint = lis.Addr().String()
	cfg.GMPConfig.GMPConfig.MetricConfig.ClientConfig.UseInsecure = true

	exp, err := NewFactory().CreateMetrics(ctx, set, cfg)
	require.NoError(t, err)
	require.NoError(t, exp.Start(ctx, componenttest.NewNopHost()))
	defer func() { _ = exp.Shutdown(ctx) }()

	md := pmetric.NewMetrics()
	m := md.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
	m.SetName("test_metric")
	dp := m.SetEmptyGauge().DataPoints().AppendEmpty()
	dp.SetIntValue(1)
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
	require.NoError(t, exp.ConsumeMetrics(ctx, md))

	select {
	case ua := <-got:
		t.Log("user-agent:", ua)
		// grpc-go appends its own " grpc-go/x.y.z" token.
		require.True(t, strings.HasPrefix(ua, "StanzaLogAgent/v1.2.3 "), ua)
	case <-time.After(10 * time.Second):
		t.Fatal("no request reached the test server")
	}
}
