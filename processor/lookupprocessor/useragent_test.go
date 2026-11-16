// Copyright  Dynatrace LLC
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

package lookupprocessor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/internal/useragent"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/processor/processortest"
)

func TestUserAgent(t *testing.T) {
	buildInfo := component.BuildInfo{Command: "test-collector", Version: "v1.2.3"}

	tests := []struct {
		name   string
		source func(url string) *Config
		want   string
	}{
		{
			name: "api from build info",
			source: func(url string) *Config {
				return &Config{API: &APIConfig{URL: url + "/${fieldValue}"}}
			},
			want: useragent.String(buildInfo),
		},
		{
			name: "api configured header wins",
			source: func(url string) *Config {
				return &Config{API: &APIConfig{
					URL:     url + "/${fieldValue}",
					Headers: map[string]string{"User-Agent": "custom-agent"},
				}}
			},
			want: "custom-agent",
		},
		{
			name: "puppetdb from build info",
			source: func(url string) *Config {
				return &Config{PuppetDB: &PuppetDBConfig{Endpoint: url, Facts: fixtureFacts()}}
			},
			want: useragent.String(buildInfo),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := make(chan string, 10)
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case got <- r.Header.Get("User-Agent"):
				default:
				}
				_, _ = w.Write([]byte(`{}`))
			}))
			defer srv.Close()

			cfg := tc.source(srv.URL)
			cfg.Context = resourceContext
			cfg.Field = "host.name"

			set := processortest.NewNopSettings(componentType)
			set.BuildInfo = buildInfo
			p, err := NewFactory().CreateLogs(context.Background(), set, cfg, consumertest.NewNop())
			require.NoError(t, err)
			require.NoError(t, p.Start(context.Background(), componenttestHost{}))
			defer func() { require.NoError(t, p.Shutdown(context.Background())) }()

			// The puppetdb source loads on start; the api source queries per lookup.
			logs := plog.NewLogs()
			logs.ResourceLogs().AppendEmpty().Resource().Attributes().PutStr("host.name", "web-1")
			require.NoError(t, p.ConsumeLogs(context.Background(), logs))

			select {
			case ua := <-got:
				require.Equal(t, tc.want, ua)
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for request")
			}
		})
	}
}
