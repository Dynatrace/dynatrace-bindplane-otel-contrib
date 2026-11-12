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

package bindplaneauditlogs

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/internal/useragent"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestUserAgent(t *testing.T) {
	buildInfo := component.BuildInfo{Command: "test-collector", Version: "v1.2.3"}

	tests := []struct {
		name      string
		configure func(*Config)
		want      string
	}{
		{
			name:      "from build info",
			configure: func(*Config) {},
			want:      useragent.String(buildInfo),
		},
		{
			name: "configured header wins",
			configure: func(cfg *Config) {
				cfg.ClientConfig.Headers.Set("User-Agent", "custom-agent")
			},
			want: "custom-agent",
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
				_, _ = w.Write([]byte(`{"auditEvents":[]}`))
			}))
			defer srv.Close()

			cfg := createDefaultConfig().(*Config)
			cfg.APIKey = "testkey"
			serverURL, err := url.Parse(srv.URL)
			require.NoError(t, err)
			cfg.bindplaneURL = serverURL
			tc.configure(cfg)

			set := receivertest.NewNopSettings(receivertest.NopType)
			set.BuildInfo = buildInfo
			rcv, err := createLogsReceiver(context.Background(), set, cfg, consumertest.NewNop())
			require.NoError(t, err)
			recv := rcv.(*bindplaneAuditLogsReceiver)

			require.NoError(t, recv.Start(context.Background(), componenttest.NewNopHost()))
			defer func() { require.NoError(t, recv.Shutdown(context.Background())) }()

			// Start polls immediately.
			select {
			case ua := <-got:
				require.Equal(t, tc.want, ua)
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for request")
			}
		})
	}
}
