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

package restapireceiver

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/internal/useragent"
	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/restapireceiver/internal/metadata"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
)

func TestUserAgent(t *testing.T) {
	buildInfo := component.BuildInfo{Command: "test-collector", Version: "v1.2.3"}

	tests := []struct {
		name      string
		configure func(*confighttp.ClientConfig)
		want      string
	}{
		{
			name:      "from build info",
			configure: func(*confighttp.ClientConfig) {},
			want:      useragent.String(buildInfo),
		},
		{
			name: "configured header wins",
			configure: func(cc *confighttp.ClientConfig) {
				cc.Headers.Set("User-Agent", "custom-agent")
			},
			want: "custom-agent",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := make(chan string, 10)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				select {
				case got <- r.Header.Get("User-Agent"):
				default:
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode([]map[string]any{{"id": "1"}})
			}))
			defer server.Close()

			cfg := &Config{
				URL:             server.URL,
				AuthMode:        authModeNone,
				Pagination:      PaginationConfig{Mode: paginationModeNone},
				MaxPollInterval: 100 * time.Millisecond,
			}
			tc.configure(&cfg.ClientConfig)

			params := receivertest.NewNopSettings(metadata.Type)
			params.BuildInfo = buildInfo
			receiver, err := newRESTAPILogsReceiver(params, cfg, new(consumertest.LogsSink))
			require.NoError(t, err)

			ctx := context.Background()
			require.NoError(t, receiver.Start(ctx, componenttest.NewNopHost()))
			defer func() { require.NoError(t, receiver.Shutdown(ctx)) }()

			select {
			case ua := <-got:
				require.Equal(t, tc.want, ua)
			case <-time.After(5 * time.Second):
				t.Fatal("timed out waiting for request")
			}
		})
	}
}
