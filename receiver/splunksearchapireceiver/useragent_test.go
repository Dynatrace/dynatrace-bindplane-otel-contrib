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

package splunksearchapireceiver

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

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
			var got string
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				got = r.Header.Get("User-Agent")
				w.WriteHeader(http.StatusInternalServerError)
			}))
			defer srv.Close()

			cfg := createDefaultConfig().(*Config)
			cfg.Endpoint = srv.URL
			tc.configure(cfg)

			set := receivertest.NewNopSettings(typeStr)
			set.BuildInfo = buildInfo
			rcv, err := createLogsReceiver(context.Background(), set, cfg, consumertest.NewNop())
			require.NoError(t, err)
			ssapir := rcv.(*splunksearchapireceiver)

			ctx := context.Background()
			require.NoError(t, ssapir.Start(ctx, componenttest.NewNopHost()))
			defer func() { require.NoError(t, ssapir.Shutdown(ctx)) }()

			// Only the request matters, not the response.
			_, _ = ssapir.client.GetJobStatus("123456")
			require.Equal(t, tc.want, got)
		})
	}
}
