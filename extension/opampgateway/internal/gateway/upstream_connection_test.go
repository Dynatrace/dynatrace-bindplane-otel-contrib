// Copyright Dynatrace LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gateway

import (
	"net/http"
	"testing"

	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestUpstreamConnectionHeader(t *testing.T) {
	const userAgent = "opamp-gateway/v1.9.0 bindplane-otel-collector/v2.0.1 (linux/amd64)"

	testCases := []struct {
		name            string
		headers         http.Header
		gatewayID       string
		expectUserAgent string
		expectID        string
	}{
		{
			name:            "no configured headers",
			headers:         nil,
			expectUserAgent: userAgent,
			expectID:        "upstream-0",
		},
		{
			name:            "gateway ID prefixes the connection ID",
			gatewayID:       "01J9GATEWAY",
			expectUserAgent: userAgent,
			expectID:        "01J9GATEWAY/upstream-0",
		},
		{
			name:            "unrelated configured header",
			headers:         http.Header{"Authorization": []string{"Secret-Key secret"}},
			expectUserAgent: userAgent,
			expectID:        "upstream-0",
		},
		{
			name:            "configured User-Agent takes precedence",
			headers:         http.Header{"User-Agent": []string{"custom/1.0.0"}},
			expectUserAgent: "custom/1.0.0",
			expectID:        "upstream-0",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := newUpstreamConnection(*websocket.DefaultDialer, nil, upstreamConnectionSettings{
				endpoint:  "ws://localhost:4320/v1/opamp",
				headers:   tc.headers,
				userAgent: userAgent,
				gatewayID: tc.gatewayID,
			}, "upstream-0", zap.NewNop())

			h := c.header("upstream-0")
			require.Equal(t, tc.expectID, h.Get("X-Opamp-Gateway-Connection-Id"))
			require.Equal(t, tc.expectUserAgent, h.Get("User-Agent"))

			// the configured headers must not be modified
			require.Equal(t, tc.headers, c.settings.headers)
		})
	}
}
