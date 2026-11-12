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

package useragent

import (
	"net/http"
	"net/http/httptest"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
)

func TestProduct(t *testing.T) {
	testCases := []struct {
		name      string
		buildInfo component.BuildInfo
		expected  string
	}{
		{
			name:      "reverse-DNS command",
			buildInfo: component.BuildInfo{Command: "com.dynatrace.bindplane.otel.collector", Version: "v1.4.0"},
			expected:  "com.dynatrace.bindplane.otel.collector/v1.4.0",
		},
		{
			name:      "unix path command",
			buildInfo: component.BuildInfo{Command: "/opt/collector/otelcol-contrib", Version: "0.164.0"},
			expected:  "otelcol-contrib/0.164.0",
		},
		{
			name:      "windows path command",
			buildInfo: component.BuildInfo{Command: `C:\Program Files\collector\otelcol.exe`, Version: "1.0.0"},
			expected:  "otelcol/1.0.0",
		},
		{
			name:      "spaces and separators are replaced",
			buildInfo: component.BuildInfo{Command: "my collector", Version: "1.0 beta/2"},
			expected:  "my-collector/1.0-beta-2",
		},
		{
			name:      "missing command",
			buildInfo: component.BuildInfo{Version: "1.0.0"},
			expected:  "",
		},
		{
			name:      "missing version",
			buildInfo: component.BuildInfo{Command: "otelcol"},
			expected:  "",
		},
		{
			name:      "nothing usable",
			buildInfo: component.BuildInfo{Command: "/", Version: "  "},
			expected:  "",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.expected, Product(tc.buildInfo))
		})
	}
}

func TestString(t *testing.T) {
	buildInfo := component.BuildInfo{Command: "otelcol", Version: "1.0.0"}
	require.Equal(t, "otelcol/1.0.0 ("+runtime.GOOS+"/"+runtime.GOARCH+")", String(buildInfo))
	require.Empty(t, String(component.BuildInfo{}))
}

func TestNewTransport(t *testing.T) {
	var got string
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("User-Agent")
	}))
	defer server.Close()

	t.Run("sets the user agent when none is set", func(t *testing.T) {
		client := &http.Client{Transport: NewTransport(nil, "otelcol/1.0.0")}
		req, err := http.NewRequest(http.MethodGet, server.URL, nil)
		require.NoError(t, err)

		resp, err := client.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, "otelcol/1.0.0", got)
		require.Empty(t, req.Header.Get("User-Agent"), "the caller's request must not be modified")
	})

	t.Run("keeps a user agent already on the request", func(t *testing.T) {
		client := &http.Client{Transport: NewTransport(nil, "otelcol/1.0.0")}
		req, err := http.NewRequest(http.MethodGet, server.URL, nil)
		require.NoError(t, err)
		req.Header.Set("User-Agent", "configured/1.0")

		resp, err := client.Do(req)
		require.NoError(t, err)
		require.NoError(t, resp.Body.Close())
		require.Equal(t, "configured/1.0", got)
	})

	t.Run("empty user agent returns base", func(t *testing.T) {
		base := &http.Transport{}
		require.Same(t, base, NewTransport(base, ""))
	})
}
