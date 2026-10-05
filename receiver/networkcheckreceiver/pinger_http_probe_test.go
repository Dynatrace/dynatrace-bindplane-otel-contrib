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

package networkcheckreceiver

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
)

func httpTarget(endpoint string, mut func(*TargetConfig)) TargetConfig {
	tc := TargetConfig{Method: MethodHTTP}
	tc.Endpoint = endpoint
	tc.Timeout = 3 * time.Second
	if mut != nil {
		mut(&tc)
	}
	return tc
}

func newTestHTTPPinger(t *testing.T, tc TargetConfig) *httpPinger {
	t.Helper()
	p, err := newHTTPPinger(context.Background(), componenttest.NewNopHost(), componenttest.NewNopTelemetrySettings(), tc, "")
	require.NoError(t, err)
	return p
}

// probeHTTP runs one probe. A failed request is a measurement, so ping must
// not return an error for it.
func probeHTTP(t *testing.T, tc TargetConfig) PingResult {
	t.Helper()
	r, err := newTestHTTPPinger(t, tc).ping(context.Background())
	require.NoError(t, err)
	return r
}

// closedPort returns a loopback address with nothing listening on it.
func closedPort(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := l.Addr().String()
	require.NoError(t, l.Close())
	return addr
}

func TestNewHTTPPinger_ErrorsCarryNoCredentials(t *testing.T) {
	cases := []struct {
		name     string
		target   TargetConfig
		wantText string
	}{
		{
			// A quote is not valid in userinfo, so url.Parse fails and its
			// error quotes the whole raw URL. The free-text redaction stops at
			// the quote and would let the password through.
			name:     "unparseable password",
			target:   httpTarget(`http://admin:hun"ter2@example.test/`, nil),
			wantText: "invalid endpoint",
		},
		{
			name: "TLS config that fails to load",
			target: httpTarget("https://admin:hunter2@example.test/", func(tc *TargetConfig) {
				tc.TLS.CAFile = "/nonexistent/ca.pem"
			}),
			wantText: "loading TLS config",
		},
		{
			name:     "no host",
			target:   httpTarget("http://admin:hunter2@/", nil),
			wantText: "want an http or https URL",
		},
		{
			name:     "invalid method",
			target:   httpTarget("http://admin:hunter2@example.test/", func(tc *TargetConfig) { tc.HTTPMethod = "GE T" }),
			wantText: "invalid http_method",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := newHTTPPinger(context.Background(), componenttest.NewNopHost(), componenttest.NewNopTelemetrySettings(), tc.target, "")
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.wantText)
			require.NotContains(t, err.Error(), "hunter2")
			require.NotContains(t, err.Error(), "ter2")
		})
	}
}

// net/http masks only the password in the URL it quotes, so a token passed as
// the username used to reach error.message on every failed probe.
func TestHTTPPinger_ErrMessageCarriesNoCredentials(t *testing.T) {
	r := probeHTTP(t, httpTarget("http://opaque_tok3n:hunter2@"+closedPort(t)+"/", nil))
	require.Equal(t, 0, r.StatusCode)
	require.Equal(t, "connect", r.ErrPhase)
	require.Contains(t, r.ErrMessage, "refused")
	require.NotContains(t, r.ErrMessage, "opaque_tok3n")
	require.NotContains(t, r.ErrMessage, "hunter2")
}
