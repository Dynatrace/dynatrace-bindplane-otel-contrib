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
	"net/http"
	"net/http/httptest"
	"sync"
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

// recorder captures what a test server saw, guarded for the race detector.
type recorder struct {
	mu   sync.Mutex
	reqs []*http.Request
}

func (rec *recorder) handler(status int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		rec.mu.Lock()
		rec.reqs = append(rec.reqs, r.Clone(context.Background()))
		rec.mu.Unlock()
		w.WriteHeader(status)
	}
}

func (rec *recorder) seen() []*http.Request {
	rec.mu.Lock()
	defer rec.mu.Unlock()
	return append([]*http.Request(nil), rec.reqs...)
}

// clearProxyEnv isolates a test from proxy settings in the developer's or the
// CI runner's environment, which the probe honours.
func clearProxyEnv(t *testing.T) {
	for _, k := range []string{"HTTP_PROXY", "http_proxy", "HTTPS_PROXY", "https_proxy", "NO_PROXY", "no_proxy", "REQUEST_METHOD"} {
		t.Setenv(k, "")
	}
}

func TestHTTPPinger_NegotiatesHTTP2(t *testing.T) {
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.EnableHTTP2 = true
	srv.StartTLS()
	defer srv.Close()

	r := probeHTTP(t, httpTarget(srv.URL, func(tc *TargetConfig) { tc.TLS.InsecureSkipVerify = true }))
	require.Equal(t, http.StatusOK, r.StatusCode, r.ErrMessage)
	require.Equal(t, "HTTP/2.0", r.Protocol)
	require.NotNil(t, r.TLS)
	require.Equal(t, "h2", r.TLS.NegotiatedProtocol)
}

func TestHTTPPinger_ProxyURL(t *testing.T) {
	clearProxyEnv(t)
	var rec recorder
	proxy := httptest.NewServer(rec.handler(http.StatusNoContent))
	defer proxy.Close()

	// The origin is never resolved or dialled: a forward proxy receives the
	// absolute URL and does that itself.
	r := probeHTTP(t, httpTarget("http://origin.example.invalid/health", func(tc *TargetConfig) { tc.ProxyURL = proxy.URL }))
	require.Equal(t, http.StatusNoContent, r.StatusCode, r.ErrMessage)
	seen := rec.seen()
	require.Len(t, seen, 1)
	require.Equal(t, "http://origin.example.invalid/health", seen[0].URL.String())
	require.Empty(t, r.ResolvedIP, "the address connected to was the proxy's")
}

func TestNewHTTPPinger_InvalidProxyURLCarriesNoCredentials(t *testing.T) {
	_, err := newHTTPPinger(context.Background(), componenttest.NewNopHost(), componenttest.NewNopTelemetrySettings(),
		httpTarget("http://example.test/", func(tc *TargetConfig) { tc.ProxyURL = `http://u:hun"ter2@proxy.test:3128` }), "")
	require.ErrorContains(t, err, "invalid proxy_url")
	require.NotContains(t, err.Error(), "ter2")
}

func TestHTTPPinger_ProxyFromEnvironment(t *testing.T) {
	var rec recorder
	proxy := httptest.NewServer(rec.handler(http.StatusForbidden))
	defer proxy.Close()

	t.Run("HTTP_PROXY", func(t *testing.T) {
		clearProxyEnv(t)
		t.Setenv("HTTP_PROXY", proxy.URL)
		before := len(rec.seen())
		r := probeHTTP(t, httpTarget("http://origin.example.invalid/", nil))
		require.Equal(t, http.StatusForbidden, r.StatusCode, r.ErrMessage)
		require.Len(t, rec.seen(), before+1)
	})

	t.Run("HTTPS_PROXY", func(t *testing.T) {
		clearProxyEnv(t)
		t.Setenv("HTTPS_PROXY", proxy.URL)
		before := len(rec.seen())
		// The proxy refuses the tunnel, so the probe fails; it still has to
		// have asked the proxy rather than gone direct.
		r := probeHTTP(t, httpTarget("https://origin.example.invalid/", nil))
		require.Equal(t, 0, r.StatusCode)
		seen := rec.seen()
		require.Len(t, seen, before+1)
		require.Equal(t, http.MethodConnect, seen[before].Method)
		require.Equal(t, "origin.example.invalid:443", seen[before].Host)
	})

	t.Run("NO_PROXY", func(t *testing.T) {
		clearProxyEnv(t)
		t.Setenv("HTTP_PROXY", proxy.URL)
		t.Setenv("NO_PROXY", ".example.invalid")
		p := newTestHTTPPinger(t, httpTarget("http://origin.example.invalid/", nil))
		require.False(t, p.proxied)
	})
}

func TestHTTPPinger_SendsConfiguredHeaders(t *testing.T) {
	var rec recorder
	srv := httptest.NewServer(rec.handler(http.StatusOK))
	defer srv.Close()
	addr := srv.Listener.Addr().String()

	r := probeHTTP(t, httpTarget("http://admin:s3cret@"+addr+"/", func(tc *TargetConfig) {
		tc.Headers.Set("X-Probe", "abc")
		tc.Headers.Set("Host", "virtual.example")
	}))
	require.Equal(t, http.StatusOK, r.StatusCode, r.ErrMessage)

	// A configured Authorization header takes precedence over userinfo.
	r = probeHTTP(t, httpTarget("http://admin:s3cret@"+addr+"/", func(tc *TargetConfig) {
		tc.Headers.Set("Authorization", "Bearer t0ken")
	}))
	require.Equal(t, http.StatusOK, r.StatusCode, r.ErrMessage)

	seen := rec.seen()
	require.Len(t, seen, 2)
	require.Equal(t, "abc", seen[0].Header.Get("X-Probe"))
	require.Equal(t, "virtual.example", seen[0].Host)
	user, pass, ok := seen[0].BasicAuth()
	require.True(t, ok, "userinfo is sent as basic auth")
	require.Equal(t, "admin", user)
	require.Equal(t, "s3cret", pass)
	require.Equal(t, "Bearer t0ken", seen[1].Header.Get("Authorization"))
}
