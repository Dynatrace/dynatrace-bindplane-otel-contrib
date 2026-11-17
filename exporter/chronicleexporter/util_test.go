// Copyright Dynatrace LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chronicleexporter

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/base64"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/oauth2"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const publicTokenURL = "https://oauth2.googleapis.com/token"

// jwtClaims holds the JWT assertion claims checked by these tests.
type jwtClaims struct {
	Iss   string `json:"iss"`
	Scope string `json:"scope"`
	Aud   string `json:"aud"`
}

// fakeTokenServer is an OAuth2 token endpoint that records the JWT assertion
// claims of each request it receives.
type fakeTokenServer struct {
	*httptest.Server
	mu     sync.Mutex
	claims []jwtClaims
}

func newFakeTokenServer(t *testing.T) *fakeTokenServer {
	t.Helper()
	s := &fakeTokenServer{}
	s.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		parts := strings.Split(r.PostForm.Get("assertion"), ".")
		if len(parts) != 3 {
			http.Error(w, "malformed assertion", http.StatusBadRequest)
			return
		}
		payload, err := base64.RawURLEncoding.DecodeString(parts[1])
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		var c jwtClaims
		if err := json.Unmarshal(payload, &c); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		s.mu.Lock()
		s.claims = append(s.claims, c)
		s.mu.Unlock()

		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"tok","token_type":"Bearer","expires_in":3600}`))
	}))
	t.Cleanup(s.Close)
	return s
}

func (s *fakeTokenServer) recorded() []jwtClaims {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]jwtClaims(nil), s.claims...)
}

// testPrivateKey is a PKCS#8 encoded RSA key, generated once and shared by all
// tests in the package.
var testPrivateKey = sync.OnceValues(func() ([]byte, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return nil, err
	}
	return x509.MarshalPKCS8PrivateKey(key)
})

// serviceAccountJSON returns a service account key with a generated private
// key. Entries in extra are added to, or override, the key's fields.
func serviceAccountJSON(t *testing.T, extra map[string]string) string {
	t.Helper()
	der, err := testPrivateKey()
	require.NoError(t, err)

	fields := map[string]string{
		"type":           "service_account",
		"project_id":     "test-project",
		"private_key_id": "test-key-id",
		"private_key":    string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: der})),
		"client_email":   "test@test-project.iam.gserviceaccount.com",
		"token_uri":      publicTokenURL,
	}
	for k, v := range extra {
		fields[k] = v
	}
	b, err := json.Marshal(fields)
	require.NoError(t, err)
	return string(b)
}

func TestTokenSourceWithTokenURL(t *testing.T) {
	testCases := []struct {
		desc          string
		protocol      string
		useCredsFile  bool
		extra         map[string]string
		expectedScope string
		expectedAud   string
	}{
		{
			desc:          "gRPC with inline creds",
			protocol:      protocolGRPC,
			expectedScope: grpcScope,
			expectedAud:   publicTokenURL,
		},
		{
			desc:          "https with creds file",
			protocol:      protocolHTTPS,
			useCredsFile:  true,
			expectedScope: httpScope,
			expectedAud:   publicTokenURL,
		},
		{
			desc:          "private token_uri in key does not change audience",
			protocol:      protocolGRPC,
			extra:         map[string]string{"token_uri": "https://oauth2-myendpoint.p.googleapis.com/token"},
			expectedScope: grpcScope,
			expectedAud:   publicTokenURL,
		},
		{
			desc:          "audience in key is preserved",
			protocol:      protocolGRPC,
			extra:         map[string]string{"audience": "https://custom.example.com/token"},
			expectedScope: grpcScope,
			expectedAud:   "https://custom.example.com/token",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			server := newFakeTokenServer(t)
			creds := serviceAccountJSON(t, tc.extra)

			cfg := &Config{Protocol: tc.protocol, TokenURL: server.URL}
			if tc.useCredsFile {
				path := filepath.Join(t.TempDir(), "creds.json")
				require.NoError(t, os.WriteFile(path, []byte(creds), 0o600))
				cfg.CredsFilePath = path
			} else {
				cfg.Creds = creds
			}

			ts, err := tokenSource(context.Background(), cfg)
			require.NoError(t, err)
			tok, err := ts.Token()
			require.NoError(t, err)
			require.Equal(t, "tok", tok.AccessToken)

			// The request went to token_url even though the key's token_uri is the
			// public endpoint, and the audience stayed on the public endpoint.
			claims := server.recorded()
			require.Len(t, claims, 1)
			require.Equal(t, tc.expectedAud, claims[0].Aud)
			require.Equal(t, tc.expectedScope, claims[0].Scope)
			require.Equal(t, "test@test-project.iam.gserviceaccount.com", claims[0].Iss)
		})
	}
}

func TestTokenSourceWithoutTokenURLUsesKeyTokenURI(t *testing.T) {
	// Without token_url, the key's token_uri is used for both the request and
	// the audience. This is the existing behavior.
	server := newFakeTokenServer(t)
	cfg := &Config{
		Protocol: protocolGRPC,
		Creds:    serviceAccountJSON(t, map[string]string{"token_uri": server.URL}),
	}

	ts, err := tokenSource(context.Background(), cfg)
	require.NoError(t, err)
	_, err = ts.Token()
	require.NoError(t, err)

	claims := server.recorded()
	require.Len(t, claims, 1)
	require.Equal(t, server.URL, claims[0].Aud)
}

func TestTokenSourceWithTokenURLErrors(t *testing.T) {
	emptyFile := filepath.Join(t.TempDir(), "empty.json")
	require.NoError(t, os.WriteFile(emptyFile, nil, 0o600))

	testCases := []struct {
		desc        string
		cfg         *Config
		expectedErr string
	}{
		{
			desc: "non service account credentials",
			cfg: &Config{
				Creds:    `{"type":"authorized_user","client_id":"id","client_secret":"secret","refresh_token":"token"}`,
				TokenURL: "https://oauth2-myendpoint.p.googleapis.com/token",
			},
			expectedErr: "token_url requires service account credentials",
		},
		{
			desc: "missing credentials file",
			cfg: &Config{
				CredsFilePath: filepath.Join(t.TempDir(), "missing.json"),
				TokenURL:      "https://oauth2-myendpoint.p.googleapis.com/token",
			},
			expectedErr: "read credentials file",
		},
		{
			desc: "empty credentials file",
			cfg: &Config{
				CredsFilePath: emptyFile,
				TokenURL:      "https://oauth2-myendpoint.p.googleapis.com/token",
			},
			expectedErr: "credentials file is empty",
		},
		{
			desc: "no credentials",
			cfg: &Config{
				TokenURL: "https://oauth2-myendpoint.p.googleapis.com/token",
			},
			expectedErr: "no credentials configured",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			_, err := tokenSource(context.Background(), tc.cfg)
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.expectedErr)
		})
	}
}

func TestTokenSourceTimeout(t *testing.T) {
	// A token endpoint that never responds must not stall token requests
	// indefinitely.
	release := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		<-release
	}))
	t.Cleanup(server.Close)
	t.Cleanup(func() { close(release) })

	defaultTimeout := tokenRequestTimeout
	tokenRequestTimeout = 100 * time.Millisecond
	t.Cleanup(func() { tokenRequestTimeout = defaultTimeout })

	testCases := []struct {
		desc string
		cfg  *Config
	}{
		{
			desc: "with token_url",
			cfg:  &Config{Protocol: protocolGRPC, Creds: serviceAccountJSON(t, nil), TokenURL: server.URL},
		},
		{
			desc: "without token_url",
			cfg:  &Config{Protocol: protocolGRPC, Creds: serviceAccountJSON(t, map[string]string{"token_uri": server.URL})},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ts, err := tokenSource(context.Background(), tc.cfg)
			require.NoError(t, err)

			start := time.Now()
			_, err = ts.Token()
			require.Error(t, err)
			require.Less(t, time.Since(start), 5*time.Second)
		})
	}
}

func TestUnavailableTokenSource(t *testing.T) {
	retrieveError := func(code int) error {
		return &oauth2.RetrieveError{Response: &http.Response{StatusCode: code}, ErrorCode: "error"}
	}

	testCases := []struct {
		desc         string
		err          error
		expectedCode codes.Code
	}{
		{
			desc:         "transport error",
			err:          errors.New("token endpoint unreachable"),
			expectedCode: codes.Unavailable,
		},
		{
			desc:         "server error",
			err:          retrieveError(http.StatusServiceUnavailable),
			expectedCode: codes.Unavailable,
		},
		{
			desc:         "request timeout",
			err:          retrieveError(http.StatusRequestTimeout),
			expectedCode: codes.Unavailable,
		},
		{
			desc:         "too many requests",
			err:          retrieveError(http.StatusTooManyRequests),
			expectedCode: codes.Unavailable,
		},
		{
			desc:         "credentials rejected",
			err:          retrieveError(http.StatusBadRequest),
			expectedCode: codes.Unknown,
		},
		{
			desc:         "wrapped credentials rejection",
			err:          fmt.Errorf("wrapped: %w", retrieveError(http.StatusUnauthorized)),
			expectedCode: codes.Unknown,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.desc, func(t *testing.T) {
			ts := unavailableTokenSource{errTokenSource{err: tc.err}}
			_, err := ts.Token()
			require.Error(t, err)
			require.Equal(t, tc.expectedCode, status.Code(err))
			require.ErrorContains(t, err, tc.err.Error())
		})
	}

	t.Run("token is passed through", func(t *testing.T) {
		ts := unavailableTokenSource{oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "tok"})}
		tok, err := ts.Token()
		require.NoError(t, err)
		require.Equal(t, "tok", tok.AccessToken)
	})
}

// errTokenSource returns err, or a transport error if err is nil.
type errTokenSource struct {
	err error
}

func (s errTokenSource) Token() (*oauth2.Token, error) {
	if s.err != nil {
		return nil, s.err
	}
	return nil, errors.New("token endpoint unreachable")
}
