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
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configtls"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest/observer"
)

// fixtureFacts is the fact selection the testdata/puppetdb fixtures were
// captured with, against live PuppetDB 8.8.1, OpenVoxDB 8.17.0, OpenVoxDB
// 9.1.0, and OpenVoxDB 9.1.0 configured with PuppetDB 9's mTLS-only defaults.
func fixtureFacts() map[string]string {
	return map[string]string{
		"facts.os.family":            "puppet.os.family",
		"trusted.extensions.pp_role": "puppet.role",
		"facts.processors.count":     "puppet.cpus",
		"facts.is_virtual":           "puppet.virtual",
		"facts.no.such.fact":         "puppet.missing",
	}
}

// fixtureExtract is the extract the fixtures answer: certname first, then the
// fact paths sorted so the query is stable regardless of map order.
const fixtureExtract = `["extract",["certname","facts.is_virtual","facts.no.such.fact","facts.os.family","facts.processors.count","trusted.extensions.pp_role"]]`

// puppetDBServer serves recorded inventory pages and records the requests it
// received.
type puppetDBServer struct {
	*httptest.Server
	mu       sync.Mutex
	requests []url.Values
	status   atomic.Int64 // non-zero forces this status with body
	body     atomic.Value // string
}

func (s *puppetDBServer) recorded() []url.Values {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]url.Values(nil), s.requests...)
}

func (s *puppetDBServer) fail(status int, body string) {
	s.body.Store(body)
	s.status.Store(int64(status))
}

func (s *puppetDBServer) recover() { s.status.Store(0) }

// puppetDBHandler answers /pdb/query/v4/inventory from the recorded pages of
// one server, keyed by offset. Pages were captured with limit 20.
func puppetDBHandler(s *puppetDBServer, server string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.mu.Lock()
		s.requests = append(s.requests, r.URL.Query())
		s.mu.Unlock()

		if status := s.status.Load(); status != 0 {
			w.WriteHeader(int(status))
			_, _ = w.Write([]byte(s.body.Load().(string)))
			return
		}
		if r.URL.Path != "/pdb/query/v4/inventory" || r.Method != http.MethodGet {
			http.NotFound(w, r)
			return
		}
		offset := r.URL.Query().Get("offset")
		data, err := os.ReadFile(filepath.Join("testdata", "puppetdb", server+"-offset"+offset+".json"))
		if err != nil {
			_, _ = w.Write([]byte("[]"))
			return
		}
		_, _ = w.Write(data)
	})
}

func newPuppetDBServer(t *testing.T, server string) *puppetDBServer {
	t.Helper()
	s := &puppetDBServer{}
	s.Server = httptest.NewServer(puppetDBHandler(s, server))
	t.Cleanup(s.Close)
	return s
}

func newTestPuppetDBSource(t *testing.T, cfg *PuppetDBConfig) (*PuppetDBSource, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	src, err := NewPuppetDBSource(cfg, zap.New(core))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, src.Close()) })
	return src, logs
}

func TestPuppetDBConfigValidate(t *testing.T) {
	valid := func() *PuppetDBConfig {
		return &PuppetDBConfig{Endpoint: "https://puppetdb:8081", Facts: map[string]string{"facts.os.family": "os"}}
	}
	testCases := []struct {
		name   string
		mutate func(*PuppetDBConfig)
		err    error
	}{
		{name: "valid", mutate: func(*PuppetDBConfig) {}},
		{name: "environment and trusted paths", mutate: func(c *PuppetDBConfig) {
			c.Facts = map[string]string{"environment": "env", "trusted.extensions.pp_role": "role", `facts.mountpoints."/".filesystem`: "fs"}
		}},
		{name: "valid query", mutate: func(c *PuppetDBConfig) { c.Query = `["=", "environment", "production"]` }},
		{name: "missing endpoint", mutate: func(c *PuppetDBConfig) { c.Endpoint = "" }, err: errMissingPuppetDBEndpoint},
		{name: "endpoint without scheme", mutate: func(c *PuppetDBConfig) { c.Endpoint = "puppetdb:8081" }, err: errInvalidPuppetDBEndpoint},
		{name: "no facts", mutate: func(c *PuppetDBConfig) { c.Facts = nil }, err: errMissingPuppetDBFacts},
		{name: "path without facts or trusted prefix", mutate: func(c *PuppetDBConfig) { c.Facts = map[string]string{"os.family": "os"} }, err: errInvalidPuppetDBFact},
		{name: "bare facts prefix", mutate: func(c *PuppetDBConfig) { c.Facts = map[string]string{"facts.": "os"} }, err: errInvalidPuppetDBFact},
		{name: "array index", mutate: func(c *PuppetDBConfig) { c.Facts = map[string]string{"facts.processors.models[0]": "cpu"} }, err: errInvalidPuppetDBFact},
		{name: "certname is the key, not a fact", mutate: func(c *PuppetDBConfig) { c.Facts = map[string]string{"certname": "name"} }, err: errInvalidPuppetDBFact},
		{name: "empty attribute", mutate: func(c *PuppetDBConfig) { c.Facts = map[string]string{"facts.os.family": ""} }, err: errInvalidPuppetDBAttr},
		{name: "duplicate attribute", mutate: func(c *PuppetDBConfig) {
			c.Facts = map[string]string{"facts.os.family": "os", "facts.os.name": "os"}
		}, err: errInvalidPuppetDBAttr},
		{name: "query not JSON", mutate: func(c *PuppetDBConfig) { c.Query = `environment = "production"` }, err: errInvalidPuppetDBQuery},
		{name: "query JSON object", mutate: func(c *PuppetDBConfig) { c.Query = `{"environment":"production"}` }, err: errInvalidPuppetDBQuery},
		{name: "negative timeout", mutate: func(c *PuppetDBConfig) { c.Timeout = -time.Second }, err: errNegativePuppetDBSetting},
		{name: "negative page_size", mutate: func(c *PuppetDBConfig) { c.PageSize = -1 }, err: errNegativePuppetDBSetting},
		{name: "negative max_nodes", mutate: func(c *PuppetDBConfig) { c.MaxNodes = -1 }, err: errNegativePuppetDBSetting},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := valid()
			tc.mutate(cfg)
			err := cfg.validate()
			if tc.err == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.err)
		})
	}
}

func TestConfigValidatePuppetDB(t *testing.T) {
	pdb := &PuppetDBConfig{Endpoint: "http://puppetdb:8080", Facts: map[string]string{"facts.os.family": "os"}}
	testCases := []struct {
		name string
		cfg  Config
		err  error
	}{
		{name: "puppetdb alone", cfg: Config{Context: resourceContext, Field: "host.name", PuppetDB: pdb}},
		{name: "puppetdb with matching source_type", cfg: Config{Context: resourceContext, Field: "host.name", SourceType: sourceTypePuppetDB, PuppetDB: pdb}},
		{name: "puppetdb with csv", cfg: Config{Context: resourceContext, Field: "host.name", CSV: "x.csv", PuppetDB: pdb}, err: errMultipleSources},
		{name: "source_type puppetdb without block", cfg: Config{Context: resourceContext, Field: "host.name", SourceType: sourceTypePuppetDB, CSV: "x.csv"}, err: errSourceTypeMismatch},
		{name: "invalid puppetdb block", cfg: Config{Context: resourceContext, Field: "host.name", PuppetDB: &PuppetDBConfig{Facts: pdb.Facts}}, err: errMissingPuppetDBEndpoint},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.cfg.Validate()
			if tc.err == nil {
				require.NoError(t, err)
				return
			}
			require.ErrorIs(t, err, tc.err)
		})
	}
}

// TestPuppetDBLoadAcrossServers runs the same load against the pages each
// server returned. They were byte-identical when captured; this keeps that
// claim checked if a fixture is ever re-recorded.
func TestPuppetDBLoadAcrossServers(t *testing.T) {
	for _, server := range []string{"pdb8", "ovx8", "ovx9", "ovx9tls"} {
		t.Run(server, func(t *testing.T) {
			s := newPuppetDBServer(t, server)
			src, _ := newTestPuppetDBSource(t, &PuppetDBConfig{Endpoint: s.URL, Facts: fixtureFacts(), PageSize: 20})

			require.NoError(t, src.LoadContext(context.Background()))

			got, err := src.Lookup(context.Background(), "web01.example.com")
			require.NoError(t, err)
			require.Equal(t, map[string]string{
				"puppet.os.family": "Debian",
				"puppet.role":      "web",
				"puppet.cpus":      "20",
				"puppet.virtual":   "true",
			}, got, "null facts are omitted and numbers and bools are stringified")

			got, err = src.Lookup(context.Background(), "node40.example.com")
			require.NoError(t, err)
			require.Equal(t, "RedHat", got["puppet.os.family"])

			reqs := s.recorded()
			require.Len(t, reqs, 3, "50 nodes in pages of 20 take three requests; the short last page ends paging")
			for i, q := range reqs {
				require.JSONEq(t, fixtureExtract, q.Get("query"))
				require.JSONEq(t, `[{"field":"certname"}]`, q.Get("order_by"))
				require.Equal(t, "20", q.Get("limit"))
				require.Equal(t, strconv.Itoa(i*20), q.Get("offset"))
			}
		})
	}
}

func TestPuppetDBQueryFilterIsAnded(t *testing.T) {
	s := newPuppetDBServer(t, "ovx9")
	src, _ := newTestPuppetDBSource(t, &PuppetDBConfig{
		Endpoint: s.URL, Facts: fixtureFacts(), PageSize: 20,
		Query: `["=", "environment", "production"]`,
	})
	require.NoError(t, src.LoadContext(context.Background()))

	want := `["extract",["certname","facts.is_virtual","facts.no.such.fact","facts.os.family","facts.processors.count","trusted.extensions.pp_role"],["=","environment","production"]]`
	require.JSONEq(t, want, s.recorded()[0].Get("query"))
}

func TestPuppetDBDefaultsAndEndpointPath(t *testing.T) {
	var gotPath, gotLimit string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotLimit = r.URL.Path, r.URL.Query().Get("limit")
		_, _ = w.Write([]byte("[]"))
	}))
	t.Cleanup(srv.Close)

	src, _ := newTestPuppetDBSource(t, &PuppetDBConfig{Endpoint: srv.URL + "/", Facts: fixtureFacts()})
	require.NoError(t, src.LoadContext(context.Background()))
	require.Equal(t, "/pdb/query/v4/inventory", gotPath, "a trailing slash on the endpoint must not double up")
	require.Equal(t, strconv.Itoa(puppetDBDefaultPageSize), gotLimit)
}

func TestPuppetDBLookupBeforeLoadAndMiss(t *testing.T) {
	s := newPuppetDBServer(t, "ovx9")
	src, _ := newTestPuppetDBSource(t, &PuppetDBConfig{Endpoint: s.URL, Facts: fixtureFacts(), PageSize: 20})

	_, err := src.Lookup(context.Background(), "web01.example.com")
	require.ErrorIs(t, err, errPuppetDBNotLoaded)

	require.NoError(t, src.Load())
	_, err = src.Lookup(context.Background(), "nope.example.com")
	require.ErrorIs(t, err, errKeyNotFound)
	_, err = src.Lookup(context.Background(), "web01")
	require.ErrorIs(t, err, errKeyNotFound, "short names only match when match_short_hostname is on")
}

func TestPuppetDBShortHostname(t *testing.T) {
	s := newPuppetDBServer(t, "ovx9")
	src, logs := newTestPuppetDBSource(t, &PuppetDBConfig{Endpoint: s.URL, Facts: fixtureFacts(), PageSize: 20, MatchShortHostname: true})
	require.NoError(t, src.LoadContext(context.Background()))

	got, err := src.Lookup(context.Background(), "db01")
	require.NoError(t, err)
	require.Equal(t, "database", got["puppet.role"])

	got, err = src.Lookup(context.Background(), "db01.example.com")
	require.NoError(t, err, "full certnames still match")
	require.Equal(t, "database", got["puppet.role"])

	_, err = src.Lookup(context.Background(), "web01")
	require.ErrorIs(t, err, errKeyNotFound, "web01.example.com and web01.other.com share a short name, so neither wins")

	entries := logs.FilterMessageSnippet("short hostname").All()
	require.Len(t, entries, 1)
	require.Equal(t, int64(1), entries[0].ContextMap()["ambiguous"])
}

func TestPuppetDBValueCoercion(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"certname":"a.example.com",
			"facts.big":12345678901234567890,
			"facts.ratio":1.50,
			"facts.os":{"family":"RedHat","release":{"major":"9"}},
			"facts.list":["x","y"],
			"facts.off":false,
			"facts.text":"plain"}]`))
	}))
	t.Cleanup(srv.Close)

	src, _ := newTestPuppetDBSource(t, &PuppetDBConfig{Endpoint: srv.URL, Facts: map[string]string{
		"facts.big": "big", "facts.ratio": "ratio", "facts.os": "os", "facts.list": "list", "facts.off": "off", "facts.text": "text",
	}})
	require.NoError(t, src.LoadContext(context.Background()))

	got, err := src.Lookup(context.Background(), "a.example.com")
	require.NoError(t, err)
	require.Equal(t, map[string]string{
		"big":   "12345678901234567890",
		"ratio": "1.50",
		"os":    `{"family":"RedHat","release":{"major":"9"}}`,
		"list":  `["x","y"]`,
		"off":   "false",
		"text":  "plain",
	}, got)
}

func TestPuppetDBFailedReloadKeepsLastGoodIndex(t *testing.T) {
	testCases := []struct {
		name    string
		status  int
		body    string
		errText string
	}{
		{name: "server error", status: http.StatusInternalServerError, body: "boom", errText: "500"},
		{name: "allowlist rejection", status: http.StatusForbidden, body: mustReadFile(t, "testdata/puppetdb/ovx9tls-403.txt"), errText: "certificate-allowlist"},
		{name: "malformed JSON", status: http.StatusOK, body: `[{"certname":`, errText: "decode"},
		{name: "row without certname", status: http.StatusOK, body: `[{"facts.os.family":"RedHat"}]`, errText: "certname"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := newPuppetDBServer(t, "ovx9")
			src, _ := newTestPuppetDBSource(t, &PuppetDBConfig{Endpoint: s.URL, Facts: fixtureFacts(), PageSize: 20})
			require.NoError(t, src.LoadContext(context.Background()))

			s.fail(tc.status, tc.body)
			err := src.LoadContext(context.Background())
			require.Error(t, err)
			require.Contains(t, err.Error(), tc.errText)

			got, err := src.Lookup(context.Background(), "web01.example.com")
			require.NoError(t, err, "a failed reload must keep serving the previous inventory")
			require.Equal(t, "web", got["puppet.role"])

			s.recover()
			require.NoError(t, src.LoadContext(context.Background()))
		})
	}
}

func TestPuppetDBMaxNodes(t *testing.T) {
	s := newPuppetDBServer(t, "ovx9")
	src, _ := newTestPuppetDBSource(t, &PuppetDBConfig{Endpoint: s.URL, Facts: fixtureFacts(), PageSize: 20, MaxNodes: 30})

	err := src.LoadContext(context.Background())
	require.ErrorIs(t, err, errPuppetDBTooManyNodes)
	_, err = src.Lookup(context.Background(), "web01.example.com")
	require.ErrorIs(t, err, errPuppetDBNotLoaded, "an over-limit inventory is never published")
	require.Len(t, s.recorded(), 2, "paging stops as soon as the limit is passed")
}

func TestPuppetDBResponseSizeLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"certname":"a.example.com","facts.text":"` + strings.Repeat("x", 2048) + `"}]`))
	}))
	t.Cleanup(srv.Close)

	src, _ := newTestPuppetDBSource(t, &PuppetDBConfig{Endpoint: srv.URL, Facts: map[string]string{"facts.text": "text"}})
	src.maxPageBytes = 1024
	err := src.LoadContext(context.Background())
	require.ErrorIs(t, err, errPuppetDBPageTooLarge)
}

func TestPuppetDBLoadHonorsContext(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() { close(release); srv.Close() })

	src, _ := newTestPuppetDBSource(t, &PuppetDBConfig{Endpoint: srv.URL, Facts: fixtureFacts()})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- src.LoadContext(ctx) }()
	cancel()

	var err error
	require.Eventually(t, func() bool {
		select {
		case err = <-done:
			return true
		default:
			return false
		}
	}, 5*time.Second, 10*time.Millisecond)
	require.ErrorIs(t, err, context.Canceled)
}

func TestPuppetDBTimeout(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() { close(release); srv.Close() })

	src, _ := newTestPuppetDBSource(t, &PuppetDBConfig{Endpoint: srv.URL, Facts: fixtureFacts(), Timeout: 50 * time.Millisecond})
	err := src.LoadContext(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "Client.Timeout")
}

func TestPuppetDBUnreachable(t *testing.T) {
	src, _ := newTestPuppetDBSource(t, &PuppetDBConfig{Endpoint: "http://127.0.0.1:1", Facts: fixtureFacts()})
	require.Error(t, src.LoadContext(context.Background()))
}

func TestNewPuppetDBSourceBadTLS(t *testing.T) {
	_, err := NewPuppetDBSource(&PuppetDBConfig{
		Endpoint: "https://puppetdb:8081", Facts: fixtureFacts(),
		TLS: configtls.ClientConfig{Config: configtls.Config{CAFile: "/does/not/exist.pem"}},
	}, zap.NewNop())
	require.Error(t, err)
}

func TestNewPuppetDBSourceBadQuery(t *testing.T) {
	_, err := NewPuppetDBSource(&PuppetDBConfig{Endpoint: "http://puppetdb:8080", Facts: fixtureFacts(), Query: "not json"}, zap.NewNop())
	require.Error(t, err)
}

func TestPuppetDBMutualTLS(t *testing.T) {
	pki := newTestPKI(t)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Mirrors PuppetDB's allowlist: a verified cert whose name is not listed is a 403.
		if len(r.TLS.PeerCertificates) == 0 || r.TLS.PeerCertificates[0].Subject.CommonName != "collector" {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte("Permission denied: not in certificate-allowlist"))
			return
		}
		_, _ = w.Write([]byte(`[{"certname":"a.example.com","facts.os.family":"RedHat"}]`))
	}))
	srv.TLS = &tls.Config{
		Certificates: []tls.Certificate{pki.serverCert},
		ClientCAs:    pki.pool,
		ClientAuth:   tls.VerifyClientCertIfGiven,
		MinVersion:   tls.VersionTLS12,
	}
	srv.StartTLS()
	t.Cleanup(srv.Close)

	tlsFor := func(client string) configtls.ClientConfig {
		c := configtls.ClientConfig{Config: configtls.Config{CAFile: pki.caFile}}
		if client != "" {
			c.CertFile, c.KeyFile = pki.clientFiles(t, client)
		}
		return c
	}

	t.Run("allowlisted client cert", func(t *testing.T) {
		src, _ := newTestPuppetDBSource(t, &PuppetDBConfig{Endpoint: srv.URL, TLS: tlsFor("collector"), Facts: map[string]string{"facts.os.family": "os"}})
		require.NoError(t, src.LoadContext(context.Background()))
		got, err := src.Lookup(context.Background(), "a.example.com")
		require.NoError(t, err)
		require.Equal(t, "RedHat", got["os"])
	})
	t.Run("cert not on allowlist", func(t *testing.T) {
		src, _ := newTestPuppetDBSource(t, &PuppetDBConfig{Endpoint: srv.URL, TLS: tlsFor("stranger"), Facts: map[string]string{"facts.os.family": "os"}})
		err := src.LoadContext(context.Background())
		require.Error(t, err)
		require.Contains(t, err.Error(), "certificate-allowlist")
	})
	t.Run("no client cert", func(t *testing.T) {
		src, _ := newTestPuppetDBSource(t, &PuppetDBConfig{Endpoint: srv.URL, TLS: tlsFor(""), Facts: map[string]string{"facts.os.family": "os"}})
		require.Error(t, src.LoadContext(context.Background()))
	})
	t.Run("server_name_override for a certname-only server cert", func(t *testing.T) {
		c := tlsFor("collector")
		c.ServerName = "puppetdb"
		u, err := url.Parse(srv.URL)
		require.NoError(t, err)
		u.Host = "localhost:" + u.Port()
		src, _ := newTestPuppetDBSource(t, &PuppetDBConfig{Endpoint: u.String(), TLS: c, Facts: map[string]string{"facts.os.family": "os"}})
		require.NoError(t, src.LoadContext(context.Background()))
	})
}

func mustReadFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	return string(data)
}

// testPKI is a throwaway CA with a server cert for the name "puppetdb" and
// 127.0.0.1, so connecting as "localhost" needs server_name_override, plus
// on-demand client certs.
type testPKI struct {
	caCert     *x509.Certificate
	caKey      *rsa.PrivateKey
	caFile     string
	pool       *x509.CertPool
	serverCert tls.Certificate
	dir        string
}

func newTestPKI(t *testing.T) *testPKI {
	t.Helper()
	dir := t.TempDir()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	caTmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "test puppet ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTmpl, caTmpl, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)
	caFile := filepath.Join(dir, "ca.pem")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600))

	p := &testPKI{caCert: caCert, caKey: caKey, caFile: caFile, pool: x509.NewCertPool(), dir: dir}
	p.pool.AddCert(caCert)

	certPEM, keyPEM := p.issueCert(t, "puppetdb", []string{"puppetdb"}, x509.ExtKeyUsageServerAuth, true)
	p.serverCert, err = tls.X509KeyPair(certPEM, keyPEM)
	require.NoError(t, err)
	return p
}

func (p *testPKI) issueCert(t *testing.T, cn string, dns []string, usage x509.ExtKeyUsage, withIP bool) ([]byte, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(time.Now().UnixNano()),
		Subject:      pkix.Name{CommonName: cn},
		DNSNames:     dns,
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{usage},
	}
	if withIP {
		tmpl.IPAddresses = append(tmpl.IPAddresses, []byte{127, 0, 0, 1})
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, p.caCert, &key.PublicKey, p.caKey)
	require.NoError(t, err)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)})
}

func (p *testPKI) clientFiles(t *testing.T, cn string) (string, string) {
	t.Helper()
	certPEM, keyPEM := p.issueCert(t, cn, nil, x509.ExtKeyUsageClientAuth, false)
	certFile := filepath.Join(p.dir, fmt.Sprintf("%s.pem", cn))
	keyFile := filepath.Join(p.dir, fmt.Sprintf("%s.key", cn))
	require.NoError(t, os.WriteFile(certFile, certPEM, 0o600))
	require.NoError(t, os.WriteFile(keyFile, keyPEM, 0o600))
	return certFile, keyFile
}

func TestPuppetDBSourceIsNotWrappedInCache(t *testing.T) {
	s := newPuppetDBServer(t, "ovx9")
	p, logs := startedProcessor(t, &Config{
		Context: resourceContext, Field: "host.name",
		PuppetDB:     &PuppetDBConfig{Endpoint: s.URL, Facts: fixtureFacts(), PageSize: 20},
		CacheEnabled: true, CacheTTL: defaultCacheTTL, CacheMaxEntries: defaultCacheMaxEntries,
		cacheKeysSet: map[string]bool{"cache_ttl": true},
	})
	require.IsType(t, &PuppetDBSource{}, p.source, "the inventory is resident in memory, so a cache adds nothing")
	require.Len(t, logs.FilterMessageSnippet("do not apply to a puppetdb source").All(), 1)
}

// TestShutdownCancelsInFlightPuppetDBLoad checks that a reload blocked on a
// slow PuppetDB does not hold up collector shutdown for the HTTP timeout.
func TestShutdownCancelsInFlightPuppetDBLoad(t *testing.T) {
	inFlight := make(chan struct{}, 1)
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		inFlight <- struct{}{}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}))
	t.Cleanup(func() { close(release); srv.Close() })

	core, _ := observer.New(zapcore.DebugLevel)
	p := newLookupProcessor(&Config{
		Context: resourceContext, Field: "host.name",
		PuppetDB: &PuppetDBConfig{Endpoint: srv.URL, Facts: fixtureFacts(), Timeout: time.Hour},
	}, component.MustNewID("lookup"), signalLogs, zap.New(core))
	require.NoError(t, p.start(context.Background(), componenttestHost{}))

	require.Eventually(t, func() bool { return len(inFlight) == 1 }, 5*time.Second, 10*time.Millisecond)

	done := make(chan error, 1)
	go func() { done <- p.shutdown(context.Background()) }()
	require.Eventually(t, func() bool { return len(done) == 1 }, 5*time.Second, 10*time.Millisecond)
	require.NoError(t, <-done)
}

func TestStartFailsOnBadPuppetDBTLS(t *testing.T) {
	p := newLookupProcessor(&Config{
		Context: resourceContext, Field: "host.name",
		PuppetDB: &PuppetDBConfig{
			Endpoint: "https://puppetdb:8081", Facts: fixtureFacts(),
			TLS: configtls.ClientConfig{Config: configtls.Config{CAFile: "/does/not/exist.pem"}},
		},
	}, component.MustNewID("lookup"), signalLogs, zap.NewNop())
	err := p.start(context.Background(), componenttestHost{})
	require.ErrorContains(t, err, "failed to create lookup source")
}

func TestPuppetDBBadEndpointURL(t *testing.T) {
	src, _ := newTestPuppetDBSource(t, &PuppetDBConfig{Endpoint: "http://puppetdb:8080/\x7f", Facts: fixtureFacts()})
	require.ErrorContains(t, src.LoadContext(context.Background()), "build puppetdb request")
}

func TestPuppetDBTruncatedBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Length", "1000")
		_, _ = w.Write([]byte(`[{"certname":`))
	}))
	t.Cleanup(srv.Close)

	src, _ := newTestPuppetDBSource(t, &PuppetDBConfig{Endpoint: srv.URL, Facts: fixtureFacts()})
	require.ErrorContains(t, src.LoadContext(context.Background()), "read puppetdb response")
}
