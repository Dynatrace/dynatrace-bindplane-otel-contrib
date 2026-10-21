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

package exabeamexporter // import "github.com/dynatrace/dynatrace-bindplane-otel-contrib/exporter/exabeamexporter"

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/exporter/exabeamexporter/internal/metadata"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

const (
	testCollectorID = "b557e4fc-1234-4567-89ab-cdef01234567"
	testAccessKey   = "GOOGTESTACCESSKEY"
	testSecretKey   = "test-secret-key"
)

// testConnectionString returns a connection string encoding the given fields.
func testConnectionString(t *testing.T, fields map[string]string) string {
	t.Helper()
	b, err := json.Marshal(fields)
	require.NoError(t, err)
	return base64.StdEncoding.EncodeToString(b)
}

func fullConnectionFields() map[string]string {
	return map[string]string{
		"collector_id":    testCollectorID,
		"access_key":      testAccessKey,
		"secret_key":      testSecretKey,
		"bucket_name":     "dl-topic-ops-dist-cc-mgrtn-e2e",
		"bucket_location": "US",
		"sitename":        "abcorp",
		"siteid":          "xdfsdfw99999sfsdf",
		"timezone_offset": "-0700",
	}
}

func TestLoadConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	defaultCfg := createDefaultConfig().(*Config)

	connCfg := createDefaultConfig().(*Config)
	connCfg.ConnectionString = "eyJjb2xsZWN0b3JfaWQiOiJiNTU3ZTRmYy0xMjM0LTQ1NjctODlhYi1jZGVmMDEyMzQ1NjciLCJhY2Nlc3Nfa2V5IjoiR09PR1RFU1RBQ0NFU1NLRVkiLCJzZWNyZXRfa2V5IjoidGVzdC1zZWNyZXQta2V5IiwiYnVja2V0X25hbWUiOiJkbC10b3BpYy1vcHMtZGlzdC1jYy1tZ3J0bi1lMmUiLCJidWNrZXRfbG9jYXRpb24iOiJVUyIsInNpdGVuYW1lIjoiYWJjb3JwIiwic2l0ZWlkIjoieGRmc2Rmdzk5OTk5c2ZzZGYiLCJ0aW1lem9uZV9vZmZzZXQiOiItMDcwMCJ9"
	connCfg.CollectorName = "bindplane-gateway"
	connCfg.Tags = `"env":"prod"`

	fieldsCfg := createDefaultConfig().(*Config)
	fieldsCfg.CollectorID = testCollectorID
	fieldsCfg.AccessKey = testAccessKey
	fieldsCfg.SecretKey = testSecretKey
	fieldsCfg.BucketName = "my-bucket"
	fieldsCfg.BucketLocation = "US"
	fieldsCfg.SiteName = "abcorp"
	fieldsCfg.SiteID = "site-1"
	fieldsCfg.TimezoneOffset = "+0100"
	fieldsCfg.Endpoint = "https://storage.example.com"
	fieldsCfg.MaxFileSize = 1048576
	fieldsCfg.TimeoutConfig.Timeout = 10 * time.Second
	fieldsCfg.QueueConfig.Get().NumConsumers = 4
	fieldsCfg.BackOffConfig.Enabled = false

	tests := []struct {
		id          component.ID
		expected    *Config
		expectedErr string
	}{
		{id: component.NewID(metadata.Type), expected: defaultCfg, expectedErr: "connection_string"},
		{id: component.NewIDWithName(metadata.Type, "connection_string"), expected: connCfg},
		{id: component.NewIDWithName(metadata.Type, "fields"), expected: fieldsCfg},
	}

	for _, tt := range tests {
		t.Run(tt.id.String(), func(t *testing.T) {
			cfg := createDefaultConfig()
			sub, err := cm.Sub(tt.id.String())
			require.NoError(t, err)
			require.NoError(t, sub.Unmarshal(cfg))
			require.Equal(t, tt.expected, cfg)

			err = confmap.Validate(cfg)
			if tt.expectedErr != "" {
				require.ErrorContains(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
		})
	}
}

func TestConfigResolve(t *testing.T) {
	t.Run("connection string only", func(t *testing.T) {
		cfg := createDefaultConfig().(*Config)
		cfg.ConnectionString = configopaque.String(testConnectionString(t, fullConnectionFields()))

		s, err := cfg.resolve()
		require.NoError(t, err)
		require.Equal(t, testCollectorID, s.CollectorID)
		require.Equal(t, testAccessKey, string(s.AccessKey))
		require.Equal(t, testSecretKey, string(s.SecretKey))
		require.Equal(t, "dl-topic-ops-dist-cc-mgrtn-e2e", s.BucketName)
		require.Equal(t, "US", s.BucketLocation)
		require.Equal(t, "abcorp", s.SiteName)
		require.Equal(t, "xdfsdfw99999sfsdf", s.SiteID)
		require.Equal(t, "-0700", s.TimezoneOffset)
	})

	t.Run("explicit settings override the connection string", func(t *testing.T) {
		cfg := createDefaultConfig().(*Config)
		cfg.ConnectionString = configopaque.String(testConnectionString(t, fullConnectionFields()))
		cfg.CollectorID = "override-id"
		cfg.AccessKey = "override-access"
		cfg.SecretKey = "override-secret"
		cfg.BucketName = "override-bucket"
		cfg.BucketLocation = "EU"
		cfg.SiteName = "override-site"
		cfg.SiteID = "override-siteid"
		cfg.TimezoneOffset = "+0200"

		s, err := cfg.resolve()
		require.NoError(t, err)
		require.Equal(t, "override-id", s.CollectorID)
		require.Equal(t, "override-access", string(s.AccessKey))
		require.Equal(t, "override-secret", string(s.SecretKey))
		require.Equal(t, "override-bucket", s.BucketName)
		require.Equal(t, "EU", s.BucketLocation)
		require.Equal(t, "override-site", s.SiteName)
		require.Equal(t, "override-siteid", s.SiteID)
		require.Equal(t, "+0200", s.TimezoneOffset)
	})

	t.Run("explicit settings only", func(t *testing.T) {
		cfg := createDefaultConfig().(*Config)
		cfg.CollectorID = testCollectorID
		cfg.AccessKey = testAccessKey
		cfg.SecretKey = testSecretKey
		cfg.BucketName = "bucket"

		s, err := cfg.resolve()
		require.NoError(t, err)
		require.Equal(t, "bucket", s.BucketName)
		require.Equal(t, "", s.BucketLocation)
	})

	t.Run("unpadded and URL-safe base64 are accepted", func(t *testing.T) {
		b, err := json.Marshal(fullConnectionFields())
		require.NoError(t, err)
		for _, enc := range []*base64.Encoding{base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
			cfg := createDefaultConfig().(*Config)
			cfg.ConnectionString = configopaque.String(" \n" + enc.EncodeToString(b) + "\n")
			s, err := cfg.resolve()
			require.NoError(t, err)
			require.Equal(t, testCollectorID, s.CollectorID)
		}
	})
}

func TestConfigValidate(t *testing.T) {
	missing := func(keys ...string) string {
		fields := fullConnectionFields()
		for _, k := range keys {
			delete(fields, k)
		}
		b, _ := json.Marshal(fields)
		return base64.StdEncoding.EncodeToString(b)
	}

	tests := []struct {
		desc        string
		mutate      func(c *Config)
		expectedErr string
	}{
		{
			desc:   "valid connection string",
			mutate: func(c *Config) { c.ConnectionString = configopaque.String(missing()) },
		},
		{
			desc:        "nothing set",
			mutate:      func(*Config) {},
			expectedErr: "missing collector_id, access_key, secret_key, bucket_name: set connection_string or the individual settings",
		},
		{
			desc:        "connection string missing secret_key and bucket_name",
			mutate:      func(c *Config) { c.ConnectionString = configopaque.String(missing("secret_key", "bucket_name")) },
			expectedErr: "missing secret_key, bucket_name",
		},
		{
			desc: "missing field provided explicitly",
			mutate: func(c *Config) {
				c.ConnectionString = configopaque.String(missing("secret_key"))
				c.SecretKey = "explicit"
			},
		},
		{
			desc:        "connection string is not base64",
			mutate:      func(c *Config) { c.ConnectionString = "not base64!" },
			expectedErr: "connection_string is not valid base64",
		},
		{
			desc: "connection string is not JSON",
			mutate: func(c *Config) {
				c.ConnectionString = configopaque.String(base64.StdEncoding.EncodeToString([]byte("secret_key=abc")))
			},
			expectedErr: "connection_string does not decode to a JSON object",
		},
		{
			desc: "bad timezone offset",
			mutate: func(c *Config) {
				c.ConnectionString = configopaque.String(missing())
				c.TimezoneOffset = "UTC-7"
			},
			expectedErr: "timezone_offset must be in the form +HHMM or -HHMM",
		},
		{
			desc: "bad endpoint scheme",
			mutate: func(c *Config) {
				c.ConnectionString = configopaque.String(missing())
				c.Endpoint = "storage.googleapis.com"
			},
			expectedErr: "endpoint must be an http or https URL",
		},
		{
			desc: "empty endpoint",
			mutate: func(c *Config) {
				c.ConnectionString = configopaque.String(missing())
				c.Endpoint = ""
			},
			expectedErr: "endpoint must be an http or https URL",
		},
		{
			desc: "max file size not positive",
			mutate: func(c *Config) {
				c.ConnectionString = configopaque.String(missing())
				c.MaxFileSize = 0
			},
			expectedErr: "max_file_size must be greater than 0",
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			cfg := createDefaultConfig().(*Config)
			tc.mutate(cfg)
			err := cfg.Validate()
			if tc.expectedErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.expectedErr)
			require.NotContains(t, err.Error(), testSecretKey)
		})
	}
}

func TestConfigDoesNotLeakSecrets(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.ConnectionString = configopaque.String(testConnectionString(t, fullConnectionFields()))
	cfg.AccessKey = testAccessKey
	cfg.SecretKey = testSecretKey

	s, err := cfg.resolve()
	require.NoError(t, err)

	for _, v := range []any{cfg, *cfg, s} {
		out := fmt.Sprintf("%v %+v", v, v)
		require.NotContains(t, out, testSecretKey)
		require.NotContains(t, out, string(cfg.ConnectionString))
	}

	conf := confmap.New()
	require.NoError(t, conf.Marshal(cfg))
	for _, key := range []string{"connection_string", "access_key", "secret_key"} {
		require.Equal(t, "[REDACTED]", conf.Get(key), key)
	}
}
