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
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

// validConfig is the default config plus one ICMP target, with traceroute
// enabled so its rules are evaluated.
func validConfig() *Config {
	c := createDefaultConfig().(*Config)
	c.Targets = []TargetConfig{{Method: MethodICMP}}
	c.Targets[0].Endpoint = "192.0.2.1"
	c.Traceroute.Enabled = true
	return c
}

type validateCase struct {
	name string
	mut  func(c *Config)
	// wantErr is a substring of the expected error, or "" for a valid config.
	wantErr string
}

func runValidateCases(t *testing.T, cases []validateCase) {
	t.Helper()
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := validConfig()
			tc.mut(c)
			err := c.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

func TestValidateDefaultsAndBasics(t *testing.T) {
	runValidateCases(t, []validateCase{
		{"valid", func(*Config) {}, ""},
		{"no targets", func(c *Config) { c.Targets = nil }, "at least one target is required"},
		{"empty endpoint", func(c *Config) { c.Targets[0].Endpoint = "" }, "target[0]: endpoint is required"},
		{"method empty means icmp", func(c *Config) { c.Targets[0].Method = "" }, ""},
		{"method invalid", func(c *Config) { c.Targets[0].Method = "tcp" }, `target[0]: method "tcp" is invalid`},
		{"batch_size 0", func(c *Config) { c.BatchSize = 0 }, ""},
		{"batch_size negative", func(c *Config) { c.BatchSize = -1 }, "batch_size must be >= 0"},
		{"traceroute method icmp", func(c *Config) { c.Traceroute.Method = "ICMP" }, ""},
		{"traceroute method invalid", func(c *Config) { c.Traceroute.Method = "tcp" }, `traceroute.method "tcp" is invalid`},
		{"max_consecutive_timeouts 0", func(c *Config) { c.Traceroute.MaxConsecutiveTimeouts = 0 }, ""},
		{"max_consecutive_timeouts negative", func(c *Config) { c.Traceroute.MaxConsecutiveTimeouts = -1 }, "traceroute.max_consecutive_timeouts must be >= 0"},
		{"traceroute rules skipped when disabled", func(c *Config) {
			c.Traceroute.Enabled = false
			c.Traceroute.MaxHops = 1000
		}, ""},
	})
}

func TestValidateErrorsAreAggregated(t *testing.T) {
	c := validConfig()
	c.BatchSize = -1
	c.Jitter = -1
	c.Traceroute.MaxHops = -1
	err := c.Validate()
	require.ErrorContains(t, err, "batch_size")
	require.ErrorContains(t, err, "jitter")
	require.ErrorContains(t, err, "traceroute.max_hops")
}

func TestValidateMaxHops(t *testing.T) {
	runValidateCases(t, []validateCase{
		{"0 means default", func(c *Config) { c.Traceroute.MaxHops = 0 }, ""},
		{"1", func(c *Config) { c.Traceroute.MaxHops = 1 }, ""},
		{"255", func(c *Config) { c.Traceroute.MaxHops = 255 }, ""},
		{"256", func(c *Config) { c.Traceroute.MaxHops = 256 }, "traceroute.max_hops must be between 0 and 255"},
		{"negative", func(c *Config) { c.Traceroute.MaxHops = -1 }, "traceroute.max_hops must be between 0 and 255"},
	})
}

func TestValidateProbesPerHop(t *testing.T) {
	runValidateCases(t, []validateCase{
		{"0 means default", func(c *Config) { c.Traceroute.ProbesPerHop = 0 }, ""},
		{"10", func(c *Config) { c.Traceroute.ProbesPerHop = 10 }, ""},
		{"11", func(c *Config) { c.Traceroute.ProbesPerHop = 11 }, "traceroute.probes_per_hop must be between 0 and 10"},
		{"negative", func(c *Config) { c.Traceroute.ProbesPerHop = -1 }, "traceroute.probes_per_hop must be between 0 and 10"},
	})
}

func TestValidatePingCount(t *testing.T) {
	runValidateCases(t, []validateCase{
		{"0 means default", func(c *Config) { c.Targets[0].PingCount = 0 }, ""},
		{"100", func(c *Config) { c.Targets[0].PingCount = 100 }, ""},
		{"101", func(c *Config) { c.Targets[0].PingCount = 101 }, "target[0]: ping_count must be between 0 and 100"},
		{"negative", func(c *Config) { c.Targets[0].PingCount = -1 }, "target[0]: ping_count must be between 0 and 100"},
	})
}

func TestValidateTracerouteTimeout(t *testing.T) {
	runValidateCases(t, []validateCase{
		{"0 means default", func(c *Config) { c.Traceroute.Timeout = 0 }, ""},
		{"equal to interval", func(c *Config) { c.Traceroute.Timeout = c.CollectionInterval }, ""},
		{"above interval", func(c *Config) { c.Traceroute.Timeout = c.CollectionInterval + time.Second }, "traceroute.timeout must not exceed collection_interval"},
		{"negative", func(c *Config) { c.Traceroute.Timeout = -time.Second }, "traceroute.timeout must be >= 0"},
	})
}

func TestValidateTracerouteInterval(t *testing.T) {
	runValidateCases(t, []validateCase{
		{"0 disables", func(c *Config) { c.Traceroute.Interval = 0 }, ""},
		{"10", func(c *Config) { c.Traceroute.Interval = 10 }, ""},
		{"negative", func(c *Config) { c.Traceroute.Interval = -1 }, "traceroute.interval must be >= 0"},
	})
}

func TestValidateFailureThreshold(t *testing.T) {
	runValidateCases(t, []validateCase{
		{"0", func(c *Config) { c.Traceroute.FailureThreshold = 0 }, ""},
		{"1", func(c *Config) { c.Traceroute.FailureThreshold = 1 }, ""},
		{"above 1", func(c *Config) { c.Traceroute.FailureThreshold = 1.5 }, "traceroute.failure_threshold must be between 0.0 and 1.0"},
		{"negative", func(c *Config) { c.Traceroute.FailureThreshold = -0.1 }, "traceroute.failure_threshold must be between 0.0 and 1.0"},
	})
}

func TestValidateTargetTimeout(t *testing.T) {
	runValidateCases(t, []validateCase{
		{"0 means default", func(c *Config) { c.Targets[0].Timeout = 0 }, ""},
		{"equal to interval", func(c *Config) { c.Targets[0].Timeout = c.CollectionInterval }, ""},
		{"above interval", func(c *Config) { c.Targets[0].Timeout = c.CollectionInterval + time.Second }, "target[0]: timeout must not exceed collection_interval"},
		{"negative", func(c *Config) { c.Targets[0].Timeout = -time.Second }, "target[0]: timeout must be >= 0"},
	})
}

func TestValidateJitter(t *testing.T) {
	runValidateCases(t, []validateCase{
		{"0", func(c *Config) { c.Jitter = 0 }, ""},
		{"just below interval", func(c *Config) { c.Jitter = c.CollectionInterval - time.Millisecond }, ""},
		{"equal to interval", func(c *Config) { c.Jitter = c.CollectionInterval }, "jitter must be less than collection_interval"},
		{"negative", func(c *Config) { c.Jitter = -1 }, "jitter must be >= 0"},
	})
}

func TestValidateMaxConcurrentProbes(t *testing.T) {
	runValidateCases(t, []validateCase{
		{"0 means default", func(c *Config) { c.MaxConcurrentProbes = 0 }, ""},
		{"256", func(c *Config) { c.MaxConcurrentProbes = 256 }, ""},
		{"257", func(c *Config) { c.MaxConcurrentProbes = 257 }, "max_concurrent_probes must be between 0 and 256"},
		{"negative", func(c *Config) { c.MaxConcurrentProbes = -1 }, "max_concurrent_probes must be between 0 and 256"},
	})
}

func TestValidateDNSQueryAndRecordType(t *testing.T) {
	dns := func(c *Config) {
		c.Targets[0].Method = MethodDNS
		c.Targets[0].DNSQuery = "example.com"
	}
	runValidateCases(t, []validateCase{
		{"query set", dns, ""},
		{"query missing", func(c *Config) { dns(c); c.Targets[0].DNSQuery = "" }, "target[0]: dns_query is required"},
		{"record type lowercase", func(c *Config) { dns(c); c.Targets[0].DNSRecordType = "aaaa" }, ""},
		{"record type unsupported", func(c *Config) { dns(c); c.Targets[0].DNSRecordType = "SRV" }, `target[0]: dns_record_type "SRV" is invalid`},
	})
}

func TestValidateEndpointShape(t *testing.T) {
	const (
		icmpErr = "icmp endpoint must be a bare hostname or IP address"
		dnsErr  = "dns endpoint must be host or host:port"
	)
	tests := []struct {
		method, endpoint, wantErr string
	}{
		{MethodICMP, "192.0.2.1", ""},
		{MethodICMP, "2001:db8::1", ""},
		{MethodICMP, "fe80::1%eth0", ""},
		{MethodICMP, "example.com", ""},
		{MethodICMP, "example.com:80", icmpErr},
		{MethodICMP, "192.0.2.1:80", icmpErr},
		{MethodICMP, "[2001:db8::1]", icmpErr},
		{MethodICMP, "http://example.com", icmpErr},
		{MethodICMP, "example.com/path", icmpErr},
		{MethodICMP, "user@example.com", icmpErr},

		{MethodDNS, "192.0.2.53", ""},
		{MethodDNS, "192.0.2.53:5353", ""},
		{MethodDNS, "2001:db8::53", ""},
		{MethodDNS, "[2001:db8::53]:53", ""},
		{MethodDNS, "ns.example.com", ""},
		{MethodDNS, "ns.example.com:53", ""},
		{MethodDNS, "dns://192.0.2.53", dnsErr},
		{MethodDNS, "192.0.2.53/path", dnsErr},
		{MethodDNS, "user@192.0.2.53", dnsErr},
		{MethodDNS, "[2001:db8::53]", dnsErr},
		{MethodDNS, "ns.example.com:0", dnsErr},
		{MethodDNS, "ns.example.com:dns", dnsErr},
		{MethodDNS, ":53", dnsErr},
		{MethodDNS, "a:b:c", dnsErr},

		{MethodHTTP, "https://example.com/health", ""},
		{MethodHTTP, "http://192.0.2.1:8080", ""},
		{MethodHTTP, "https://[2001:db8::1]:8443/", ""},
		{MethodHTTP, "https://user:pass@example.com/", ""},
		{MethodHTTP, "example.com/health", ""},
		{MethodHTTP, "ftp://example.com", "http endpoint scheme must be http or https"},
		{MethodHTTP, "http:///path", "http endpoint has no host"},
		{MethodHTTP, "http://example.com:port", "http endpoint is not a valid URL"},
	}
	for _, tc := range tests {
		t.Run(tc.method+" "+tc.endpoint, func(t *testing.T) {
			c := validConfig()
			c.Targets[0].Method = tc.method
			c.Targets[0].Endpoint = tc.endpoint
			c.Targets[0].DNSQuery = "example.com"
			err := c.Validate()
			if tc.wantErr == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, "target[0]: "+tc.wantErr)
		})
	}
}

// The endpoint may carry a password, and a config error is logged, so the
// message must name the problem without quoting the URL.
func TestValidateEndpointErrorOmitsCredentials(t *testing.T) {
	c := validConfig()
	c.Targets[0].Method = MethodHTTP
	c.Targets[0].Endpoint = "https://user:hunter 2@example.com/"
	err := c.Validate()
	require.ErrorContains(t, err, "target[0]: http endpoint is not a valid URL")
	require.NotContains(t, err.Error(), "hunter")
}

func TestLoadConfig(t *testing.T) {
	cm, err := confmaptest.LoadConf(filepath.Join("testdata", "config.yaml"))
	require.NoError(t, err)

	t.Run("networkcheck", func(t *testing.T) {
		cfg := createDefaultConfig().(*Config)
		sub, err := cm.Sub("receivers::networkcheck")
		require.NoError(t, err)
		require.NoError(t, sub.Unmarshal(cfg))
		require.NoError(t, confmap.Validate(cfg))

		require.Equal(t, 8, cfg.MaxConcurrentProbes)
		require.Len(t, cfg.Targets, 3)
		require.Equal(t, "192.0.2.1", cfg.Targets[0].Endpoint)
		require.Equal(t, MethodICMP, cfg.Targets[0].Method)
		require.Equal(t, 3, cfg.Targets[0].PingCount)
		require.Equal(t, MethodHTTP, cfg.Targets[1].Method)
		require.Equal(t, "HEAD", cfg.Targets[1].HTTPMethod)
		require.Equal(t, 10*time.Second, cfg.Targets[1].Timeout)
		require.Equal(t, MethodDNS, cfg.Targets[2].Method)
		require.Equal(t, "example.com", cfg.Targets[2].DNSQuery)
		require.Equal(t, "AAAA", cfg.Targets[2].DNSRecordType)

		tr := cfg.Traceroute
		require.True(t, tr.Enabled)
		require.Equal(t, "udp", tr.Method)
		require.Equal(t, 20, tr.MaxHops)
		require.Equal(t, 10, tr.Interval)
		require.True(t, tr.OnFailure)
		require.InDelta(t, 0.5, tr.FailureThreshold, 0)
		require.Equal(t, 2*time.Second, tr.Timeout)
		require.Equal(t, 2, tr.ProbesPerHop)
		require.Equal(t, 4, tr.MaxConsecutiveTimeouts)
	})

	t.Run("networkcheck/batch", func(t *testing.T) {
		cfg := createDefaultConfig().(*Config)
		sub, err := cm.Sub("receivers::networkcheck/batch")
		require.NoError(t, err)
		require.NoError(t, sub.Unmarshal(cfg))
		require.NoError(t, confmap.Validate(cfg))

		require.Equal(t, 1, cfg.BatchSize)
		require.Len(t, cfg.Targets, 3)
		require.Equal(t, "192.0.2.53:53", cfg.Targets[1].DNSServer)
		require.False(t, cfg.Traceroute.Enabled)
	})
}
