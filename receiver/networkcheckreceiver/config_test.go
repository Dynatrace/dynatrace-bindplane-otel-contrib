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
	"os"
	"path/filepath"
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/confmaptest"
)

// load decodes a receiver configuration the way the collector does.
func load(t *testing.T, yaml string) (*Config, error) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "config.yaml")
	require.NoError(t, os.WriteFile(path, []byte(yaml), 0o600))
	cm, err := confmaptest.LoadConf(path)
	require.NoError(t, err)
	cfg := NewFactory().CreateDefaultConfig()
	if err := cm.Unmarshal(&cfg); err != nil {
		return nil, err
	}
	return cfg.(*Config), nil
}

func mustLoad(t *testing.T, yaml string) *Config {
	t.Helper()
	cfg, err := load(t, yaml)
	require.NoError(t, err)
	return cfg
}

// Decoding a section straight into a nil pointer leaves the upstream
// MetricsBuilderConfig zero, which disables every upstream metric. Each
// section must start from its factory's defaults.
func TestUnmarshalSeedsSectionDefaults(t *testing.T) {
	cfg := mustLoad(t, `
http:
  targets:
    - endpoint: http://127.0.0.1:8080
icmp:
  targets:
    - host: 127.0.0.1
dns:
  dns_servers:
    - endpoint: 127.0.0.1:53
  hostnames:
    - name: example.com
tcp:
  targets:
    - endpoint: 127.0.0.1:443
traceroute:
  targets:
    - host: 127.0.0.1
`)
	http := cfg.HTTP.MetricsBuilderConfig.Metrics
	require.True(t, http.HttpcheckStatus.Enabled)
	require.True(t, http.HttpcheckDuration.Enabled)
	require.True(t, http.HttpcheckError.Enabled)
	require.False(t, http.HttpcheckResponseSize.Enabled, "an upstream default-off metric stays off")
	require.Equal(t, "http://127.0.0.1:8080", cfg.HTTP.Targets[0].ClientConfig.Endpoint)

	require.True(t, cfg.ICMP.MetricsBuilderConfig.Metrics.PingLossRatio.Enabled)
	require.True(t, cfg.ICMP.MetricsBuilderConfig.ResourceAttributes.NetPeerName.Enabled)
	require.True(t, cfg.DNS.MetricsBuilderConfig.Metrics.DnscheckStatus.Enabled)
	require.True(t, cfg.TCP.MetricsBuilderConfig.Metrics.TcpcheckStatus.Enabled)

	tr := cfg.Traceroute
	require.True(t, tr.Metrics.TracerouteReached.Enabled)
	require.True(t, tr.Schedule)
	require.Equal(t, "udp", tr.Method)
	require.Equal(t, defaultMaxHops, tr.MaxHops)
	require.Equal(t, defaultHopTimeout, tr.Timeout)
	require.Equal(t, defaultProbesPerHop, tr.ProbesPerHop)
	require.Equal(t, defaultMaxConsecutiveTimeouts, tr.MaxConsecutiveTimeouts)
	require.Equal(t, defaultMaxConcurrentTraces, tr.MaxConcurrentTraces)
	require.Equal(t, OnFailureConfig{Enabled: true, LossThreshold: 50, RetraceEvery: 10, MaxHosts: 256, Timeout: time.Minute}, tr.OnFailure)
	require.NoError(t, confmap.Validate(cfg))
}

func TestUnmarshalInheritance(t *testing.T) {
	cfg := mustLoad(t, `
collection_interval: 30s
initial_delay: 5s
timeout: 10s
http:
  targets:
    - endpoint: http://127.0.0.1:8080
tcp:
  collection_interval: 15s
  initial_delay: 0s
  targets:
    - endpoint: 127.0.0.1:443
traceroute:
  timeout: 2s
  targets:
    - host: 127.0.0.1
`)
	for _, tc := range []struct {
		name                          string
		interval, delay, scrapeTimout time.Duration
		want                          [3]time.Duration
	}{
		{"http inherits all three", cfg.HTTP.ControllerConfig.CollectionInterval, cfg.HTTP.ControllerConfig.InitialDelay, cfg.HTTP.ControllerConfig.Timeout, [3]time.Duration{30 * time.Second, 5 * time.Second, 10 * time.Second}},
		{"tcp keeps its own", cfg.TCP.ControllerConfig.CollectionInterval, cfg.TCP.ControllerConfig.InitialDelay, cfg.TCP.ControllerConfig.Timeout, [3]time.Duration{15 * time.Second, 0, 10 * time.Second}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, [3]time.Duration{tc.interval, tc.delay, tc.scrapeTimout})
		})
	}
	require.Equal(t, 30*time.Second, cfg.Traceroute.CollectionInterval)
	require.Equal(t, 5*time.Second, cfg.Traceroute.InitialDelay)
	require.Equal(t, 2*time.Second, cfg.Traceroute.Timeout, "traceroute's timeout is per probe, not the inherited scrape deadline")
	require.False(t, cfg.Traceroute.OnFailure.Enabled, "on_failure defaults off without an icmp section")
	require.NoError(t, confmap.Validate(cfg))

	cfg = mustLoad(t, `
traceroute:
  collection_interval: 5m
  targets:
    - host: 127.0.0.1
`)
	require.Equal(t, 5*time.Minute, cfg.Traceroute.CollectionInterval)
	require.Equal(t, time.Second, cfg.Traceroute.InitialDelay, "the top-level default")
}

// An empty traceroute section next to icmp is trigger-only with defaults.
func TestUnmarshalEmptyTracerouteSection(t *testing.T) {
	cfg := mustLoad(t, "icmp: {targets: [{host: 127.0.0.1}]}\ntraceroute:\n")
	require.NotNil(t, cfg.Traceroute)
	require.True(t, cfg.Traceroute.OnFailure.Enabled)
	require.False(t, cfg.Traceroute.scheduled())
	require.NoError(t, confmap.Validate(cfg))
}

func TestUnmarshalRejectsUnknownKeys(t *testing.T) {
	for name, yaml := range map[string]string{
		"top level":  "bogus: 1\ntraceroute: {targets: [{host: 127.0.0.1}]}",
		"http":       "http: {bogus: 1, targets: [{endpoint: 'http://127.0.0.1'}]}",
		"traceroute": "traceroute: {bogus: 1, targets: [{host: 127.0.0.1}]}",
		"on_failure": "traceroute: {on_failure: {bogus: 1}, targets: [{host: 127.0.0.1}]}",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := load(t, yaml)
			require.ErrorContains(t, err, "bogus")
		})
	}
}

// The collector validates by walking every exported field, so each upstream
// section's Validate, and its nested structs', run under the section's key.
func TestValidateDelegatesToSections(t *testing.T) {
	for _, tc := range []struct {
		name, yaml string
		want       []string
	}{
		{"no section", "collection_interval: 1m", []string{"at least one of http, icmp, dns, tcp or traceroute"}},
		{"http without targets", "http: {}", []string{"http: no targets configured"}},
		{"http target endpoint", "http: {targets: [{endpoint: 'not a url'}]}", []string{`http::targets::0: "endpoint" must be in the form`}},
		{"icmp target without host", "icmp: {targets: [{ping_count: 1}]}", []string{"icmp: target host is required"}},
		{"dns network", "dns: {dns_servers: [{endpoint: '127.0.0.1', network: quic}], hostnames: [{name: a.test}]}", []string{"dns: dns server 'network' must be one of"}},
		{"tcp endpoint", "tcp: {targets: [{endpoint: example.com}]}", []string{`tcp: "Endpoint" must be in the form`}},
		{"section interval", "tcp: {collection_interval: 0s, targets: [{endpoint: 'example.com:1'}]}", []string{"tcp::", `"collection_interval": requires positive value`}},
		{"top-level interval", "collection_interval: 0s\ntcp: {collection_interval: 1s, targets: [{endpoint: 'example.com:1'}]}", []string{`"collection_interval"`}},
		{"traceroute rules", "traceroute: {max_hops: 0, targets: [{host: 'http://x'}]}", []string{"traceroute: targets[0]: host must be a bare hostname", "max_hops must be between 1 and 255"}},
		{"on_failure without icmp", "traceroute: {on_failure: {enabled: true}}", []string{"on_failure::enabled requires an icmp section"}},
		{"on_failure without ping.loss.ratio", "icmp: {targets: [{host: 127.0.0.1}], metrics: {ping.loss.ratio: {enabled: false}}}\ntraceroute: {}", []string{"requires the icmp section's ping.loss.ratio metric"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := confmap.Validate(mustLoad(t, tc.yaml))
			require.Error(t, err)
			for _, w := range tc.want {
				require.ErrorContains(t, err, w)
			}
		})
	}
}

func TestValidateTraceroute(t *testing.T) {
	valid := func() *TracerouteConfig {
		c := defaultTracerouteConfig()
		c.CollectionInterval = time.Minute
		c.Targets = []TracerouteTarget{{Host: "example.com"}}
		return c
	}
	for _, tc := range []struct {
		name   string
		mutate func(*TracerouteConfig)
		want   string // "" = valid
	}{
		{"defaults", func(*TracerouteConfig) {}, ""},
		{"ip literal and dns server", func(c *TracerouteConfig) {
			c.Targets = []TracerouteTarget{{Host: "2001:db8::1", DNSServer: "[2001:db8::53]:53"}, {Host: "192.0.2.1", DNSServer: "192.0.2.53"}}
		}, ""},
		{"trigger only", func(c *TracerouteConfig) { c.Targets = nil; c.OnFailure.Enabled = true }, ""},
		{"schedule off, trigger on", func(c *TracerouteConfig) { c.Schedule = false; c.OnFailure.Enabled = true }, ""},
		{"nothing to trace", func(c *TracerouteConfig) { c.Targets = nil }, "nothing to trace"},
		{"schedule off, trigger off", func(c *TracerouteConfig) { c.Schedule = false }, "nothing to trace"},
		{"empty host", func(c *TracerouteConfig) { c.Targets[0].Host = "" }, "targets[0]: host is required"},
		{"host with port", func(c *TracerouteConfig) { c.Targets[0].Host = "example.com:80" }, "targets[0]: host must be a bare hostname"},
		{"host with userinfo", func(c *TracerouteConfig) { c.Targets[0].Host = "u@example.com" }, "targets[0]: host must be a bare hostname"},
		{"dns server with scheme", func(c *TracerouteConfig) { c.Targets[0].DNSServer = "udp://192.0.2.53" }, "targets[0]: dns_server must be host or host:port"},
		{"dns server bad port", func(c *TracerouteConfig) { c.Targets[0].DNSServer = "192.0.2.53:70000" }, "targets[0]: dns_server must be host or host:port"},
		{"dns server unbracketed v6 with port", func(c *TracerouteConfig) { c.Targets[0].DNSServer = "2001:db8::53:53x" }, "targets[0]: dns_server must be host or host:port"},
		{"method", func(c *TracerouteConfig) { c.Method = "tcp" }, `method "tcp" is invalid`},
		{"method case", func(c *TracerouteConfig) { c.Method = "ICMP" }, ""},
		{"max_hops 0", func(c *TracerouteConfig) { c.MaxHops = 0 }, "max_hops must be between 1 and 255"},
		{"max_hops 256", func(c *TracerouteConfig) { c.MaxHops = 256 }, "max_hops must be between 1 and 255"},
		{"max_hops 255", func(c *TracerouteConfig) { c.MaxHops = 255 }, ""},
		{"probes_per_hop 0", func(c *TracerouteConfig) { c.ProbesPerHop = 0 }, "probes_per_hop must be between 1 and 10"},
		{"probes_per_hop 11", func(c *TracerouteConfig) { c.ProbesPerHop = 11 }, "probes_per_hop must be between 1 and 10"},
		{"max_concurrent_traces 0", func(c *TracerouteConfig) { c.MaxConcurrentTraces = 0 }, "max_concurrent_traces must be between 1 and 64"},
		{"max_concurrent_traces 65", func(c *TracerouteConfig) { c.MaxConcurrentTraces = 65 }, "max_concurrent_traces must be between 1 and 64"},
		{"max_consecutive_timeouts 0", func(c *TracerouteConfig) { c.MaxConsecutiveTimeouts = 0 }, ""},
		{"max_consecutive_timeouts negative", func(c *TracerouteConfig) { c.MaxConsecutiveTimeouts = -1 }, "max_consecutive_timeouts must be >= 0"},
		{"timeout 0", func(c *TracerouteConfig) { c.Timeout = 0 }, "timeout must be > 0"},
		{"timeout above interval", func(c *TracerouteConfig) { c.Timeout = 2 * time.Minute }, "timeout must not exceed collection_interval"},
		{"interval 0", func(c *TracerouteConfig) { c.CollectionInterval = 0 }, "collection_interval must be > 0"},
		{"initial_delay negative", func(c *TracerouteConfig) { c.InitialDelay = -time.Second }, "initial_delay must be >= 0"},
		{"batch_size negative", func(c *TracerouteConfig) { c.BatchSize = -1 }, "batch_size must be >= 0"},
		{"jitter half", func(c *TracerouteConfig) { c.Jitter = 30 * time.Second }, ""},
		{"jitter above half", func(c *TracerouteConfig) { c.Jitter = 31 * time.Second }, "jitter must be at most half"},
		{"loss_threshold 0 and 100", func(c *TracerouteConfig) { c.OnFailure.LossThreshold = 100 }, ""},
		{"loss_threshold negative", func(c *TracerouteConfig) { c.OnFailure.LossThreshold = -1 }, "loss_threshold must be between 0 and 100"},
		{"loss_threshold as a ratio is still accepted", func(c *TracerouteConfig) { c.OnFailure.LossThreshold = 0.5 }, ""},
		{"loss_threshold above 100", func(c *TracerouteConfig) { c.OnFailure.LossThreshold = 101 }, "loss_threshold must be between 0 and 100"},
		{"retrace_every 0", func(c *TracerouteConfig) { c.OnFailure.RetraceEvery = 0 }, "retrace_every must be >= 1"},
		{"max_hosts 0", func(c *TracerouteConfig) { c.OnFailure.MaxHosts = 0 }, "max_hosts must be >= 1"},
		{"on_failure timeout 0", func(c *TracerouteConfig) { c.OnFailure.Timeout = 0 }, "on_failure::timeout must be > 0"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := valid()
			tc.mutate(c)
			err := c.Validate()
			if tc.want == "" {
				require.NoError(t, err)
				return
			}
			require.ErrorContains(t, err, tc.want)
		})
	}
}

func TestValidateReportsEveryTracerouteProblem(t *testing.T) {
	c := defaultTracerouteConfig()
	c.CollectionInterval = time.Minute
	c.Targets = []TracerouteTarget{{Host: ""}}
	c.MaxHops = 0
	c.OnFailure.RetraceEvery = 0
	err := c.Validate()
	require.ErrorContains(t, err, "host is required")
	require.ErrorContains(t, err, "max_hops")
	require.ErrorContains(t, err, "retrace_every")
}

func TestMetadataTestConfigIsValid(t *testing.T) {
	cm, err := confmaptest.LoadConf("metadata.yaml")
	require.NoError(t, err)
	sub, err := cm.Sub("tests::config")
	require.NoError(t, err)
	cfg := NewFactory().CreateDefaultConfig()
	require.NoError(t, sub.Unmarshal(&cfg))
	require.NoError(t, confmap.Validate(cfg))
}

// The README's configuration block is loaded verbatim, so an example that
// stops parsing or validating fails here.
func TestReadmeConfigurationBlockIsValid(t *testing.T) {
	readme, err := os.ReadFile("README.md")
	require.NoError(t, err)
	// \r?: Windows CI checks the README out with CRLF line endings.
	m := regexp.MustCompile("(?s)\r?\n## Configuration\r?\n.*?```yaml\r?\n(.*?)```").FindSubmatch(readme)
	require.NotNil(t, m, "README.md must have a yaml block under ## Configuration")

	path := filepath.Join(t.TempDir(), "readme.yaml")
	require.NoError(t, os.WriteFile(path, m[1], 0o600))
	cm, err := confmaptest.LoadConf(path)
	require.NoError(t, err)
	sub, err := cm.Sub("receivers::networkcheck")
	require.NoError(t, err)
	cfg := NewFactory().CreateDefaultConfig()
	require.NoError(t, sub.Unmarshal(&cfg))
	require.NoError(t, confmap.Validate(cfg))

	c := cfg.(*Config)
	require.NotNil(t, c.HTTP)
	require.NotNil(t, c.ICMP)
	require.NotNil(t, c.DNS)
	require.NotNil(t, c.TCP)
	require.Equal(t, 30*time.Second, c.TCP.ControllerConfig.CollectionInterval)
	require.Equal(t, time.Minute, c.HTTP.ControllerConfig.CollectionInterval)
	// The commented values are the defaults.
	want := defaultTracerouteConfig()
	want.CollectionInterval = 5 * time.Minute
	want.InitialDelay = time.Second
	want.OnFailure.Enabled = true
	want.Targets = []TracerouteTarget{{Host: "example.com"}}
	require.Equal(t, want, c.Traceroute)
}
