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
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/dnscheckreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httpcheckreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/icmpcheckreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/tcpcheckreceiver"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/confmap"
)

// Strict decoding reaches every section and the structs nested in them, not
// only the sections the existing test covers. Not http targets: upstream
// http_check's targetConfig.Unmarshal drops strictness, standalone too.
func TestUnknownKeysRejectedEverywhere(t *testing.T) {
	for name, yaml := range map[string]string{
		"icmp":              "icmp: {bogus: 1, targets: [{host: 127.0.0.1}]}",
		"icmp target":       "icmp: {targets: [{host: 127.0.0.1, bogus: 1}]}",
		"icmp metric":       "icmp: {targets: [{host: 127.0.0.1}], metrics: {ping.bogus: {enabled: true}}}",
		"dns":               "dns: {bogus: 1, dns_servers: [{endpoint: '127.0.0.1:53'}], hostnames: [{name: a.test}]}",
		"dns hostname":      "dns: {dns_servers: [{endpoint: '127.0.0.1:53'}], hostnames: [{name: a.test, bogus: 1}]}",
		"tcp":               "tcp: {bogus: 1, targets: [{endpoint: '127.0.0.1:1'}]}",
		"traceroute target": "traceroute: {targets: [{host: 127.0.0.1, bogus: 1}]}",
		"traceroute metric": "traceroute: {targets: [{host: 127.0.0.1}], metrics: {traceroute.bogus: {enabled: true}}}",
	} {
		t.Run(name, func(t *testing.T) {
			_, err := load(t, yaml)
			require.ErrorContains(t, err, "bogus")
		})
	}
}

// A section that sets only its targets carries exactly the upstream factory's
// MetricsBuilderConfig: every default-on metric and resource attribute stays
// on and every default-off one stays off.
func TestMinimalSectionsKeepUpstreamMetricDefaults(t *testing.T) {
	cfg := mustLoad(t, `
http: {targets: [{endpoint: 'http://127.0.0.1:8080'}]}
icmp: {targets: [{host: 127.0.0.1}]}
dns: {dns_servers: [{endpoint: '127.0.0.1:53'}], hostnames: [{name: a.test}]}
tcp: {targets: [{endpoint: '127.0.0.1:443'}]}
`)
	require.Equal(t, httpcheckreceiver.NewFactory().CreateDefaultConfig().(*httpcheckreceiver.Config).MetricsBuilderConfig, cfg.HTTP.MetricsBuilderConfig)
	require.Equal(t, icmpcheckreceiver.NewFactory().CreateDefaultConfig().(*icmpcheckreceiver.Config).MetricsBuilderConfig, cfg.ICMP.MetricsBuilderConfig)
	require.Equal(t, dnscheckreceiver.NewFactory().CreateDefaultConfig().(*dnscheckreceiver.Config).MetricsBuilderConfig, cfg.DNS.MetricsBuilderConfig)
	require.Equal(t, tcpcheckreceiver.NewFactory().CreateDefaultConfig().(*tcpcheckreceiver.Config).MetricsBuilderConfig, cfg.TCP.MetricsBuilderConfig)
	require.NoError(t, confmap.Validate(cfg))
}

// Each controller key a section sets wins; the ones it leaves out come from
// the top level, independently per key and per section.
func TestSectionKeysOverrideInheritedPerKey(t *testing.T) {
	cfg := mustLoad(t, `
collection_interval: 30s
initial_delay: 5s
timeout: 10s
http: {collection_interval: 11s, targets: [{endpoint: 'http://127.0.0.1:8080'}]}
icmp: {initial_delay: 12s, targets: [{host: 127.0.0.1}]}
dns: {timeout: 13s, dns_servers: [{endpoint: '127.0.0.1:53'}], hostnames: [{name: a.test}]}
tcp: {collection_interval: 14s, initial_delay: 0s, timeout: 0s, targets: [{endpoint: '127.0.0.1:443'}]}
traceroute: {initial_delay: 15s, targets: [{host: 127.0.0.1}]}
`)
	type cc = [3]time.Duration
	s := time.Second
	require.Equal(t, cc{11 * s, 5 * s, 10 * s}, cc{cfg.HTTP.ControllerConfig.CollectionInterval, cfg.HTTP.ControllerConfig.InitialDelay, cfg.HTTP.ControllerConfig.Timeout})
	require.Equal(t, cc{30 * s, 12 * s, 10 * s}, cc{cfg.ICMP.ControllerConfig.CollectionInterval, cfg.ICMP.ControllerConfig.InitialDelay, cfg.ICMP.ControllerConfig.Timeout})
	require.Equal(t, cc{30 * s, 5 * s, 13 * s}, cc{cfg.DNS.ControllerConfig.CollectionInterval, cfg.DNS.ControllerConfig.InitialDelay, cfg.DNS.ControllerConfig.Timeout})
	require.Equal(t, cc{14 * s, 0, 0}, cc{cfg.TCP.ControllerConfig.CollectionInterval, cfg.TCP.ControllerConfig.InitialDelay, cfg.TCP.ControllerConfig.Timeout}, "an explicit zero is the section's own value")
	require.Equal(t, 30*s, cfg.Traceroute.CollectionInterval)
	require.Equal(t, 15*s, cfg.Traceroute.InitialDelay)
	require.Equal(t, defaultHopTimeout, cfg.Traceroute.Timeout, "the top-level scrape deadline is not the per-probe timeout")
	require.NoError(t, confmap.Validate(cfg))
}

// An explicit collection_interval of 0 in any section is rejected under that
// section's key, even when the top level is valid.
func TestSectionZeroIntervalRejected(t *testing.T) {
	for name, yaml := range map[string]string{
		"http":       "http: {collection_interval: 0s, targets: [{endpoint: 'http://127.0.0.1'}]}",
		"icmp":       "icmp: {collection_interval: 0s, targets: [{host: 127.0.0.1}]}",
		"dns":        "dns: {collection_interval: 0s, dns_servers: [{endpoint: '127.0.0.1:53'}], hostnames: [{name: a.test}]}",
		"tcp":        "tcp: {collection_interval: 0s, targets: [{endpoint: '127.0.0.1:1'}]}",
		"traceroute": "traceroute: {collection_interval: 0s, targets: [{host: 127.0.0.1}]}",
	} {
		t.Run(name, func(t *testing.T) {
			err := confmap.Validate(mustLoad(t, yaml))
			require.ErrorContains(t, err, name+":")
			require.ErrorContains(t, err, "collection_interval")
		})
	}
}

// metrics overrides inside a section are honoured and leave the other
// defaults alone.
func TestSectionMetricOverrides(t *testing.T) {
	cfg := mustLoad(t, `
http:
  targets: [{endpoint: 'http://127.0.0.1:8080'}]
  metrics: {httpcheck.response.size: {enabled: true}, httpcheck.error: {enabled: false}}
icmp:
  targets: [{host: 127.0.0.1}]
  metrics: {ping.rtt.min: {enabled: false}}
  resource_attributes: {net.peer.ip: {enabled: false}}
dns:
  dns_servers: [{endpoint: '127.0.0.1:53'}]
  hostnames: [{name: a.test}]
  metrics: {dnscheck.duration: {enabled: false}}
tcp:
  targets: [{endpoint: '127.0.0.1:443'}]
  metrics: {tcpcheck.duration: {enabled: false}}
traceroute:
  targets: [{host: 127.0.0.1}]
  metrics: {traceroute.hop.latency: {enabled: false}}
`)
	require.True(t, cfg.HTTP.MetricsBuilderConfig.Metrics.HttpcheckResponseSize.Enabled)
	require.False(t, cfg.HTTP.MetricsBuilderConfig.Metrics.HttpcheckError.Enabled)
	require.True(t, cfg.HTTP.MetricsBuilderConfig.Metrics.HttpcheckStatus.Enabled)

	require.False(t, cfg.ICMP.MetricsBuilderConfig.Metrics.PingRttMin.Enabled)
	require.True(t, cfg.ICMP.MetricsBuilderConfig.Metrics.PingLossRatio.Enabled)
	require.False(t, cfg.ICMP.MetricsBuilderConfig.ResourceAttributes.NetPeerIP.Enabled)
	require.True(t, cfg.ICMP.MetricsBuilderConfig.ResourceAttributes.NetPeerName.Enabled)

	require.False(t, cfg.DNS.MetricsBuilderConfig.Metrics.DnscheckDuration.Enabled)
	require.True(t, cfg.DNS.MetricsBuilderConfig.Metrics.DnscheckStatus.Enabled)
	require.False(t, cfg.TCP.MetricsBuilderConfig.Metrics.TcpcheckDuration.Enabled)
	require.True(t, cfg.TCP.MetricsBuilderConfig.Metrics.TcpcheckStatus.Enabled)

	require.False(t, cfg.Traceroute.Metrics.TracerouteHopLatency.Enabled)
	require.True(t, cfg.Traceroute.Metrics.TracerouteReached.Enabled)
	require.NoError(t, confmap.Validate(cfg))
}

// Both spellings of an empty traceroute section next to icmp are trigger-only
// with the defaults; next to anything else they are rejected.
func TestEmptyTracerouteForms(t *testing.T) {
	for _, tr := range []string{"traceroute:", "traceroute: {}", "traceroute: {on_failure: {}}"} {
		t.Run(tr, func(t *testing.T) {
			cfg := mustLoad(t, "icmp: {targets: [{host: 127.0.0.1}]}\n"+tr+"\n")
			require.True(t, cfg.Traceroute.OnFailure.Enabled)
			require.False(t, cfg.Traceroute.scheduled())
			require.Equal(t, 50.0, cfg.Traceroute.OnFailure.LossThreshold)
			require.NoError(t, confmap.Validate(cfg))

			cfg = mustLoad(t, "tcp: {targets: [{endpoint: '127.0.0.1:1'}]}\n"+tr+"\n")
			require.False(t, cfg.Traceroute.OnFailure.Enabled)
			require.ErrorContains(t, confmap.Validate(cfg), "nothing to trace")
		})
	}
}

// on_failure needs icmp even when it is enabled explicitly, and an explicit
// enabled: false next to icmp turns it off.
func TestOnFailureEnabledFlag(t *testing.T) {
	require.ErrorContains(t,
		confmap.Validate(mustLoad(t, "tcp: {targets: [{endpoint: '127.0.0.1:1'}]}\ntraceroute: {on_failure: {enabled: true}, targets: [{host: 127.0.0.1}]}")),
		"requires an icmp section")

	cfg := mustLoad(t, "icmp: {targets: [{host: 127.0.0.1}]}\ntraceroute: {on_failure: {enabled: false}, targets: [{host: 127.0.0.1}]}")
	require.False(t, cfg.Traceroute.OnFailure.Enabled)
	require.NoError(t, confmap.Validate(cfg))
}

// schedule: false with targets: the targets are still validated, then ignored
// without a word when on_failure carries the section. Without on_failure the
// error asks for targets that are already there.
func TestScheduleFalseWithTargets(t *testing.T) {
	cfg := mustLoad(t, "icmp: {targets: [{host: 127.0.0.1}]}\ntraceroute: {schedule: false, targets: [{host: 192.0.2.1}]}")
	require.NoError(t, confmap.Validate(cfg))
	require.False(t, cfg.Traceroute.scheduled())
	require.Empty(t, newSharedProber(cfg.Traceroute, nil).targets, "the targets are never traced")

	err := confmap.Validate(mustLoad(t, "icmp: {targets: [{host: 127.0.0.1}]}\ntraceroute: {schedule: false, targets: [{host: 'http://x'}]}"))
	require.ErrorContains(t, err, "host must be a bare hostname", "ignored targets are still validated")

	err = confmap.Validate(mustLoad(t, "tcp: {targets: [{endpoint: '127.0.0.1:1'}]}\ntraceroute: {schedule: false, targets: [{host: 192.0.2.1}]}"))
	require.ErrorContains(t, err, "nothing to trace: add targets")
}

func TestHTTPTargetsWithCredentialsAreRejected(t *testing.T) {
	validate := func(t *testing.T, yaml string) error {
		t.Helper()
		cfg, err := load(t, yaml)
		require.NoError(t, err)
		return confmap.Validate(cfg)
	}
	err := validate(t, `
http:
  targets:
    - endpoint: https://user:pw@example.test/health
`)
	require.ErrorContains(t, err, "http::targets::0: credentials in the endpoint URL are not allowed")
	require.NotContains(t, err.Error(), "pw", "the credential must not be quoted")

	err = validate(t, `
http:
  targets:
    - endpoint: https://example.test/health
      endpoints: [https://example.test/a, https://u:pw@example.test/b]
`)
	require.ErrorContains(t, err, "http::targets::0: credentials")

	require.NoError(t, validate(t, `
http:
  targets:
    - endpoint: https://example.test/health
      headers:
        Authorization: "Bearer not-in-the-url"
`))
}

func TestUnmarshalNilConf(t *testing.T) {
	c := &Config{}
	require.NoError(t, c.Unmarshal(nil))
	require.Equal(t, &Config{}, c)
}

func TestDecodeSectionErrors(t *testing.T) {
	c := &Config{}
	err := c.decodeSection(confmap.NewFromStringMap(map[string]any{"traceroute": "not a map"}), "traceroute", defaultTracerouteConfig(), nil)
	require.Error(t, err)

	err = c.decodeSection(confmap.NewFromStringMap(map[string]any{"traceroute": map[string]any{"max_hops": "many"}}), "traceroute", defaultTracerouteConfig(), nil)
	require.ErrorContains(t, err, "traceroute: ")
	require.ErrorContains(t, err, "max_hops")
}

func TestHTTPTargetsWithCredentialsShapes(t *testing.T) {
	bad := func(http any) []int {
		return httpTargetsWithCredentials(confmap.NewFromStringMap(map[string]any{"http": http}))
	}
	require.Nil(t, bad("not a map"))
	require.Nil(t, bad(map[string]any{}), "no targets")
	require.Nil(t, bad(map[string]any{"targets": "not a list"}))
	require.Equal(t, []int{2, 3}, bad(map[string]any{"targets": []any{
		"https://u:pw@example.test", // not a target; strict decoding rejects it
		map[string]any{"endpoint": 42, "endpoints": []any{nil, "https://example.test/a"}},
		map[string]any{"endpoint": "https://u:pw@example.test"},
		map[string]any{"endpoints": []any{"https://example.test/a", "https://u@example.test/b", "https://u:pw@example.test/c"}},
		map[string]any{"endpoint": "://not a url"},
	}}), "each target with userinfo once, by index")
}

func TestValidateNegativeJitter(t *testing.T) {
	c := defaultTracerouteConfig()
	c.CollectionInterval = time.Minute
	c.Targets = []TracerouteTarget{{Host: "example.com", DNSServer: "dns.example.test:5353"}}
	require.NoError(t, c.Validate(), "a named DNS server with a port is valid")
	c.Jitter = -time.Second
	require.ErrorContains(t, c.Validate(), "jitter must be >= 0")
}
