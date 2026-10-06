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

package networkcheckreceiver // import "github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver"

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/dnscheckreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/httpcheckreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/icmpcheckreceiver"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/tcpcheckreceiver"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/multierr"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver/internal/metadata"
)

// Config is the configuration of the networkcheck receiver. Every section is
// optional and at least one is required. http, icmp, dns and tcp are the
// configurations of the upstream http_check, icmp_check, dns_check and
// tcp_check receivers, unchanged.
//
// The collector validates the configuration by walking every exported field,
// so each section's own Validate, and those of its nested structs, run with
// the section's key in the error path; Validate below only adds the rules
// that span sections.
type Config struct {
	// collection_interval, initial_delay and timeout: the defaults for every
	// section that does not set its own.
	scraperhelper.ControllerConfig `mapstructure:",squash"`

	HTTP       *httpcheckreceiver.Config `mapstructure:"http"`
	ICMP       *icmpcheckreceiver.Config `mapstructure:"icmp"`
	DNS        *dnscheckreceiver.Config  `mapstructure:"dns"`
	TCP        *tcpcheckreceiver.Config  `mapstructure:"tcp"`
	Traceroute *TracerouteConfig         `mapstructure:"traceroute"`
}

// TracerouteConfig is the traceroute section.
//
// collection_interval and initial_delay are declared here rather than by
// squashing scraperhelper.ControllerConfig, whose own "timeout" key (the
// scrape deadline) would collide with the per-hop timeout below.
type TracerouteConfig struct {
	// CollectionInterval is how often the targets are traced. Defaults to the
	// top-level collection_interval.
	CollectionInterval time.Duration `mapstructure:"collection_interval"`

	// InitialDelay delays the first scheduled trace. Defaults to the
	// top-level initial_delay.
	InitialDelay time.Duration `mapstructure:"initial_delay"`

	// Schedule false disables scheduled traces; only on_failure traces run.
	// Default true.
	Schedule bool `mapstructure:"schedule"`

	metadata.MetricsBuilderConfig `mapstructure:",squash"`

	// Targets are traced on the schedule.
	Targets []TracerouteTarget `mapstructure:"targets"`

	// Method is "udp" (default) or "icmp". Ignored on Windows, which always
	// uses the native IcmpSendEcho API.
	Method string `mapstructure:"method"`

	// MaxHops is the highest TTL probed, 1-255. Default 30.
	MaxHops int `mapstructure:"max_hops"`

	// Timeout is how long one probe waits for its reply. Default 3s. Must not
	// exceed collection_interval.
	Timeout time.Duration `mapstructure:"timeout"`

	// ProbesPerHop is the most probes sent to one hop, 1-10. Probing stops at
	// the first reply. Default 3.
	ProbesPerHop int `mapstructure:"probes_per_hop"`

	// MaxConsecutiveTimeouts abandons a trace after this many silent hops in
	// a row. 0 disables the early abort, leaving max_hops as the only bound.
	// Default 5.
	MaxConsecutiveTimeouts int `mapstructure:"max_consecutive_timeouts"`

	// MaxConcurrentTraces bounds how many traces run at once, scheduled and
	// triggered together, 1-64. Default 4.
	MaxConcurrentTraces int `mapstructure:"max_concurrent_traces"`

	// BatchSize is how many targets are traced per cycle, rotating through
	// the list. 0 means every target every cycle.
	BatchSize int `mapstructure:"batch_size"`

	// Jitter delays each cycle by a random duration in [0, jitter). It counts
	// against the cycle budget and must be at most half of
	// collection_interval. Default 0.
	Jitter time.Duration `mapstructure:"jitter"`

	// OnFailure traces a host when the icmp section reports it failing.
	OnFailure OnFailureConfig `mapstructure:"on_failure"`
}

// TracerouteTarget is one host traced on the schedule.
type TracerouteTarget struct {
	// Host is a bare hostname or IP address literal.
	Host string `mapstructure:"host"`

	// DNSServer resolves Host instead of the system resolver: host or
	// host:port, port 53 when omitted. An IPv6 address with a port is
	// bracketed, as in [2001:db8::53]:53.
	DNSServer string `mapstructure:"dns_server"`
}

// OnFailureConfig configures traces triggered by failing icmp checks.
type OnFailureConfig struct {
	// Enabled defaults to true when an icmp section exists.
	Enabled bool `mapstructure:"enabled"`

	// LossThreshold is the ping.loss.ratio, a percentage, at or above which a
	// check is failing. Default 50.
	LossThreshold float64 `mapstructure:"loss_threshold"`

	// RetraceEvery re-traces a host that keeps failing on every Nth failing
	// check. The first failing check always traces. Default 10.
	RetraceEvery int `mapstructure:"retrace_every"`

	// MaxHosts caps how many failing hosts are tracked at once. Default 256.
	MaxHosts int `mapstructure:"max_hosts"`

	// Timeout bounds a triggered trace, including the wait for a free trace
	// slot. Default 60s.
	Timeout time.Duration `mapstructure:"timeout"`
}

func defaultTracerouteConfig() *TracerouteConfig {
	return &TracerouteConfig{
		Schedule:               true,
		MetricsBuilderConfig:   metadata.NewDefaultMetricsBuilderConfig(),
		Method:                 "udp",
		MaxHops:                defaultMaxHops,
		Timeout:                defaultHopTimeout,
		ProbesPerHop:           defaultProbesPerHop,
		MaxConsecutiveTimeouts: defaultMaxConsecutiveTimeouts,
		MaxConcurrentTraces:    defaultMaxConcurrentTraces,
		OnFailure: OnFailureConfig{
			LossThreshold: 50,
			RetraceEvery:  10,
			MaxHosts:      256,
			Timeout:       time.Minute,
		},
	}
}

// Unmarshal decodes each present section onto that section's own defaults,
// so an upstream section keeps the defaults its factory sets (enabled
// metrics among them), and fills in the top-level collection_interval,
// initial_delay and timeout where the section does not set its own.
func (c *Config) Unmarshal(conf *confmap.Conf) error {
	if conf == nil {
		return nil
	}
	// The top-level keys, plus a strict check of every section.
	if err := conf.Unmarshal(c); err != nil {
		return err
	}

	var err error
	if conf.IsSet("http") {
		c.HTTP = httpcheckreceiver.NewFactory().CreateDefaultConfig().(*httpcheckreceiver.Config)
		err = multierr.Append(err, c.decodeSection(conf, "http", c.HTTP, &c.HTTP.ControllerConfig))
	}
	if conf.IsSet("icmp") {
		c.ICMP = icmpcheckreceiver.NewFactory().CreateDefaultConfig().(*icmpcheckreceiver.Config)
		err = multierr.Append(err, c.decodeSection(conf, "icmp", c.ICMP, &c.ICMP.ControllerConfig))
	}
	if conf.IsSet("dns") {
		c.DNS = dnscheckreceiver.NewFactory().CreateDefaultConfig().(*dnscheckreceiver.Config)
		err = multierr.Append(err, c.decodeSection(conf, "dns", c.DNS, &c.DNS.ControllerConfig))
	}
	if conf.IsSet("tcp") {
		c.TCP = tcpcheckreceiver.NewFactory().CreateDefaultConfig().(*tcpcheckreceiver.Config)
		err = multierr.Append(err, c.decodeSection(conf, "tcp", c.TCP, &c.TCP.ControllerConfig))
	}
	if conf.IsSet("traceroute") {
		c.Traceroute = defaultTracerouteConfig()
		c.Traceroute.CollectionInterval = c.CollectionInterval
		c.Traceroute.InitialDelay = c.InitialDelay
		c.Traceroute.OnFailure.Enabled = c.ICMP != nil
		err = multierr.Append(err, c.decodeSection(conf, "traceroute", c.Traceroute, nil))
	}
	return err
}

// decodeSection seeds cc with the top-level controller settings and decodes
// the section over dst, so the section's own keys win.
func (c *Config) decodeSection(conf *confmap.Conf, key string, dst any, cc *scraperhelper.ControllerConfig) error {
	sub, err := conf.Sub(key)
	if err != nil {
		return err
	}
	if cc != nil {
		cc.CollectionInterval = c.CollectionInterval
		cc.InitialDelay = c.InitialDelay
		cc.Timeout = c.Timeout
	}
	if err := sub.Unmarshal(dst); err != nil {
		return fmt.Errorf("%s: %w", key, err)
	}
	return nil
}

// Validate checks the rules that span sections.
func (c *Config) Validate() error {
	if c.HTTP == nil && c.ICMP == nil && c.DNS == nil && c.TCP == nil && c.Traceroute == nil {
		return errors.New("at least one of http, icmp, dns, tcp or traceroute must be configured")
	}
	if c.Traceroute == nil || !c.Traceroute.OnFailure.Enabled {
		return nil
	}
	if c.ICMP == nil {
		return errors.New("traceroute::on_failure::enabled requires an icmp section, whose failing checks trigger the traces")
	}
	if !c.ICMP.MetricsBuilderConfig.Metrics.PingLossRatio.Enabled {
		return errors.New("traceroute::on_failure::enabled requires the icmp section's ping.loss.ratio metric, which it reads to find failing hosts")
	}
	if !c.ICMP.MetricsBuilderConfig.ResourceAttributes.NetPeerName.Enabled {
		return errors.New("traceroute::on_failure::enabled requires the icmp section's net.peer.name resource attribute, which names the failing host")
	}
	return nil
}

// Validate reports every problem in the traceroute section, not just the
// first.
func (c *TracerouteConfig) Validate() error {
	var errs error
	// The interval paces scheduled traces only; trigger-only ignores it.
	interval := c.CollectionInterval
	if !c.scheduled() {
		interval = 0
	} else if interval <= 0 {
		errs = multierr.Append(errs, errors.New("collection_interval must be > 0"))
	}
	if c.InitialDelay < 0 {
		errs = multierr.Append(errs, errors.New("initial_delay must be >= 0"))
	}

	if !c.scheduled() && !c.OnFailure.Enabled {
		errs = multierr.Append(errs, errors.New("nothing to trace: add targets, or enable on_failure with an icmp section"))
	}
	for i, t := range c.Targets {
		if t.Host == "" {
			errs = multierr.Append(errs, fmt.Errorf("targets[%d]: host is required", i))
		} else if !validHost(t.Host) {
			errs = multierr.Append(errs, fmt.Errorf("targets[%d]: host must be a bare hostname or IP address, without scheme, port, path, or userinfo", i))
		}
		if t.DNSServer != "" && !validDNSServer(t.DNSServer) {
			errs = multierr.Append(errs, fmt.Errorf("targets[%d]: dns_server must be host or host:port, without scheme, path, or userinfo; an IPv6 address with a port must be bracketed, as in [2001:db8::53]:53", i))
		}
	}

	switch strings.ToLower(c.Method) {
	case "udp", "icmp":
	default:
		errs = multierr.Append(errs, fmt.Errorf("method %q is invalid; must be \"udp\" or \"icmp\"", c.Method))
	}
	// TTL is a single byte on the wire.
	if c.MaxHops < 1 || c.MaxHops > maxTTL {
		errs = multierr.Append(errs, errors.New("max_hops must be between 1 and 255"))
	}
	if c.Timeout <= 0 {
		errs = multierr.Append(errs, errors.New("timeout must be > 0"))
	} else if interval > 0 && c.Timeout > interval {
		errs = multierr.Append(errs, errors.New("timeout must not exceed collection_interval"))
	}
	if c.ProbesPerHop < 1 || c.ProbesPerHop > 10 {
		errs = multierr.Append(errs, errors.New("probes_per_hop must be between 1 and 10"))
	}
	if c.MaxConsecutiveTimeouts < 0 {
		errs = multierr.Append(errs, errors.New("max_consecutive_timeouts must be >= 0"))
	}
	if c.MaxConcurrentTraces < 1 || c.MaxConcurrentTraces > 64 {
		errs = multierr.Append(errs, errors.New("max_concurrent_traces must be between 1 and 64"))
	}
	if c.BatchSize < 0 {
		errs = multierr.Append(errs, errors.New("batch_size must be >= 0"))
	}
	// A jitter at or above the interval would push one cycle into the next.
	if c.Jitter < 0 {
		errs = multierr.Append(errs, errors.New("jitter must be >= 0"))
	} else if interval > 0 && c.Jitter > interval/2 {
		errs = multierr.Append(errs, errors.New("jitter must be at most half of collection_interval; the delay counts against the cycle budget"))
	}

	of := c.OnFailure
	if of.LossThreshold < 0 || of.LossThreshold > 100 {
		errs = multierr.Append(errs, errors.New("on_failure::loss_threshold must be between 0 and 100 (percent of lost packets)"))
	}
	if of.RetraceEvery < 1 {
		errs = multierr.Append(errs, errors.New("on_failure::retrace_every must be >= 1"))
	}
	if of.MaxHosts < 1 {
		errs = multierr.Append(errs, errors.New("on_failure::max_hosts must be >= 1"))
	}
	if of.Timeout <= 0 {
		errs = multierr.Append(errs, errors.New("on_failure::timeout must be > 0"))
	}
	return errs
}

// scheduled reports whether any target is traced on the schedule.
func (c *TracerouteConfig) scheduled() bool {
	return c.Schedule && len(c.Targets) > 0
}

// validHost accepts an IP address literal or a name without the characters a
// scheme, port, path, query or userinfo would bring.
func validHost(host string) bool {
	if _, err := netip.ParseAddr(host); err == nil {
		return true
	}
	return !strings.ContainsAny(host, ":/@[]?# ")
}

// validDNSServer accepts host or host:port with a port in 1-65535. A bare
// IPv6 address is accepted; with a port it must be bracketed.
func validDNSServer(server string) bool {
	if strings.ContainsAny(server, "/@?# ") {
		return false
	}
	if _, err := netip.ParseAddr(server); err == nil {
		return true
	}
	host, port, err := net.SplitHostPort(server)
	if err != nil {
		// No port: acceptable only as a plain hostname.
		return !strings.ContainsAny(server, ":[]")
	}
	if p, err := strconv.Atoi(port); host == "" || err != nil || p < 1 || p > 65535 {
		return false
	}
	if strings.Contains(host, ":") {
		_, err := netip.ParseAddr(host)
		return err == nil
	}
	return true
}
