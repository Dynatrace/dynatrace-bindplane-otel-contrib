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

// Package networkcheckreceiver actively probes network targets and emits
// ICMP ping, HTTP timing, DNS query, and traceroute telemetry.
package networkcheckreceiver // import "github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver"

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"time"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/scraper/scraperhelper"
	"go.uber.org/multierr"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver/internal/metadata"
)

// Probe method constants used in TargetConfig.Method.
const (
	MethodICMP = "icmp"
	MethodHTTP = "http"
	MethodDNS  = "dns"
)

// Config is the top-level configuration for the networkcheck receiver.
type Config struct {
	scraperhelper.ControllerConfig `mapstructure:",squash"`
	metadata.MetricsBuilderConfig  `mapstructure:",squash"`

	// Targets is the list of endpoints to probe.
	Targets []TargetConfig `mapstructure:"targets"`

	// BatchSize controls how many targets are checked per scrape cycle.
	// 0 (default) means all targets every cycle. With N targets and batch_size 1,
	// each target is checked once every N * collection_interval.
	BatchSize int `mapstructure:"batch_size"`

	// MaxConcurrentProbes bounds how many targets are probed at the same time
	// within one cycle, 0-256. 0 means the default (16). Traceroutes are
	// additionally limited to 4 in flight.
	MaxConcurrentProbes int `mapstructure:"max_concurrent_probes"`

	// Jitter is the maximum random delay added at the start of each scrape cycle.
	// A random duration in [0, jitter) is chosen independently per cycle, which
	// spreads probes across the interval when many agents share the same config.
	// The delay counts against the cycle budget. Must be less than
	// collection_interval. Default 0 disables jitter.
	Jitter time.Duration `mapstructure:"jitter"`

	// Traceroute configures optional traceroute probes.
	Traceroute TracerouteConfig `mapstructure:"traceroute"`

	// Logs configures log record content. Which signals the receiver emits is
	// decided by pipeline membership, not here: log records are produced only
	// when the receiver is used in a logs pipeline.
	Logs LogsConfig `mapstructure:"logs"`
}

// LogsConfig configures the content of emitted log records.
//
// Note there is deliberately no toggle for request or response headers and
// bodies, and none for URL credentials: auth headers, cookies, payloads and
// userinfo are exactly what HTTP checks carry, so they are never recorded.
type LogsConfig struct {
	// IncludeTLSDetails includes certificate and handshake detail in HTTPS
	// records. Default true.
	IncludeTLSDetails bool `mapstructure:"include_tls_details"`
}

// TargetConfig configures a single probe target.
type TargetConfig struct {
	confighttp.ClientConfig `mapstructure:",squash"`

	// Method is "icmp" (default), "http", or "dns". The endpoint shape each
	// method accepts is enforced by Validate. Of the embedded
	// confighttp.ClientConfig only endpoint, timeout, tls, proxy_url and
	// headers are honoured; Validate rejects the rest.
	Method string `mapstructure:"method"`

	// PingCount is the number of ICMP packets to send per scrape, 1-100.
	// 0 means the default (3).
	PingCount int `mapstructure:"ping_count"`

	// HTTPMethod is the HTTP verb to use in HTTP mode. Default "HEAD".
	HTTPMethod string `mapstructure:"http_method"`

	// DNSServer overrides the DNS resolver for this target (e.g. "8.8.8.8:53").
	// If empty the system resolver is used and its address is detected from
	// /etc/resolv.conf (GetAdaptersAddresses on Windows).
	DNSServer string `mapstructure:"dns_server"`

	// DNSQuery is the name to query when method is "dns". Required for dns
	// targets. For dns targets the endpoint is the server being probed.
	DNSQuery string `mapstructure:"dns_query"`

	// DNSRecordType is the record type to query in dns mode: "A" (default), "AAAA", "CNAME", "MX", "TXT".
	DNSRecordType string `mapstructure:"dns_record_type"`
}

// Unmarshal decodes a target. confighttp.ClientConfig has its own Unmarshal
// method, which the embedded field promotes to TargetConfig; without this
// override only the ClientConfig keys would be decoded and the target's own
// fields (method, ping_count, ...) would be silently dropped.
func (t *TargetConfig) Unmarshal(conf *confmap.Conf) error {
	return conf.Unmarshal(t)
}

// TracerouteConfig configures optional traceroute probes.
type TracerouteConfig struct {
	// Enabled enables traceroute. Default false.
	Enabled bool `mapstructure:"enabled"`

	// Method is "udp" (default) or "icmp". On Linux "udp" needs no privilege
	// (the probe socket's error queue is read via IP_RECVERR) and "icmp" needs
	// root or CAP_NET_RAW; on macOS both need root; on Windows both use the
	// native IcmpSendEcho API and need no privilege.
	Method string `mapstructure:"method"`

	// MaxHops is the maximum TTL to probe, 1-255. 0 means the default (30).
	MaxHops int `mapstructure:"max_hops"`

	// Interval runs a traceroute every N times a target is checked. 0 disables
	// interval-based runs.
	Interval int `mapstructure:"interval"`

	// OnFailure traces an ICMP target on the first check whose packet loss is
	// >= FailureThreshold, then every 10th consecutive failing check while
	// Interval is 0 (the Interval schedule re-traces otherwise). A passing
	// check resets the count.
	OnFailure bool `mapstructure:"on_failure"`

	// FailureThreshold is the packet-loss ratio (0.0–1.0) that triggers on-failure traceroute. Default 0.5.
	FailureThreshold float64 `mapstructure:"failure_threshold"`

	// Timeout is the per-hop probe timeout. Default 3s. Must not exceed
	// collection_interval.
	Timeout time.Duration `mapstructure:"timeout"`

	// ProbesPerHop is the maximum number of probes sent for a single hop.
	// Probing stops at the first reply, so a hop that answers costs one probe
	// and only a silent hop costs more. 1-10; 0 means the default (3).
	//
	// Routers rate-limit ICMP time-exceeded generation, so a single probe
	// regularly goes unanswered on a path that is otherwise healthy — which
	// reads as a missing hop. Retrying only the silent hops recovers them
	// without multiplying traffic against the routers doing the limiting.
	ProbesPerHop int `mapstructure:"probes_per_hop"`

	// MaxConsecutiveTimeouts abandons the trace after this many unanswered
	// hops in a row. Without a bound, a path that stops answering walks the
	// full max_hops range at the per-hop timeout inside a single scrape.
	// 0 disables the early abort, leaving max_hops as the only bound.
	// Default 5.
	MaxConsecutiveTimeouts int `mapstructure:"max_consecutive_timeouts"`
}

// Validate checks the configuration for required fields and valid values.
// Every problem found is reported, not just the first.
func (c *Config) Validate() error {
	var errs error
	interval := c.CollectionInterval

	if len(c.Targets) == 0 {
		errs = multierr.Append(errs, errors.New("at least one target is required"))
	}

	for i, t := range c.Targets {
		errs = multierr.Append(errs, t.validate(i, interval))
	}

	if c.BatchSize < 0 {
		errs = multierr.Append(errs, errors.New("batch_size must be >= 0"))
	}

	if c.MaxConcurrentProbes < 0 || c.MaxConcurrentProbes > 256 {
		errs = multierr.Append(errs, errors.New("max_concurrent_probes must be between 0 and 256"))
	}

	// A jitter at or above the interval would push the start of one cycle into
	// the next.
	if c.Jitter < 0 {
		errs = multierr.Append(errs, errors.New("jitter must be >= 0"))
	} else if interval > 0 && c.Jitter > interval/2 {
		errs = multierr.Append(errs, errors.New("jitter must be at most half of collection_interval; the delay counts against the probe cycle budget"))
	}

	if c.Traceroute.Enabled {
		errs = multierr.Append(errs, c.Traceroute.validate(interval))
	}

	return errs
}

func (t *TargetConfig) validate(i int, interval time.Duration) error {
	var errs error

	method := t.Method
	if method == "" {
		method = MethodICMP
	}
	switch method {
	case MethodICMP, MethodHTTP, MethodDNS:
		if t.Endpoint == "" {
			errs = multierr.Append(errs, fmt.Errorf("target[%d]: endpoint is required", i))
		} else if err := validateEndpoint(method, t.Endpoint); err != nil {
			errs = multierr.Append(errs, fmt.Errorf("target[%d]: %w", i, err))
		}
	default:
		errs = multierr.Append(errs, fmt.Errorf("target[%d]: method %q is invalid; must be %q, %q, or %q", i, t.Method, MethodICMP, MethodHTTP, MethodDNS))
	}

	if method == MethodDNS && t.DNSQuery == "" {
		errs = multierr.Append(errs, fmt.Errorf("target[%d]: dns_query is required when method is %q", i, MethodDNS))
	}
	switch strings.ToUpper(t.DNSRecordType) {
	case "", "A", "AAAA", "CNAME", "MX", "TXT":
	default:
		errs = multierr.Append(errs, fmt.Errorf("target[%d]: dns_record_type %q is invalid; must be A, AAAA, CNAME, MX, or TXT", i, t.DNSRecordType))
	}

	if t.PingCount < 0 || t.PingCount > 100 {
		errs = multierr.Append(errs, fmt.Errorf("target[%d]: ping_count must be between 0 and 100", i))
	}

	// dns_server has the same shape as a DNS probe endpoint and the same IPv6
	// ambiguity, so it gets the same check.
	if t.DNSServer != "" {
		if err := validateEndpoint(MethodDNS, t.DNSServer); err != nil {
			errs = multierr.Append(errs, fmt.Errorf("target[%d]: dns_server: %w", i, err))
		}
	}

	if t.Timeout < 0 {
		errs = multierr.Append(errs, fmt.Errorf("target[%d]: timeout must be >= 0", i))
	} else if interval > 0 && t.Timeout > timeoutCeiling(interval) {
		errs = multierr.Append(errs, fmt.Errorf("target[%d]: timeout must be at most 80%% of collection_interval so the probe finishes inside the cycle budget", i))
	}

	for _, key := range unsupportedClientKeys(t.ClientConfig) {
		errs = multierr.Append(errs, fmt.Errorf("target[%d]: %s is not supported by networkcheck targets", i, key))
	}

	return errs
}

func (tc *TracerouteConfig) validate(interval time.Duration) error {
	var errs error

	switch strings.ToLower(tc.Method) {
	case "", "udp", "icmp":
	default:
		errs = multierr.Append(errs, fmt.Errorf("traceroute.method %q is invalid; must be \"udp\" or \"icmp\"", tc.Method))
	}
	// TTL is a single byte on the wire.
	if tc.MaxHops < 0 || tc.MaxHops > 255 {
		errs = multierr.Append(errs, errors.New("traceroute.max_hops must be between 0 and 255"))
	}
	if tc.ProbesPerHop < 0 || tc.ProbesPerHop > 10 {
		errs = multierr.Append(errs, errors.New("traceroute.probes_per_hop must be between 0 and 10"))
	}
	if tc.Timeout < 0 {
		errs = multierr.Append(errs, errors.New("traceroute.timeout must be >= 0"))
	} else if interval > 0 && tc.Timeout > interval {
		errs = multierr.Append(errs, errors.New("traceroute.timeout must not exceed collection_interval"))
	}
	if tc.Interval < 0 {
		errs = multierr.Append(errs, errors.New("traceroute.interval must be >= 0"))
	}
	if tc.FailureThreshold < 0 || tc.FailureThreshold > 1 {
		errs = multierr.Append(errs, errors.New("traceroute.failure_threshold must be between 0.0 and 1.0"))
	}
	if tc.MaxConsecutiveTimeouts < 0 {
		errs = multierr.Append(errs, errors.New("traceroute.max_consecutive_timeouts must be >= 0"))
	}

	return errs
}

// validateEndpoint checks that an endpoint has the shape its probe method
// consumes. Errors never quote the endpoint, which may carry credentials.
func validateEndpoint(method, endpoint string) error {
	switch method {
	case MethodICMP:
		if _, err := netip.ParseAddr(endpoint); err == nil {
			return nil
		}
		if strings.ContainsAny(endpoint, ":/@[]?# ") {
			return errors.New("icmp endpoint must be a bare hostname or IP address, without scheme, port, path, or userinfo")
		}
	case MethodDNS:
		const msg = "dns endpoint must be host or host:port, without scheme, path, or userinfo; an IPv6 address with a port must be bracketed, as in [2001:db8::1]:53"
		if strings.ContainsAny(endpoint, "/@?# ") {
			return errors.New(msg)
		}
		if _, err := netip.ParseAddr(endpoint); err == nil {
			return nil
		}
		host, port, err := net.SplitHostPort(endpoint)
		if err != nil {
			// No port: acceptable only as a plain hostname.
			if strings.ContainsAny(endpoint, ":[]") {
				return errors.New(msg)
			}
			return nil
		}
		if p, err := strconv.Atoi(port); host == "" || err != nil || p < 1 || p > 65535 {
			return errors.New(msg)
		}
		if strings.Contains(host, ":") {
			if _, err := netip.ParseAddr(host); err != nil {
				return errors.New(msg)
			}
		}
	case MethodHTTP:
		// The prober adds http:// to an endpoint given without a scheme.
		raw := endpoint
		if !strings.Contains(raw, "://") {
			raw = "http://" + raw
		}
		u, err := url.Parse(raw)
		if err != nil {
			// url.Error quotes the whole URL; keep only the reason.
			var uerr *url.Error
			if errors.As(err, &uerr) {
				err = uerr.Err
			}
			return fmt.Errorf("http endpoint is not a valid URL: %w", err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return errors.New("http endpoint scheme must be http or https")
		}
		if u.Host == "" {
			return errors.New("http endpoint has no host")
		}
	}
	return nil
}

// supportedClientKeys are the confighttp.ClientConfig keys the HTTP probe
// honours. The probe builds its own transport, so the rest would be accepted
// and silently ignored; they are rejected instead.
var supportedClientKeys = map[string]bool{
	"endpoint":  true,
	"timeout":   true,
	"tls":       true,
	"proxy_url": true,
	"headers":   true,
}

// unsupportedClientKeys returns the mapstructure keys of every set
// ClientConfig field outside supportedClientKeys. It walks the struct rather
// than listing fields so a field added by a confighttp upgrade is rejected
// until the probe learns to honour it.
func unsupportedClientKeys(cc confighttp.ClientConfig) []string {
	v := reflect.ValueOf(cc)
	var keys []string
	for i := range v.NumField() {
		f := v.Type().Field(i)
		key, _, _ := strings.Cut(f.Tag.Get("mapstructure"), ",")
		if !f.IsExported() || key == "" || supportedClientKeys[key] {
			continue
		}
		if !v.Field(i).IsZero() {
			keys = append(keys, key)
		}
	}
	return keys
}
