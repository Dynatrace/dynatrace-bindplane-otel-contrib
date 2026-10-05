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
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/collector/component"
	"golang.org/x/net/http/httpproxy"
)

// httpPinger performs a single HTTP request and records per-phase timings using httptrace.
type httpPinger struct {
	client     *http.Client
	url        *url.URL
	httpMethod string
	header     http.Header
	host       string // Host header override; empty sends the URL's host

	// proxied is true when requests go through a proxy. The DNS and connect
	// phases then describe the hop to the proxy, and the address connected to
	// is the proxy's, not the target's.
	proxied bool
}

const (
	// defaultHTTPTimeout bounds a probe whose target sets no timeout.
	defaultHTTPTimeout = 10 * time.Second

	// maxBodyRead caps how much of a response body a probe reads (1 MiB).
	maxBodyRead = 1 << 20
)

// newHTTPPinger builds the probe client for one HTTP target. ctx loads the TLS
// config; host and set are unused until the client comes from
// ClientConfig.ToClient, which needs them for auth and middleware extensions.
// The last argument is the system nameserver the prober detected for the
// dns.server label, and is deliberately not used for resolution.
func newHTTPPinger(ctx context.Context, _ component.Host, _ component.TelemetrySettings, target TargetConfig, _ string) (*httpPinger, error) {
	ep := target.Endpoint
	u, err := url.Parse(ep)
	if err != nil {
		return nil, fmt.Errorf("target %s: invalid endpoint: %w", redactEndpoint(ep), urlErrReason(err))
	}
	if (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("target %s: invalid endpoint: want an http or https URL with a host", redactEndpoint(ep))
	}

	httpMethod := target.HTTPMethod
	if httpMethod == "" {
		httpMethod = http.MethodHead
	}
	if _, err := http.NewRequest(httpMethod, ep, nil); err != nil {
		return nil, fmt.Errorf("target %s: invalid http_method %q", redactEndpoint(ep), httpMethod)
	}

	timeout := target.Timeout
	if timeout <= 0 {
		timeout = defaultHTTPTimeout
	}

	// The probe URL never changes (redirects are not followed), so the proxy
	// decision is made once, here.
	var proxy *url.URL
	if target.ProxyURL != "" {
		if proxy, err = url.ParseRequestURI(target.ProxyURL); err != nil {
			return nil, fmt.Errorf("target %s: invalid proxy_url: %w", redactEndpoint(ep), urlErrReason(err))
		}
	} else if proxy, err = httpproxy.FromEnvironment().ProxyFunc()(u); err != nil {
		// Same HTTP_PROXY / HTTPS_PROXY / NO_PROXY rules as
		// http.ProxyFromEnvironment, which caches the environment for the life
		// of the process. The error quotes the raw proxy value, credentials
		// included, so it is not passed on.
		return nil, fmt.Errorf("target %s: invalid proxy in the HTTP_PROXY/HTTPS_PROXY environment", redactEndpoint(ep))
	}

	dialer := &net.Dialer{Timeout: timeout}
	// Only an explicit dns_server replaces the resolver. Forcing lookups to
	// the detected system nameserver bypassed the OS resolver's failover to
	// further nameservers, its search domains and split DNS, and turned the
	// probe into a test of one server rather than of what clients on the host
	// see.
	if target.DNSServer != "" {
		dialer.Resolver = overrideResolver(target.DNSServer, timeout)
	}

	tlsCfg, err := target.TLS.LoadTLSConfig(ctx)
	if err != nil {
		return nil, fmt.Errorf("target %s: loading TLS config: %w", redactEndpoint(ep), redactErr(err))
	}

	// Deliberate simplification: the transport is built here rather than by
	// ClientConfig.ToClient because the probe needs its own dialer (the
	// dns_server resolver override) and nothing between it and the wire, so the
	// httptrace timings describe the network rather than auth, middleware or
	// otelhttp round trippers. The cost is that only endpoint, timeout, tls,
	// proxy_url and headers are honoured. ToClient exposes no dialer hook;
	// switch to it when it does, and the remaining confighttp fields come for
	// free.
	transport := &http.Transport{
		Proxy:               http.ProxyURL(proxy), // nil proxy: direct
		DialContext:         dialer.DialContext,
		TLSClientConfig:     tlsCfg,
		TLSHandshakeTimeout: timeout,
		// A fresh connection per probe, so every probe pays and measures DNS,
		// connect and TLS instead of reusing a pooled connection.
		DisableKeepAlives: true,
		// A custom dialer or TLS config otherwise turns HTTP/2 off; servers
		// that offer h2 are probed over h2, as a browser would.
		ForceAttemptHTTP2: true,
	}

	header := http.Header{}
	var host string
	for name, v := range target.Headers.Iter {
		// net/http writes req.Host, not a Host header, so the override has to
		// go there. string(v), not v.String(): the opaque type prints as
		// "[REDACTED]".
		if strings.EqualFold(name, "Host") {
			host = string(v)
			continue
		}
		header.Set(name, string(v))
	}

	client := &http.Client{
		Transport: transport,
		Timeout:   timeout,
		// Do not follow redirects; we measure the first response.
		CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
			return http.ErrUseLastResponse
		},
	}

	return &httpPinger{
		client:     client,
		url:        u,
		httpMethod: httpMethod,
		header:     header,
		host:       host,
		proxied:    proxy != nil,
	}, nil
}

func (p *httpPinger) ping(ctx context.Context) (PingResult, error) {
	var pt probeTrace
	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, pt.hooks()), p.httpMethod, p.url.String(), nil)
	if err != nil {
		// Unreachable: the URL and method were validated in newHTTPPinger.
		return PingResult{}, fmt.Errorf("building request for %s: %w", redactEndpoint(p.url.String()), urlErrReason(err))
	}
	// Credentials in the endpoint's userinfo are sent as basic auth by
	// net/http itself, unless a configured Authorization header is present.
	req.Header = p.header.Clone()
	if p.host != "" {
		req.Host = p.host
	}

	start := time.Now()
	resp, err := p.client.Do(req)
	// Taken before the body is read: the total runs to the response headers.
	end := time.Now()
	if resp != nil {
		defer resp.Body.Close()
	}
	// A cancelled or expired caller context says nothing about the target, so
	// it is returned as an error rather than recorded as a failed probe.
	if ctxErr := ctx.Err(); ctxErr != nil {
		return PingResult{}, ctxErr
	}

	t, resolvedIP, tlsState := pt.snapshot()
	if p.proxied {
		// The address dialled was the proxy's; the target's is not observable.
		resolvedIP = ""
	}
	res := PingResult{
		TotalDuration: end.Sub(start),
		Method:        MethodHTTP,
		ResolvedIP:    resolvedIP,
	}
	res.DNSLookup, res.TCPConnect, res.TLSHandshake, res.RequestWrite, res.ResponseRead = t.durations()
	if tlsState != nil {
		res.TLS = tlsDetailsFrom(*tlsState, end)
	}

	if err != nil {
		// A failed request is a measurement, not an error. It keeps the
		// phases that completed before the one that broke.
		res.ErrMessage = redactErr(err).Error()
		res.ErrPhase = failurePhase(t)
		return res, nil
	}

	res.StatusCode = resp.StatusCode
	res.Protocol = resp.Proto
	// Deliberate simplification: the body is read only to size it, and at most
	// maxBodyRead of it, so a large or endless GET cannot turn every probe into
	// a download. ResponseSize is therefore capped and a bigger body reads as
	// exactly maxBodyRead; a truncation flag would need a PingResult field.
	res.ResponseSize, _ = io.CopyN(io.Discard, resp.Body, maxBodyRead)
	return res, nil
}

// probeTrace records what the httptrace hooks observe during one request.
// Hooks run on transport goroutines and can still fire after Do has returned
// (a dial that outlives a timed-out request, the losing dial of a Happy
// Eyeballs race), so every field is guarded by mu.
type probeTrace struct {
	mu         sync.Mutex
	t          phaseTimings
	resolvedIP string
	tls        *tls.ConnectionState
}

// mark sets *at to now under the lock.
func (pt *probeTrace) mark(at *time.Time) {
	now := time.Now()
	pt.mu.Lock()
	*at = now
	pt.mu.Unlock()
}

func (pt *probeTrace) snapshot() (phaseTimings, string, *tls.ConnectionState) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	return pt.t, pt.resolvedIP, pt.tls
}

func (pt *probeTrace) hooks() *httptrace.ClientTrace {
	return &httptrace.ClientTrace{
		DNSStart: func(httptrace.DNSStartInfo) { pt.mark(&pt.t.dnsStart) },
		DNSDone: func(info httptrace.DNSDoneInfo) {
			now := time.Now()
			pt.mu.Lock()
			defer pt.mu.Unlock()
			pt.t.dnsDone, pt.t.dnsErr = now, info.Err
			if len(info.Addrs) > 0 {
				pt.resolvedIP = info.Addrs[0].IP.String()
			}
		},
		ConnectStart: func(_, _ string) {
			now := time.Now()
			pt.mu.Lock()
			defer pt.mu.Unlock()
			// One dial starts per address tried, and both address families
			// may race; the phase runs from the first.
			if pt.t.connectStart.IsZero() {
				pt.t.connectStart = now
			}
		},
		ConnectDone: func(_, addr string, err error) {
			now := time.Now()
			pt.mu.Lock()
			defer pt.mu.Unlock()
			if !pt.t.connectDone.IsZero() {
				return // a losing parallel dial reporting after the winner
			}
			// ConnectDone receives the dial target, which is a hostname when
			// the transport dials by name. Only take it when it is genuinely
			// an address, otherwise server.resolved_ip would carry a
			// hostname; the DNSDone value stands in that case.
			if host, _, splitErr := net.SplitHostPort(addr); splitErr == nil && net.ParseIP(host) != nil {
				pt.resolvedIP = host
			}
			// A failed dial moves on to the next address, so the error stands
			// only until one succeeds.
			pt.t.connectErr = err
			if err == nil {
				pt.t.connectDone = now
			}
		},
		TLSHandshakeStart: func() { pt.mark(&pt.t.tlsStart) },
		TLSHandshakeDone: func(cs tls.ConnectionState, err error) {
			now := time.Now()
			pt.mu.Lock()
			defer pt.mu.Unlock()
			pt.t.tlsDone, pt.t.tlsErr = now, err
			if err == nil {
				pt.tls = &cs
			}
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			now := time.Now()
			pt.mu.Lock()
			defer pt.mu.Unlock()
			pt.t.wroteRequest, pt.t.writeErr = now, info.Err
		},
		GotFirstResponseByte: func() { pt.mark(&pt.t.gotFirstByte) },
	}
}

// overrideResolver sends every lookup to server instead of the configured
// nameservers. Dial honours the network it is asked for: after a truncated UDP
// answer the resolver retries over TCP, and dialling UDP again would hand it
// the same truncated answer.
func overrideResolver(server string, timeout time.Duration) *net.Resolver {
	addr := dnsServerAddr(server)
	return &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			d := net.Dialer{Timeout: timeout}
			return d.DialContext(ctx, network, addr)
		},
	}
}

// urlErrReason returns err without the URL that *url.Error quotes. It is for
// errors about raw configuration, where that URL is the unparsed setting: a
// password with characters the URL grammar rejects is quoted in a form the
// free-text redaction cannot recognise, and the message names the target
// already.
func urlErrReason(err error) error {
	var ue *url.Error
	if errors.As(err, &ue) {
		err = ue.Err
	}
	return redactErr(err)
}

// phaseTimings carries what the trace hooks observed about one request.
type phaseTimings struct {
	dnsStart, dnsDone                    time.Time
	connectStart, connectDone            time.Time
	tlsStart, tlsDone                    time.Time
	wroteRequest, gotFirstByte           time.Time
	dnsErr, connectErr, tlsErr, writeErr error
}

// durations returns the length of each phase that completed. A phase that
// failed or never finished is zero, so a failed probe still reports the phases
// that ran before the one that broke.
//
// Connect runs from the first dial start (from DNS done only when no dial
// start was seen), so an IP-literal target, which has no DNS phase, still
// measures it. Write runs from the end of the TLS handshake, or of the connect
// for plain HTTP, so it does not include the handshake. First byte runs from
// the request being written.
func (t phaseTimings) durations() (dns, connect, tlsHandshake, write, firstByte time.Duration) {
	span := func(from, to time.Time, err error) time.Duration {
		if from.IsZero() || to.IsZero() || err != nil {
			return 0
		}
		return to.Sub(from)
	}
	connectFrom := t.connectStart
	if connectFrom.IsZero() {
		connectFrom = t.dnsDone
	}
	writeFrom := t.tlsDone
	if writeFrom.IsZero() {
		writeFrom = t.connectDone
	}
	return span(t.dnsStart, t.dnsDone, t.dnsErr),
		span(connectFrom, t.connectDone, t.connectErr),
		span(t.tlsStart, t.tlsDone, t.tlsErr),
		span(writeFrom, t.wroteRequest, t.writeErr),
		span(t.wroteRequest, t.gotFirstByte, nil)
}

// failurePhase names the request phase that broke. A bare status code of 0 says
// a check failed but not where, which is the difference between a DNS problem
// and a slow origin.
//
// Reported errors are checked before timestamps because every hook fires on
// failure as well as success: a refused connection still calls ConnectDone, so
// timing alone would blame the phase after the one that actually failed.
func failurePhase(t phaseTimings) string {
	switch {
	case t.dnsErr != nil:
		return "dns"
	case t.connectErr != nil:
		return "connect"
	case t.tlsErr != nil:
		return "tls"
	case t.writeErr != nil:
		return "request"

	// No hook reported an error, so fall back to the first phase that started
	// and never finished.
	case !t.dnsStart.IsZero() && t.dnsDone.IsZero():
		return "dns"
	case !t.connectStart.IsZero() && t.connectDone.IsZero():
		// Covers an IP-literal endpoint, where no DNS lookup runs and the dial
		// is the first thing to happen.
		return "connect"
	case !t.dnsDone.IsZero() && t.connectDone.IsZero():
		return "connect"
	case !t.tlsStart.IsZero() && t.tlsDone.IsZero():
		return "tls"
	case !t.connectDone.IsZero() && t.wroteRequest.IsZero():
		return "request"
	case !t.wroteRequest.IsZero() && t.gotFirstByte.IsZero():
		return "response"
	case t.dnsStart.IsZero() && t.connectDone.IsZero():
		// Nothing started: usually an invalid URL or a proxy rejection.
		return "setup"
	default:
		return "unknown"
	}
}

// tlsDetailsFrom extracts certificate and handshake detail from a completed
// handshake. DisableKeepAlives means every probe performs a real handshake, so
// this is always current rather than cached from an earlier connection.
func tlsDetailsFrom(cs tls.ConnectionState, now time.Time) *TLSDetails {
	d := &TLSDetails{
		Version:            tls.VersionName(cs.Version),
		CipherSuite:        tls.CipherSuiteName(cs.CipherSuite),
		NegotiatedProtocol: cs.NegotiatedProtocol,
	}
	if len(cs.PeerCertificates) > 0 {
		leaf := cs.PeerCertificates[0]
		d.CertIssuer = leaf.Issuer.String()
		d.CertSubject = leaf.Subject.String()
		d.CertNotAfter = leaf.NotAfter
		d.CertDaysLeft = leaf.NotAfter.Sub(now).Hours() / 24
	}
	return d
}
