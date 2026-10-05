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

// defaultHTTPTimeout bounds a probe whose target sets no timeout.
const defaultHTTPTimeout = 10 * time.Second

// newHTTPPinger builds the probe client for one HTTP target. ctx, host and set
// are what confighttp needs to resolve auth and middleware extensions.
func newHTTPPinger(ctx context.Context, _ component.Host, _ component.TelemetrySettings, target TargetConfig, dnsServer string) (*httpPinger, error) {
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

	// Build a custom dialer that uses the specified DNS server if provided.
	dialServer := dnsServer
	if target.DNSServer != "" {
		dialServer = target.DNSServer
	}

	dialer := &net.Dialer{Timeout: timeout}
	if dialServer != "" {
		resolver := &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, _, _ string) (net.Conn, error) {
				d := net.Dialer{}
				addr := dialServer
				if !strings.Contains(addr, ":") {
					addr = addr + ":53"
				}
				return d.DialContext(ctx, "udp", addr)
			},
		}
		dialer.Resolver = resolver
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
	var (
		dnsStart, dnsDone         time.Time
		connectStart, connectDone time.Time
		tlsStart, tlsDone         time.Time
		wroteRequest              time.Time
		gotFirstResponseByte      time.Time
		requestStart              time.Time
	)

	var (
		resolvedIP string
		tlsState   tls.ConnectionState
		haveTLS    bool

		// Each of these hooks fires on failure as well as success, so the
		// timestamps alone cannot say which phase broke. The errors can.
		dnsErr, connectErr, tlsErr, writeErr error
	)

	trace := &httptrace.ClientTrace{
		DNSStart: func(_ httptrace.DNSStartInfo) { dnsStart = time.Now() },
		DNSDone: func(info httptrace.DNSDoneInfo) {
			dnsDone = time.Now()
			dnsErr = info.Err
			if len(info.Addrs) > 0 {
				resolvedIP = info.Addrs[0].IP.String()
			}
		},
		ConnectStart: func(_, _ string) { connectStart = time.Now() },
		ConnectDone: func(_, addr string, err error) {
			connectDone = time.Now()
			connectErr = err
			// ConnectDone receives the dial target, which is a hostname when the
			// transport dials by name. Only take it when it is genuinely an
			// address, otherwise server.resolved_ip would carry a hostname; the
			// DNSDone value stands in that case.
			if host, _, splitErr := net.SplitHostPort(addr); splitErr == nil && net.ParseIP(host) != nil {
				resolvedIP = host
			}
		},
		TLSHandshakeStart: func() { tlsStart = time.Now() },
		TLSHandshakeDone: func(cs tls.ConnectionState, err error) {
			tlsDone = time.Now()
			tlsErr = err
			if err == nil {
				tlsState, haveTLS = cs, true
			}
		},
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			wroteRequest = time.Now()
			writeErr = info.Err
		},
		GotFirstResponseByte: func() { gotFirstResponseByte = time.Now() },
	}

	req, err := http.NewRequestWithContext(httptrace.WithClientTrace(ctx, trace), p.httpMethod, p.url.String(), nil)
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

	requestStart = time.Now()
	resp, err := p.client.Do(req)
	end := time.Now()

	if p.proxied {
		// The address dialled was the proxy's; the target's is not observable.
		resolvedIP = ""
	}

	statusCode := 0
	var responseSize int64
	var protocol string
	if resp != nil {
		statusCode = resp.StatusCode
		protocol = resp.Proto
		// Drain and close body so the connection can be reused. The byte count
		// is the response size; only the timing was kept before.
		responseSize, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
	}

	if err != nil {
		// Record a failed probe but don't return an error — it's a valid measurement.
		return PingResult{
			TotalDuration: end.Sub(requestStart),
			StatusCode:    0,
			Method:        MethodHTTP,
			ResolvedIP:    resolvedIP,
			ErrMessage:    urlErrReason(err).Error(),
			ErrPhase: failurePhase(phaseTimings{
				dnsStart: dnsStart, dnsDone: dnsDone,
				connectStart: connectStart, connectDone: connectDone,
				tlsStart: tlsStart, tlsDone: tlsDone,
				wroteRequest: wroteRequest, gotFirstByte: gotFirstResponseByte,
				dnsErr: dnsErr, connectErr: connectErr, tlsErr: tlsErr, writeErr: writeErr,
			}),
		}, nil
	}

	var (
		dnsLookup    time.Duration
		tcpConnect   time.Duration
		tlsHandshake time.Duration
		reqWrite     time.Duration
		respRead     time.Duration
	)
	if !dnsStart.IsZero() && !dnsDone.IsZero() {
		dnsLookup = dnsDone.Sub(dnsStart)
	}
	if !dnsDone.IsZero() && !connectDone.IsZero() {
		tcpConnect = connectDone.Sub(dnsDone)
	}
	if !tlsStart.IsZero() && !tlsDone.IsZero() {
		tlsHandshake = tlsDone.Sub(tlsStart)
	}
	if !connectDone.IsZero() && !wroteRequest.IsZero() {
		reqWrite = wroteRequest.Sub(connectDone)
	}
	if !wroteRequest.IsZero() && !gotFirstResponseByte.IsZero() {
		respRead = gotFirstResponseByte.Sub(wroteRequest)
	}

	res := PingResult{
		DNSLookup:     dnsLookup,
		TCPConnect:    tcpConnect,
		TLSHandshake:  tlsHandshake,
		RequestWrite:  reqWrite,
		ResponseRead:  respRead,
		TotalDuration: end.Sub(requestStart),
		StatusCode:    statusCode,
		Method:        MethodHTTP,
		ResolvedIP:    resolvedIP,
		ResponseSize:  responseSize,
		Protocol:      protocol,
	}
	if haveTLS {
		res.TLS = tlsDetailsFrom(tlsState, end)
	}
	return res, nil
}

// urlErrReason returns err without the URL that *url.Error quotes. That URL is
// the configured endpoint: the record already carries it redacted as
// server.address, net/http keeps the username when it masks the password, and a
// password with characters the URL grammar rejects is quoted in a form the
// free-text redaction cannot recognise.
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
