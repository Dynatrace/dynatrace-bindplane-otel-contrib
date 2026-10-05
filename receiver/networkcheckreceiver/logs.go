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
	"net/url"
	"regexp"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// redactEndpoint strips any userinfo from a target endpoint. A target may be
// configured as https://user:pass@host, and the endpoint reaches log records,
// resource attributes, and error messages. Credentials must not follow it there.
//
// The username goes too, not just the password: a token passed as the username
// alone (https://TOKEN@host) is common, and nothing downstream needs either.
func redactEndpoint(endpoint string) string {
	if !strings.Contains(endpoint, "@") {
		return endpoint
	}
	if u, err := url.Parse(endpoint); err == nil && u.Host != "" {
		if u.User == nil {
			// The "@" sits in the path, query or fragment, not in userinfo.
			// Truncating at it would rewrite the endpoint to a different host.
			return endpoint
		}
		u.User = nil
		return u.String()
	}
	// Unparseable, or no authority to take userinfo from (a bare
	// "user:pass@host" parses as scheme "user" with an opaque rest): drop
	// everything up to the last "@" rather than risk emitting a credential.
	i := strings.LastIndex(endpoint, "@")
	if scheme := strings.Index(endpoint, "://"); scheme >= 0 && scheme < i {
		return endpoint[:scheme+3] + endpoint[i+1:]
	}
	return endpoint[i+1:]
}

// userinfoInURL matches the userinfo segment of a URL embedded in free text.
//
// "?" and "#" are excluded along with "/" so the match cannot run past the URL
// authority: an "@" inside a query or fragment (an email address in a
// parameter, say) would otherwise swallow the host on the way to it. "@" itself
// is allowed so that an unencoded "@" in a password is consumed up to the last
// one in the authority, rather than leaving the password's tail behind.
var userinfoInURL = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.\-]*://)[^/\s"?#]+@`)

// userinfoBare matches a schemeless "user:pass@" or "token@" at the start of a
// word, which is how an ICMP or DNS endpoint shows up in resolver errors
// ("lookup user:pass@host: no such host").
//
// Deliberate over-match: an email address in a message loses its local part
// too. A false positive only removes text from an error message, while a false
// negative leaks a credential, so the trade goes this way.
var userinfoBare = regexp.MustCompile(`(^|[\s"'(])[^/\s"'?#]+@`)

// quotedText matches a double-quoted fragment of an error message.
var quotedText = regexp.MustCompile(`"[^"]*"`)

// redactMessage strips credentials from any URL embedded in free-form text.
//
// redactEndpoint must not be used for this: it assumes its input is a URL, so a
// message containing an unrelated "@" would be truncated to whatever followed
// it, discarding the failure detail the message exists to carry.
func redactMessage(msg string) string {
	msg = userinfoInURL.ReplaceAllString(msg, "$1")
	return userinfoBare.ReplaceAllString(msg, "$1")
}

// redactErr strips credentials from an error's text.
//
// A *url.Error is rebuilt from its parts rather than pattern-matched: net/url
// embeds the raw URL in it, and a password containing a quote, space, "/", "?"
// or "#" is exactly what makes the URL unparseable and also what stops the
// free-text patterns short of the "@".
func redactErr(err error) error {
	if err == nil {
		return nil
	}
	var ue *url.Error
	if !errors.As(err, &ue) {
		return errors.New(redactMessage(err.Error()))
	}

	var clean error
	if ue.Op == "parse" {
		// A URL that failed to parse cannot be redacted reliably: url.Parse
		// cuts at "#" before it looks for userinfo, so "https://u:pa#ss@h"
		// arrives here as "https://u:pa" with no "@" left to anchor on, and the
		// reason quotes the piece it choked on (`invalid port ":pa" after
		// host`). Every caller already names the redacted endpoint, so the URL
		// is dropped and quoted fragments are masked.
		clean = fmt.Errorf("parse url: %s", quotedText.ReplaceAllString(redactMessage(ue.Err.Error()), `"***"`))
	} else {
		clean = fmt.Errorf("%s %s: %w", ue.Op, redactEndpoint(ue.URL), redactErr(ue.Err))
	}

	if _, direct := err.(*url.Error); direct {
		return clean
	}
	// Wrapped: the outer text repeats the url.Error's text verbatim.
	return errors.New(redactMessage(strings.Replace(err.Error(), ue.Error(), clean.Error(), 1)))
}

// buildHTTPLogRecord renders one HTTP probe as a log record. The record is the
// transaction: phases sit together in the body so a slow check can be
// attributed to a phase, which averaged metric series cannot express.
func buildHTTPLogRecord(lr plog.LogRecord, ts *targetState, r PingResult, startedAt time.Time, observed time.Time, cfg LogsConfig) {
	lr.SetTimestamp(pcommon.NewTimestampFromTime(startedAt))
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(observed))

	endpoint := redactEndpoint(ts.cfg.Endpoint)

	failed := r.StatusCode == 0 || r.ErrMessage != ""
	if failed {
		lr.SetSeverityNumber(plog.SeverityNumberError)
		lr.SetSeverityText("ERROR")
	} else {
		lr.SetSeverityNumber(plog.SeverityNumberInfo)
		lr.SetSeverityText("INFO")
	}

	attrs := lr.Attributes()
	attrs.EnsureCapacity(9)
	attrs.PutStr("server.address", endpoint)
	attrs.PutStr("http.request.method", methodOrDefault(ts.cfg.HTTPMethod))
	if r.StatusCode != 0 {
		attrs.PutInt("http.response.status_code", int64(r.StatusCode))
	}
	if r.ResponseSize > 0 {
		attrs.PutInt("http.response.size", r.ResponseSize)
	}
	if r.Protocol != "" {
		attrs.PutStr("network.protocol.version", r.Protocol)
	}
	if r.ResolvedIP != "" {
		attrs.PutStr("server.resolved_ip", r.ResolvedIP)
	}
	if ts.dnsServer != "" {
		attrs.PutStr("dns.server", ts.dnsServer)
	}
	if r.TLS != nil {
		attrs.PutDouble("tls.cert.days_remaining", r.TLS.CertDaysLeft)
	}
	if failed && r.ErrPhase != "" {
		attrs.PutStr("error.type", r.ErrPhase)
	}

	body := lr.Body().SetEmptyMap()

	// Phase semantics follow the metric definitions: connect_ms runs from the
	// first dial start, write_ms from the end of the TLS handshake (or of the
	// connect for plain HTTP), and ttfb_ms is time to first byte rather than a
	// full body read. On failure, phases that completed are non-zero and the
	// one that broke is zero.
	phases := body.PutEmptyMap("phases")
	phases.EnsureCapacity(6)
	putPhase := func(k string, d time.Duration) {
		// A failed request stops partway: the phases after the break never
		// ran, and six zeros would hide which ones did complete.
		if failed && d == 0 {
			return
		}
		phases.PutDouble(k, msFloat(d))
	}
	putPhase("dns_ms", r.DNSLookup)
	putPhase("connect_ms", r.TCPConnect)
	putPhase("tls_ms", r.TLSHandshake)
	putPhase("write_ms", r.RequestWrite)
	putPhase("ttfb_ms", r.ResponseRead)
	phases.PutDouble("total_ms", msFloat(r.TotalDuration))

	if r.TLS != nil && cfg.IncludeTLSDetails {
		t := body.PutEmptyMap("tls")
		t.PutStr("version", r.TLS.Version)
		t.PutStr("cipher", r.TLS.CipherSuite)
		if r.TLS.NegotiatedProtocol != "" {
			t.PutStr("negotiated_protocol", r.TLS.NegotiatedProtocol)
		}
		if !r.TLS.CertNotAfter.IsZero() {
			cert := t.PutEmptyMap("cert")
			cert.PutStr("issuer", r.TLS.CertIssuer)
			cert.PutStr("subject", r.TLS.CertSubject)
			cert.PutStr("not_after", r.TLS.CertNotAfter.UTC().Format(time.RFC3339))
			cert.PutDouble("days_remaining", r.TLS.CertDaysLeft)
		}
	}

	if failed {
		e := body.PutEmptyMap("error")
		if r.ErrPhase != "" {
			e.PutStr("phase", r.ErrPhase)
		}
		if r.ErrMessage != "" {
			e.PutStr("message", redactMessage(r.ErrMessage))
		}
	}
}

// buildTracerouteLogRecord renders one traceroute as a single record. The path
// is the unit of meaning, so hops stay together and ordered — including hops
// that never answered, which as metrics can only be represented by a separate
// status series.
func buildTracerouteLogRecord(lr plog.LogRecord, ts *targetState, tr TraceResult, startedAt time.Time, observed time.Time, _ LogsConfig) {
	lr.SetTimestamp(pcommon.NewTimestampFromTime(startedAt))
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(observed))

	endpoint := redactEndpoint(ts.cfg.Endpoint)

	answered, retried := 0, 0
	for _, h := range tr.Hops {
		if !h.TimedOut {
			answered++
		}
		if h.Probes > 1 {
			retried++
		}
	}

	// A trace that reached its destination is routine. One that gave up early,
	// or walked to the TTL ceiling without arriving, is a path problem worth
	// surfacing without making it an error.
	if tr.Reached {
		lr.SetSeverityNumber(plog.SeverityNumberInfo)
		lr.SetSeverityText("INFO")
	} else {
		lr.SetSeverityNumber(plog.SeverityNumberWarn)
		lr.SetSeverityText("WARN")
	}

	attrs := lr.Attributes()
	attrs.EnsureCapacity(9)
	attrs.PutStr("server.address", endpoint)
	if tr.DestIP != "" {
		attrs.PutStr("server.resolved_ip", tr.DestIP)
	}
	if tr.Method != "" {
		attrs.PutStr("traceroute.method", tr.Method)
	}
	attrs.PutInt("traceroute.hop_count", int64(len(tr.Hops)))
	attrs.PutInt("traceroute.hops_answered", int64(answered))
	if retried > 0 {
		attrs.PutInt("traceroute.hops_retried", int64(retried))
	}
	attrs.PutBool("traceroute.reached_dest", tr.Reached)
	attrs.PutBool("traceroute.aborted_early", tr.AbortedEarly)
	if ts.dnsServer != "" {
		attrs.PutStr("dns.server", ts.dnsServer)
	}

	body := lr.Body().SetEmptyMap()
	hops := body.PutEmptySlice("hops")
	hops.EnsureCapacity(len(tr.Hops))
	for _, h := range tr.Hops {
		m := hops.AppendEmpty().SetEmptyMap()
		m.EnsureCapacity(5)
		m.PutInt("index", int64(h.Index))
		m.PutStr("address", h.Address)
		m.PutBool("timed_out", h.TimedOut)
		// Probes above 1 means earlier attempts went unanswered, which
		// separates a hop that is merely rate-limiting from a healthy one.
		if h.Probes > 0 {
			m.PutInt("probes", int64(h.Probes))
		}
		// A hop that did not answer has no latency, only the timeout we chose.
		// Emitting that as an rtt would read as a real, very slow measurement.
		if !h.TimedOut {
			m.PutDouble("rtt_ms", msFloat(h.RTT))
		}
	}
}

func methodOrDefault(m string) string {
	if m == "" {
		return "HEAD"
	}
	return m
}
