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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

func defaultLogsConfig() LogsConfig {
	return LogsConfig{IncludeTLSDetails: true}
}

func TestBuildHTTPLogRecord_Success(t *testing.T) {
	start := time.Now().Add(-2 * time.Second)
	ts := &targetState{
		cfg:       TargetConfig{Method: MethodHTTP, HTTPMethod: "GET"},
		dnsServer: "1.1.1.1:53",
	}
	ts.cfg.Endpoint = "https://example.com"

	r := PingResult{
		Method:        MethodHTTP,
		StatusCode:    200,
		DNSLookup:     2 * time.Millisecond,
		TCPConnect:    11 * time.Millisecond,
		TLSHandshake:  184 * time.Millisecond,
		RequestWrite:  300 * time.Microsecond,
		ResponseRead:  8 * time.Millisecond,
		TotalDuration: 206 * time.Millisecond,
		ResolvedIP:    "93.184.216.34",
		ResponseSize:  1256,
		Protocol:      "HTTP/2.0",
		TLS: &TLSDetails{
			Version:      "TLS 1.3",
			CipherSuite:  "TLS_AES_128_GCM_SHA256",
			CertIssuer:   "CN=Test CA",
			CertSubject:  "CN=example.com",
			CertNotAfter: time.Now().Add(30 * 24 * time.Hour),
			CertDaysLeft: 30,
		},
	}

	rec := plog.NewLogRecord()
	buildHTTPLogRecord(rec, ts, r, start, time.Now(), defaultLogsConfig())

	require.Equal(t, plog.SeverityNumberInfo, rec.SeverityNumber())
	require.Equal(t, start.UnixNano(), rec.Timestamp().AsTime().UnixNano(),
		"timestamp must be request start, not completion")

	attrs := rec.Attributes()
	v, ok := attrs.Get("http.response.status_code")
	require.True(t, ok)
	require.EqualValues(t, 200, v.Int())

	v, ok = attrs.Get("server.resolved_ip")
	require.True(t, ok)
	require.Equal(t, "93.184.216.34", v.Str())

	v, ok = attrs.Get("tls.cert.days_remaining")
	require.True(t, ok)
	require.InDelta(t, 30, v.Double(), 0.001)

	phases, ok := rec.Body().Map().Get("phases")
	require.True(t, ok)
	pm := phases.Map()
	dns, _ := pm.Get("dns_ms")
	require.InDelta(t, 2.0, dns.Double(), 0.001)
	tlsMs, _ := pm.Get("tls_ms")
	require.InDelta(t, 184.0, tlsMs.Double(), 0.001)

	// Sub-millisecond phases must survive as fractions rather than truncating,
	// which is what msFloat exists to guarantee.
	write, _ := pm.Get("write_ms")
	require.InDelta(t, 0.3, write.Double(), 0.001)

	_, hasErr := rec.Body().Map().Get("error")
	require.False(t, hasErr, "successful check must not carry an error block")
}

func TestBuildHTTPLogRecord_FailureIsError(t *testing.T) {
	ts := &targetState{cfg: TargetConfig{Method: MethodHTTP}}
	ts.cfg.Endpoint = "https://127.0.0.1:9"

	r := PingResult{
		Method:        MethodHTTP,
		StatusCode:    0,
		TotalDuration: 5 * time.Millisecond,
		ErrMessage:    "dial tcp 127.0.0.1:9: connect: connection refused",
		ErrPhase:      "connect",
	}

	rec := plog.NewLogRecord()
	buildHTTPLogRecord(rec, ts, r, time.Now(), time.Now(), defaultLogsConfig())

	require.Equal(t, plog.SeverityNumberError, rec.SeverityNumber())

	v, ok := rec.Attributes().Get("error.type")
	require.True(t, ok)
	require.Equal(t, "connect", v.Str())

	errBlock, ok := rec.Body().Map().Get("error")
	require.True(t, ok)
	msg, ok := errBlock.Map().Get("message")
	require.True(t, ok)
	require.Contains(t, msg.Str(), "connection refused",
		"error text is the whole point: status 0 alone cannot distinguish failure modes")
}

func TestBuildHTTPLogRecord_RedactsCredentials(t *testing.T) {
	ts := &targetState{cfg: TargetConfig{Method: MethodHTTP}}
	ts.cfg.Endpoint = "https://admin:pw7@example.com/health"

	rec := plog.NewLogRecord()
	buildHTTPLogRecord(rec, ts, PingResult{Method: MethodHTTP, StatusCode: 200},
		time.Now(), time.Now(), defaultLogsConfig())

	v, ok := rec.Attributes().Get("server.address")
	require.True(t, ok)
	require.NotContains(t, v.Str(), "pw7")
	require.Contains(t, v.Str(), "example.com")
}

func TestBuildTracerouteLogRecord(t *testing.T) {
	ts := &targetState{cfg: TargetConfig{}, dnsServer: "8.8.8.8:53"}
	ts.cfg.Endpoint = "www.cloudflare.com"

	tr := TraceResult{
		DestIP:  "104.16.124.96",
		Method:  "udp",
		MaxHops: 30,
		Reached: true,
		Hops: []HopResult{
			{Index: 1, Address: "172.16.1.1", RTT: 443 * time.Microsecond},
			{Index: 2, Address: "10.112.162.67", RTT: 11 * time.Millisecond},
			{Index: 3, Address: unansweredHopAddress, TimedOut: true},
			{Index: 4, Address: "104.16.124.96", RTT: 13 * time.Millisecond},
		},
	}

	rec := plog.NewLogRecord()
	buildTracerouteLogRecord(rec, ts, tr, time.Now(), time.Now(), defaultLogsConfig())

	require.Equal(t, plog.SeverityNumberInfo, rec.SeverityNumber())

	attrs := rec.Attributes()
	hopCount, _ := attrs.Get("traceroute.hop_count")
	require.EqualValues(t, 4, hopCount.Int())
	answered, _ := attrs.Get("traceroute.hops_answered")
	require.EqualValues(t, 3, answered.Int())
	reached, _ := attrs.Get("traceroute.reached_dest")
	require.True(t, reached.Bool())

	hops, ok := rec.Body().Map().Get("hops")
	require.True(t, ok)
	require.Equal(t, 4, hops.Slice().Len(), "unanswered hops must still appear in the path")

	// Hop 1 is sub-millisecond — the exact case that used to truncate to zero.
	h0 := hops.Slice().At(0).Map()
	rtt, ok := h0.Get("rtt_ms")
	require.True(t, ok)
	require.InDelta(t, 0.443, rtt.Double(), 0.001)

	// The timed-out hop carries no latency: the only duration available for it
	// is the timeout we chose, which is not a measurement.
	h2 := hops.Slice().At(2).Map()
	timedOut, _ := h2.Get("timed_out")
	require.True(t, timedOut.Bool())
	_, hasRTT := h2.Get("rtt_ms")
	require.False(t, hasRTT)
	addr, _ := h2.Get("address")
	require.Equal(t, unansweredHopAddress, addr.Str())
}

// A trace that did not reach its destination is a path problem worth
// surfacing without making it an error, whether it gave up early or walked to
// the TTL ceiling.
func TestBuildTracerouteLogRecord_IncompleteIsWarn(t *testing.T) {
	cases := []struct {
		name    string
		tr      TraceResult
		aborted bool
	}{
		{
			name: "aborted early",
			tr: TraceResult{Method: "udp", MaxHops: 30, AbortedEarly: true, Hops: []HopResult{
				{Index: 1, Address: "172.16.1.1", RTT: time.Millisecond},
				{Index: 2, Address: unansweredHopAddress, TimedOut: true},
			}},
			aborted: true,
		},
		{
			name: "hit the ttl ceiling",
			tr: TraceResult{Method: "udp", MaxHops: 2, Hops: []HopResult{
				{Index: 1, Address: "172.16.1.1", RTT: time.Millisecond},
				{Index: 2, Address: "10.0.0.1", RTT: 2 * time.Millisecond},
			}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := &targetState{cfg: TargetConfig{}}
			ts.cfg.Endpoint = "blackhole.example"

			rec := plog.NewLogRecord()
			buildTracerouteLogRecord(rec, ts, tc.tr, time.Now(), time.Now(), defaultLogsConfig())

			require.Equal(t, plog.SeverityNumberWarn, rec.SeverityNumber())
			reached, _ := rec.Attributes().Get("traceroute.reached_dest")
			require.False(t, reached.Bool())
			aborted, _ := rec.Attributes().Get("traceroute.aborted_early")
			require.Equal(t, tc.aborted, aborted.Bool(),
				"an early bail-out must be distinguishable from a genuinely short path")
		})
	}
}

// A failed request stops partway through. Phases after the break never ran,
// and reporting them as 0ms hid which phases did complete. A successful
// request keeps all six entries, so its shape is unchanged.
func TestBuildHTTPLogRecord_PhasesShape(t *testing.T) {
	cases := []struct {
		name string
		r    PingResult
		want map[string]float64
	}{
		{
			name: "failure after dns keeps dns and total",
			r: PingResult{Method: MethodHTTP, DNSLookup: 3 * time.Millisecond, TotalDuration: 5 * time.Second,
				ErrPhase: "connect", ErrMessage: "connection refused"},
			want: map[string]float64{"dns_ms": 3, "total_ms": 5000},
		},
		{
			name: "failure in tls keeps the phases before it",
			r: PingResult{Method: MethodHTTP, DNSLookup: time.Millisecond, TCPConnect: 2 * time.Millisecond,
				TotalDuration: 10 * time.Millisecond, ErrPhase: "tls", ErrMessage: "bad certificate"},
			want: map[string]float64{"dns_ms": 1, "connect_ms": 2, "total_ms": 10},
		},
		{
			name: "failure before any phase keeps only total",
			r:    PingResult{Method: MethodHTTP, TotalDuration: time.Millisecond, ErrPhase: "setup", ErrMessage: "bad url"},
			want: map[string]float64{"total_ms": 1},
		},
		{
			name: "plain http success keeps a zero tls phase",
			r: PingResult{Method: MethodHTTP, StatusCode: 200, DNSLookup: time.Millisecond, TCPConnect: time.Millisecond,
				RequestWrite: time.Millisecond, ResponseRead: time.Millisecond, TotalDuration: 4 * time.Millisecond},
			want: map[string]float64{"dns_ms": 1, "connect_ms": 1, "tls_ms": 0, "write_ms": 1, "ttfb_ms": 1, "total_ms": 4},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ts := &targetState{cfg: TargetConfig{Method: MethodHTTP}}
			ts.cfg.Endpoint = "https://example.com"

			rec := plog.NewLogRecord()
			buildHTTPLogRecord(rec, ts, tc.r, time.Now(), time.Now(), defaultLogsConfig())

			phases, ok := rec.Body().Map().Get("phases")
			require.True(t, ok)
			got := map[string]float64{}
			for k, v := range phases.Map().All() {
				got[k] = v.Double()
			}
			require.Equal(t, tc.want, got)
		})
	}
}

func TestTLSDetailsOmittedWhenDisabled(t *testing.T) {
	ts := &targetState{cfg: TargetConfig{Method: MethodHTTP}}
	ts.cfg.Endpoint = "https://example.com"

	r := PingResult{
		Method:     MethodHTTP,
		StatusCode: 200,
		TLS:        &TLSDetails{Version: "TLS 1.3", CertIssuer: "CN=Test CA", CertDaysLeft: 10},
	}

	cfg := defaultLogsConfig()
	cfg.IncludeTLSDetails = false

	rec := plog.NewLogRecord()
	buildHTTPLogRecord(rec, ts, r, time.Now(), time.Now(), cfg)

	_, ok := rec.Body().Map().Get("tls")
	require.False(t, ok)

	// The summary attribute stays regardless, since cert expiry is the field
	// most likely to be alerted on.
	_, ok = rec.Attributes().Get("tls.cert.days_remaining")
	require.True(t, ok)
}

func TestNoHeadersOrBodyEverRecorded(t *testing.T) {
	ts := &targetState{cfg: TargetConfig{Method: MethodHTTP}}
	ts.cfg.Endpoint = "https://example.com"

	rec := plog.NewLogRecord()
	buildHTTPLogRecord(rec, ts, PingResult{Method: MethodHTTP, StatusCode: 200, ResponseSize: 42},
		time.Now(), time.Now(), defaultLogsConfig())

	// Guards the one part of this feature that can cause a security incident.
	banned := []string{"header", "cookie", "authorization", "body"}
	for k := range rec.Attributes().All() {
		for _, b := range banned {
			require.NotContains(t, strings.ToLower(k), b)
		}
	}
	for k := range rec.Body().Map().All() {
		for _, b := range banned {
			require.NotContains(t, strings.ToLower(k), b)
		}
	}
}

func TestFailurePhase(t *testing.T) {
	now := time.Now()
	refused := errors.New("connect: connection refused")

	cases := []struct {
		name string
		in   phaseTimings
		want string
	}{
		{
			// ConnectDone fires even when the connection is refused, so a
			// timestamp-only heuristic blames "request" — the phase after the
			// one that actually failed.
			name: "refused connection reports connect, not request",
			in:   phaseTimings{connectDone: now, connectErr: refused},
			want: "connect",
		},
		{
			name: "dns failure",
			in:   phaseTimings{dnsStart: now, dnsDone: now, dnsErr: errors.New("no such host")},
			want: "dns",
		},
		{
			name: "tls failure",
			in: phaseTimings{
				dnsStart: now, dnsDone: now, connectDone: now,
				tlsStart: now, tlsDone: now, tlsErr: errors.New("bad certificate"),
			},
			want: "tls",
		},
		{
			name: "timeout waiting for response",
			in:   phaseTimings{dnsStart: now, dnsDone: now, connectDone: now, wroteRequest: now},
			want: "response",
		},
		{
			name: "hung mid-connect with no error reported",
			in:   phaseTimings{dnsStart: now, dnsDone: now},
			want: "connect",
		},
		{
			name: "nothing started",
			in:   phaseTimings{},
			want: "setup",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, failurePhase(tc.in))
		})
	}
}

func TestFailurePhase_IPLiteralDialFailure(t *testing.T) {
	now := time.Now()
	// An IP-literal target runs no DNS lookup, so dnsDone stays zero. Before
	// ConnectStart was tracked, a dial that hung was reported as "setup".
	got := failurePhase(phaseTimings{connectStart: now})
	require.Equal(t, "connect", got)
}
