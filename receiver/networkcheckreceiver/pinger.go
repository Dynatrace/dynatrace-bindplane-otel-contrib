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
	"net"
	"strings"
	"time"
)

// PingResult holds the outcome of a single probe cycle.
type PingResult struct {
	// ICMP fields (populated when Method == "icmp")
	MinRTT     time.Duration
	AvgRTT     time.Duration
	MaxRTT     time.Duration
	PacketLoss float64 // 0.0–1.0

	// HTTP timing fields (populated when Method == "http")
	DNSLookup     time.Duration
	TCPConnect    time.Duration
	TLSHandshake  time.Duration
	RequestWrite  time.Duration
	ResponseRead  time.Duration
	TotalDuration time.Duration
	StatusCode    int

	// DNS fields (populated when Method == "dns")
	QueryDuration time.Duration
	QuerySuccess  bool
	QueryName     string

	// Method is the actual probe method used (may differ from config after fallback).
	Method string

	// --- Fields below are captured for log records only. Metric recording does
	// --- not read them, so adding to this block cannot change metric output.

	// ResolvedIP is the address the target resolved to for this probe. A
	// hostname behind a CDN or round-robin DNS resolves differently over time,
	// which is invisible in the timing numbers alone.
	ResolvedIP string

	// ResponseSize is the number of body bytes read from an HTTP response.
	ResponseSize int64

	// Protocol is the negotiated HTTP version, e.g. "HTTP/1.1" or "HTTP/2.0".
	Protocol string

	// ErrMessage and ErrPhase describe a failed probe. Without them a failed
	// HTTP check is indistinguishable between DNS failure, connection refused,
	// and timeout, since all three surface only as status code 0.
	ErrMessage string
	ErrPhase   string

	// TLS carries certificate and handshake detail, nil for non-TLS probes.
	TLS *TLSDetails
}

// TLSDetails is the certificate and handshake state observed during an HTTPS
// probe. The handshake already computes all of this; it was previously
// discarded.
type TLSDetails struct {
	Version            string
	CipherSuite        string
	NegotiatedProtocol string

	CertIssuer   string
	CertSubject  string
	CertNotAfter time.Time
	CertDaysLeft float64
}

// pinger is the interface implemented by icmpPinger and httpPinger.
type pinger interface {
	ping(ctx context.Context) (PingResult, error)
}

// dnsServerAddr turns a configured DNS server into a dialable host:port. The
// port defaults to 53, and an IPv6 address may come with or without brackets;
// splitting rather than looking for a colon keeps bare IPv6 addresses working.
func dnsServerAddr(server string) string {
	if _, _, err := net.SplitHostPort(server); err == nil {
		return server
	}
	return net.JoinHostPort(strings.Trim(server, "[]"), "53")
}
