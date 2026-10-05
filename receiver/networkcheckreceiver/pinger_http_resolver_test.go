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
	"context"
	"encoding/binary"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"golang.org/x/net/dns/dnsmessage"
)

// dnsResponder is an in-process DNS server. Every name it knows resolves to
// 127.0.0.1; anything else is NXDOMAIN.
type dnsResponder struct {
	addr        string
	known       map[string]bool // fully qualified, lower case
	truncateUDP bool

	mu   sync.Mutex
	seen []string // "udp TypeA probe.test."
	wg   sync.WaitGroup
}

// startDNSResponder serves UDP on addr and, when withTCP is set, TCP on the
// same port.
func startDNSResponder(t *testing.T, addr string, withTCP, truncateUDP bool, names ...string) *dnsResponder {
	t.Helper()
	s := &dnsResponder{known: map[string]bool{}, truncateUDP: truncateUDP}
	for _, n := range names {
		s.known[strings.ToLower(n)+"."] = true
	}

	var pc net.PacketConn
	var ln net.Listener
	for attempt := 0; ; attempt++ {
		var err error
		pc, err = net.ListenPacket("udp", addr)
		if err != nil {
			t.Skipf("cannot listen on udp %s: %v", addr, err)
		}
		if !withTCP {
			break
		}
		if ln, err = net.Listen("tcp", pc.LocalAddr().String()); err == nil {
			break
		}
		_ = pc.Close()
		require.Less(t, attempt, 10, "no port free for both udp and tcp: %v", err)
	}
	s.addr = pc.LocalAddr().String()

	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		buf := make([]byte, 1500)
		for {
			n, from, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			if out := s.reply("udp", buf[:n]); out != nil {
				_, _ = pc.WriteTo(out, from)
			}
		}
	}()
	if ln != nil {
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				s.wg.Add(1)
				go func() {
					defer s.wg.Done()
					defer c.Close()
					_ = c.SetDeadline(time.Now().Add(5 * time.Second))
					s.serveStream(c)
				}()
			}
		}()
	}

	t.Cleanup(func() {
		_ = pc.Close()
		if ln != nil {
			_ = ln.Close()
		}
		s.wg.Wait()
	})
	return s
}

// serveStream answers length-prefixed queries until the client closes.
func (s *dnsResponder) serveStream(c net.Conn) {
	for {
		var l [2]byte
		if _, err := io.ReadFull(c, l[:]); err != nil {
			return
		}
		msg := make([]byte, binary.BigEndian.Uint16(l[:]))
		if _, err := io.ReadFull(c, msg); err != nil {
			return
		}
		out := s.reply("tcp", msg)
		if out == nil {
			return
		}
		if _, err := c.Write(binary.BigEndian.AppendUint16(nil, uint16(len(out)))); err != nil {
			return
		}
		if _, err := c.Write(out); err != nil {
			return
		}
	}
}

func (s *dnsResponder) reply(network string, req []byte) []byte {
	var p dnsmessage.Parser
	h, err := p.Start(req)
	if err != nil {
		return nil
	}
	q, err := p.Question()
	if err != nil {
		return nil
	}
	name := strings.ToLower(q.Name.String())
	s.mu.Lock()
	s.seen = append(s.seen, network+" "+q.Type.String()+" "+name)
	s.mu.Unlock()

	hdr := dnsmessage.Header{ID: h.ID, Response: true, Authoritative: true, RecursionAvailable: true}
	known := s.known[name]
	if !known {
		hdr.RCode = dnsmessage.RCodeNameError
	}
	truncated := network == "udp" && s.truncateUDP
	hdr.Truncated = truncated

	b := dnsmessage.NewBuilder(nil, hdr)
	_ = b.StartQuestions()
	_ = b.Question(q)
	if known && !truncated && q.Type == dnsmessage.TypeA {
		_ = b.StartAnswers()
		_ = b.AResource(
			dnsmessage.ResourceHeader{Name: q.Name, Class: dnsmessage.ClassINET, TTL: 60},
			dnsmessage.AResource{A: [4]byte{127, 0, 0, 1}},
		)
	}
	out, err := b.Finish()
	if err != nil {
		return nil
	}
	return out
}

func (s *dnsResponder) queries() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.seen...)
}

// okServer starts an HTTP server on 127.0.0.1 and returns its port.
func okServer(t *testing.T) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	t.Cleanup(srv.Close)
	_, port, err := net.SplitHostPort(srv.Listener.Addr().String())
	require.NoError(t, err)
	return port
}

func TestDNSServerAddr(t *testing.T) {
	for in, want := range map[string]string{
		"8.8.8.8":         "8.8.8.8:53",
		"8.8.8.8:5353":    "8.8.8.8:5353",
		"::1":             "[::1]:53",
		"[::1]":           "[::1]:53",
		"[::1]:5353":      "[::1]:5353",
		"2405:201::1d01":  "[2405:201::1d01]:53",
		"fe80::1%en0":     "[fe80::1%en0]:53",
		"dns.example":     "dns.example:53",
		"dns.example:853": "dns.example:853",
	} {
		require.Equal(t, want, dnsServerAddr(in), in)
	}
}

func TestHTTPPinger_ResolverOverride(t *testing.T) {
	clearProxyEnv(t)
	port := okServer(t)

	t.Run("IPv4 with port", func(t *testing.T) {
		dns := startDNSResponder(t, "127.0.0.1:0", false, false, "probe.test")
		r := probeHTTP(t, httpTarget("http://probe.test:"+port+"/", func(tc *TargetConfig) { tc.DNSServer = dns.addr }))
		require.Equal(t, http.StatusOK, r.StatusCode, r.ErrMessage)
		require.Positive(t, r.DNSLookup)
		require.Equal(t, "127.0.0.1", r.ResolvedIP)
		require.Contains(t, dns.queries(), "udp TypeA probe.test.")
	})

	t.Run("bracketed IPv6 with port", func(t *testing.T) {
		dns := startDNSResponder(t, "[::1]:0", false, false, "probe.test")
		require.True(t, strings.HasPrefix(dns.addr, "[::1]:"), dns.addr)
		r := probeHTTP(t, httpTarget("http://probe.test:"+port+"/", func(tc *TargetConfig) { tc.DNSServer = dns.addr }))
		require.Equal(t, http.StatusOK, r.StatusCode, r.ErrMessage)
	})

	t.Run("bare IPv6 without port", func(t *testing.T) {
		// "::1" used to become "::1:53", which no dialler accepts.
		target := httpTarget("http://probe.test:"+port+"/", func(tc *TargetConfig) {
			tc.DNSServer = "::1"
			tc.Timeout = time.Second
		})
		if pc, err := net.ListenPacket("udp", "[::1]:53"); err == nil {
			_ = pc.Close()
			dns := startDNSResponder(t, "[::1]:53", false, false, "probe.test")
			r := probeHTTP(t, target)
			require.Equal(t, http.StatusOK, r.StatusCode, r.ErrMessage)
			require.NotEmpty(t, dns.queries())
			return
		}
		// Port 53 cannot be bound here (it usually needs privileges), so the
		// lookup fails and the error names the address actually dialled.
		r := probeHTTP(t, target)
		require.Equal(t, "dns", r.ErrPhase)
		require.NotContains(t, r.ErrMessage, "too many colons")
		require.Contains(t, r.ErrMessage, "[::1]:53")
	})

	t.Run("NXDOMAIN fails in the dns phase", func(t *testing.T) {
		dns := startDNSResponder(t, "127.0.0.1:0", false, false)
		r := probeHTTP(t, httpTarget("http://nonexistent.invalid:"+port+"/", func(tc *TargetConfig) { tc.DNSServer = dns.addr }))
		require.Equal(t, 0, r.StatusCode)
		require.Equal(t, "dns", r.ErrPhase)
		require.Contains(t, r.ErrMessage, "no such host")
	})
}

// A truncated UDP answer must be retried over TCP. The resolver asks Dial for
// "tcp"; a Dial that ignored the network dialled UDP again, received the same
// truncated, empty answer and reported the host as nonexistent.
func TestHTTPPinger_ResolverRetriesTruncatedAnswerOverTCP(t *testing.T) {
	clearProxyEnv(t)
	port := okServer(t)
	dns := startDNSResponder(t, "127.0.0.1:0", true, true, "big.test")

	r := probeHTTP(t, httpTarget("http://big.test:"+port+"/", func(tc *TargetConfig) { tc.DNSServer = dns.addr }))
	require.Equal(t, http.StatusOK, r.StatusCode, r.ErrMessage)
	q := dns.queries()
	require.Contains(t, q, "udp TypeA big.test.")
	require.Contains(t, q, "tcp TypeA big.test.")
}

// The prober hands every HTTP target the system's first nameserver for the
// dns.server label. Used as a resolver override it bypassed the OS resolver
// (nameserver failover, search domains, split DNS), and with an IPv6 first
// nameserver it broke every hostname probe.
func TestHTTPPinger_SystemNameserverDoesNotReplaceOSResolver(t *testing.T) {
	clearProxyEnv(t)
	label := startDNSResponder(t, "127.0.0.1:0", false, false, "probe.test")

	p, err := newHTTPPinger(context.Background(), componenttest.NewNopHost(), componenttest.NewNopTelemetrySettings(),
		httpTarget("http://probe.test:"+okServer(t)+"/", func(tc *TargetConfig) { tc.Timeout = 500 * time.Millisecond }),
		label.addr)
	require.NoError(t, err)
	_, err = p.ping(context.Background())
	require.NoError(t, err)
	// Whatever the OS resolver makes of probe.test, it must not have been
	// sent to the label-only nameserver.
	require.Empty(t, label.queries())
}
