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
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/dns/dnsmessage"
)

// fakeDNS is an in-process authoritative server for the .test zone, listening
// on UDP and TCP on the same port. It records every question it receives as
// "<transport> <type> <name>".
//
//	a.test      A 10.1.1.1 (no AAAA)       aaaa.test   AAAA fd00::1 (no A)
//	alias.test  CNAME a.test (+A for A)    mx.test     MX    txt.test   TXT
//	probe.test  A 127.0.0.1                localhost   A 10.9.9.9
//	empty.test  NOERROR, no answers        silent.test never answers
//	big.test    TC over UDP, A over TCP    bigdead.test TC over UDP, TCP closes
//	badid.test  wrong-ID NXDOMAIN first, then the real A answer
//	anything else NXDOMAIN
type fakeDNS struct {
	addr string
	udp  net.PacketConn
	tcp  net.Listener
	wg   sync.WaitGroup

	mu   sync.Mutex
	seen []string
}

func startFakeDNS(t *testing.T, host string) *fakeDNS {
	t.Helper()
	s := &fakeDNS{}
	for attempt := 0; s.tcp == nil; attempt++ {
		pc, err := net.ListenPacket("udp", net.JoinHostPort(host, "0"))
		if err != nil {
			t.Skipf("cannot listen on %s: %v", host, err)
		}
		ln, err := net.Listen("tcp", pc.LocalAddr().String())
		if err != nil {
			_ = pc.Close()
			require.Less(t, attempt, 5, "no port free on both UDP and TCP: %v", err)
			continue
		}
		s.udp, s.tcp = pc, ln
	}
	s.addr = s.udp.LocalAddr().String()

	s.wg.Add(2)
	go s.serveUDP()
	go s.serveTCP()
	t.Cleanup(func() {
		_ = s.udp.Close()
		_ = s.tcp.Close()
		s.wg.Wait()
	})
	return s
}

func (s *fakeDNS) queries() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.seen...)
}

func (s *fakeDNS) serveUDP() {
	defer s.wg.Done()
	buf := make([]byte, 1500)
	for {
		n, from, err := s.udp.ReadFrom(buf)
		if err != nil {
			return
		}
		for _, reply := range s.answer(buf[:n], "udp") {
			_, _ = s.udp.WriteTo(reply, from)
		}
	}
}

func (s *fakeDNS) serveTCP() {
	defer s.wg.Done()
	for {
		c, err := s.tcp.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer c.Close()
			_ = c.SetDeadline(time.Now().Add(5 * time.Second))
			var size [2]byte
			if _, err := io.ReadFull(c, size[:]); err != nil {
				return
			}
			msg := make([]byte, binary.BigEndian.Uint16(size[:]))
			if _, err := io.ReadFull(c, msg); err != nil {
				return
			}
			for _, reply := range s.answer(msg, "tcp") {
				_, _ = c.Write(append(binary.BigEndian.AppendUint16(nil, uint16(len(reply))), reply...))
			}
		}()
	}
}

func (s *fakeDNS) answer(msg []byte, transport string) [][]byte {
	var p dnsmessage.Parser
	h, err := p.Start(msg)
	if err != nil {
		return nil
	}
	q, err := p.Question()
	if err != nil {
		return nil
	}
	name := strings.ToLower(q.Name.String())
	s.mu.Lock()
	s.seen = append(s.seen, transport+" "+strings.TrimPrefix(q.Type.String(), "Type")+" "+name)
	s.mu.Unlock()

	rh := dnsmessage.ResourceHeader{Name: q.Name, Class: dnsmessage.ClassINET, TTL: 60}
	target := dnsmessage.MustNewName("a.test.")
	a := func(ip ...byte) func(*dnsmessage.Builder) error {
		return func(b *dnsmessage.Builder) error { return b.AResource(rh, dnsmessage.AResource{A: [4]byte(ip)}) }
	}
	reply := func(id uint16, rcode dnsmessage.RCode, tc bool, rrs ...func(*dnsmessage.Builder) error) []byte {
		b := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: id, Response: true, RecursionAvailable: true, RCode: rcode, Truncated: tc})
		_ = b.StartQuestions()
		_ = b.Question(q)
		_ = b.StartAnswers()
		for _, rr := range rrs {
			_ = rr(&b)
		}
		out, _ := b.Finish()
		return out
	}
	ok := dnsmessage.RCodeSuccess
	is := func(t dnsmessage.Type) bool { return q.Type == t }

	switch name {
	case "silent.test.":
		return nil
	case "a.test.":
		if is(dnsmessage.TypeA) {
			return [][]byte{reply(h.ID, ok, false, a(10, 1, 1, 1))}
		}
		return [][]byte{reply(h.ID, ok, false)}
	case "aaaa.test.":
		if is(dnsmessage.TypeAAAA) {
			return [][]byte{reply(h.ID, ok, false, func(b *dnsmessage.Builder) error {
				return b.AAAAResource(rh, dnsmessage.AAAAResource{AAAA: [16]byte{0xfd, 15: 1}})
			})}
		}
		return [][]byte{reply(h.ID, ok, false)}
	case "alias.test.":
		rrs := []func(*dnsmessage.Builder) error{func(b *dnsmessage.Builder) error {
			return b.CNAMEResource(rh, dnsmessage.CNAMEResource{CNAME: target})
		}}
		if is(dnsmessage.TypeA) {
			rrs = append(rrs, func(b *dnsmessage.Builder) error {
				return b.AResource(dnsmessage.ResourceHeader{Name: target, Class: dnsmessage.ClassINET, TTL: 60}, dnsmessage.AResource{A: [4]byte{10, 1, 1, 1}})
			})
		}
		return [][]byte{reply(h.ID, ok, false, rrs...)}
	case "mx.test.":
		if is(dnsmessage.TypeMX) {
			return [][]byte{reply(h.ID, ok, false, func(b *dnsmessage.Builder) error {
				return b.MXResource(rh, dnsmessage.MXResource{Pref: 10, MX: target})
			})}
		}
		return [][]byte{reply(h.ID, ok, false)}
	case "txt.test.":
		if is(dnsmessage.TypeTXT) {
			return [][]byte{reply(h.ID, ok, false, func(b *dnsmessage.Builder) error {
				return b.TXTResource(rh, dnsmessage.TXTResource{TXT: []string{"v=probe"}})
			})}
		}
		return [][]byte{reply(h.ID, ok, false)}
	case "probe.test.":
		if is(dnsmessage.TypeA) {
			return [][]byte{reply(h.ID, ok, false, a(127, 0, 0, 1))}
		}
		return [][]byte{reply(h.ID, ok, false)}
	case "localhost.":
		if is(dnsmessage.TypeA) {
			return [][]byte{reply(h.ID, ok, false, a(10, 9, 9, 9))}
		}
		return [][]byte{reply(h.ID, ok, false)}
	case "empty.test.":
		return [][]byte{reply(h.ID, ok, false)}
	case "big.test.", "bigdead.test.":
		if transport == "udp" {
			return [][]byte{reply(h.ID, ok, true)}
		}
		if name == "bigdead.test." {
			return nil
		}
		return [][]byte{reply(h.ID, ok, false, a(10, 2, 2, 2))}
	case "badid.test.":
		return [][]byte{
			reply(h.ID^0xffff, dnsmessage.RCodeNameError, false),
			reply(h.ID, ok, false, a(10, 3, 3, 3)),
		}
	}
	return [][]byte{reply(h.ID, dnsmessage.RCodeNameError, false)}
}

func dnsProbe(t *testing.T, server, query, recordType string, timeout time.Duration) (PingResult, time.Duration) {
	t.Helper()
	tc := TargetConfig{Method: MethodDNS, DNSQuery: query, DNSRecordType: recordType}
	tc.Endpoint = server
	tc.Timeout = timeout
	start := time.Now()
	r, err := newDNSPinger(tc).ping(context.Background())
	require.NoError(t, err, "a failed query is a measurement, not an error")
	return r, time.Since(start)
}

func TestDNSPingRecordTypes(t *testing.T) {
	s := startFakeDNS(t, "127.0.0.1")

	for _, tc := range []struct {
		query, recordType string
		wantErr           string // empty means success
	}{
		{"a.test", "A", ""},
		{"a.test.", "A", ""},
		{"a.test", "AAAA", "no AAAA records in answer"},
		{"aaaa.test", "AAAA", ""},
		{"aaaa.test", "A", "no A records in answer"},
		{"alias.test", "CNAME", ""},
		{"alias.test", "A", ""},
		{"a.test", "CNAME", "no CNAME records in answer"},
		{"mx.test", "MX", ""},
		{"a.test", "MX", "no MX records in answer"},
		{"txt.test", "TXT", ""},
		{"empty.test", "A", "no A records in answer"},
		{"nope.test", "A", "rcode NXDOMAIN"},
	} {
		t.Run(tc.recordType+"_"+tc.query, func(t *testing.T) {
			before := len(s.queries())
			r, _ := dnsProbe(t, s.addr, tc.query, tc.recordType, 2*time.Second)

			require.Equal(t, MethodDNS, r.Method)
			require.Equal(t, tc.query, r.QueryName)
			require.Equal(t, tc.wantErr == "", r.QuerySuccess)
			require.Equal(t, tc.wantErr, r.ErrMessage)
			if tc.wantErr != "" {
				require.Equal(t, "dns", r.ErrPhase)
			}
			requireTimed(t, r.QueryDuration)

			fqdn := strings.TrimSuffix(tc.query, ".") + "."
			require.Equal(t, []string{"udp " + tc.recordType + " " + fqdn}, s.queries()[before:],
				"exactly one query, of the configured type, for the configured name")
		})
	}
}

func TestDNSPingTruncatedRetriesOverTCP(t *testing.T) {
	s := startFakeDNS(t, "127.0.0.1")

	r, _ := dnsProbe(t, s.addr, "big.test", "A", 2*time.Second)
	require.True(t, r.QuerySuccess, r.ErrMessage)
	require.Equal(t, []string{"udp A big.test.", "tcp A big.test."}, s.queries())

	r, _ = dnsProbe(t, s.addr, "bigdead.test", "A", 2*time.Second)
	require.False(t, r.QuerySuccess)
	require.Equal(t, "dns", r.ErrPhase)
	require.True(t, strings.HasPrefix(r.ErrMessage, "truncated and tcp retry failed: "), r.ErrMessage)
}

func TestDNSPingTimeout(t *testing.T) {
	s := startFakeDNS(t, "127.0.0.1")

	r, took := dnsProbe(t, s.addr, "silent.test", "A", 300*time.Millisecond)
	require.False(t, r.QuerySuccess)
	require.Equal(t, "dns", r.ErrPhase)
	require.Equal(t, "timeout", r.ErrMessage)
	require.InDelta(t, 300*time.Millisecond, took, float64(200*time.Millisecond))
	require.InDelta(t, 300*time.Millisecond, r.QueryDuration, float64(200*time.Millisecond))
}

// A resolver answers "localhost" from the hosts file without asking anyone, so
// a probe built on it reported a healthy server that was never contacted.
func TestDNSPingLocalhostReachesServer(t *testing.T) {
	s := startFakeDNS(t, "127.0.0.1")

	r, _ := dnsProbe(t, s.addr, "localhost", "A", 2*time.Second)
	require.True(t, r.QuerySuccess, r.ErrMessage)
	require.Equal(t, []string{"udp A localhost."}, s.queries())
}

func TestDNSPingIgnoresMismatchedID(t *testing.T) {
	s := startFakeDNS(t, "127.0.0.1")

	r, _ := dnsProbe(t, s.addr, "badid.test", "A", 2*time.Second)
	require.True(t, r.QuerySuccess, "the wrong-ID NXDOMAIN must be skipped: %s", r.ErrMessage)
}

func TestDNSPingCancelReturnsError(t *testing.T) {
	s := startFakeDNS(t, "127.0.0.1")
	tc := TargetConfig{Method: MethodDNS, DNSQuery: "silent.test"}
	tc.Endpoint = s.addr
	tc.Timeout = 5 * time.Second

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()
	_, err := newDNSPinger(tc).ping(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(start), 400*time.Millisecond)
}

func TestDNSPingIPv6Server(t *testing.T) {
	s := startFakeDNS(t, "::1")

	require.True(t, strings.HasPrefix(s.addr, "[::1]:"), s.addr)
	r, _ := dnsProbe(t, s.addr, "a.test", "A", 2*time.Second)
	require.True(t, r.QuerySuccess, r.ErrMessage)
}

func TestDNSPingDropsEndpointUserinfo(t *testing.T) {
	s := startFakeDNS(t, "127.0.0.1")

	r, _ := dnsProbe(t, "user:pw@"+s.addr, "a.test", "A", 2*time.Second)
	require.True(t, r.QuerySuccess, r.ErrMessage)

	r, _ = dnsProbe(t, "user:pw@127.0.0.1:1", "a.test", "A", 300*time.Millisecond)
	require.False(t, r.QuerySuccess)
	require.NotContains(t, r.ErrMessage, "pw")
}

func TestDNSServerAddrForDNSTarget(t *testing.T) {
	for in, want := range map[string]string{
		"8.8.8.8":       "8.8.8.8:53",
		"8.8.8.8:5353":  "8.8.8.8:5353",
		"dns.google":    "dns.google:53",
		"::1":           "[::1]:53",
		"[::1]":         "[::1]:53",
		"[::1]:5353":    "[::1]:5353",
		"2001:db8::53":  "[2001:db8::53]:53",
		"fe80::1%en0":   "[fe80::1%en0]:53",
		"127.0.0.1:53":  "127.0.0.1:53",
		"resolver:1053": "resolver:1053",
	} {
		require.Equal(t, want, dnsServerAddr(in), in)
	}
	tc := TargetConfig{}
	tc.Endpoint = "::1"
	require.Equal(t, "[::1]:53", newDNSPinger(tc).server)
}
