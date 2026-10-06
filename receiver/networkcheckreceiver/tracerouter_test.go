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
	"errors"
	"net"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/dns/dnsmessage"
	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

// TestICMPPacketConnIsNotNetConn pins the interface fact behind a crash in
// traceICMP: ipv4.NewPacketConn type-asserts its argument to net.Conn without a
// comma-ok, so handing it an *icmp.PacketConn panics. traceICMP must use
// conn.IPv4PacketConn() instead. If this test ever fails, x/net has widened
// *icmp.PacketConn and the workaround can be revisited.
func TestICMPPacketConnIsNotNetConn(t *testing.T) {
	var conn *icmp.PacketConn
	var v any = conn

	_, isPacketConn := v.(net.PacketConn)
	require.True(t, isPacketConn, "*icmp.PacketConn should satisfy net.PacketConn")

	_, isConn := v.(net.Conn)
	require.False(t, isConn,
		"*icmp.PacketConn must not satisfy net.Conn; ipv4.NewPacketConn would panic on it")
}

// TestSetTTLViaIPv4PacketConn exercises the accessor traceICMP relies on, which
// is the call that previously panicked.
func TestSetTTLViaIPv4PacketConn(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("datagram ICMP sockets do not exist on Windows; the native traceroute path does not use them")
	}
	// "udp4" is the unprivileged ICMP socket flavor; raw "ip4:icmp" needs root.
	conn, err := icmp.ListenPacket("udp4", "0.0.0.0")
	if err != nil {
		t.Skipf("ICMP socket unavailable in this environment: %v", err)
	}
	defer conn.Close()

	p4 := conn.IPv4PacketConn()
	require.NotNil(t, p4, "IPv4PacketConn should be non-nil for an IPv4 listener")
	require.NotPanics(t, func() {
		_ = p4.SetTTL(5)
	})
}

// TestTraceUnansweredHopsAreMarkedAndBounded checks the two reporting rules for
// a path that never answers: every silent hop is flagged TimedOut (so no bogus
// latency reaches the metrics builder), and the walk stops after
// defaultMaxConsecutiveTimeouts instead of running the full max_hops range.
func TestTraceUnansweredHopsAreMarkedAndBounded(t *testing.T) {
	// 192.0.2.0/24 is TEST-NET-1 (RFC 5737) and is not routed, so probes to it
	// go unanswered without depending on any particular network.
	const blackhole = "192.0.2.1"

	tr := newTracerouter(&TracerouteConfig{
		Method:  "udp",
		MaxHops: 30,
		Timeout: 50 * time.Millisecond,
		// Set explicitly: the zero value disables the early abort entirely,
		// which would leave the assertions below testing nothing.
		MaxConsecutiveTimeouts: defaultMaxConsecutiveTimeouts,
		ProbesPerHop:           1,
	}, blackhole, "")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	res, err := tr.trace(ctx)
	if err != nil {
		// No raw ICMP socket (macOS without root) or no route to the blackhole.
		t.Skipf("traceroute unavailable in this environment: %v", err)
	}
	hops := res.Hops

	// Early hops toward the blackhole are real routers that do answer, so the
	// bound is on the run of consecutive silent hops, not the total.
	longestRun, run := 0, 0
	for _, hop := range hops {
		if hop.TimedOut {
			run++
			if run > longestRun {
				longestRun = run
			}
			require.Equal(t, unansweredHopAddress, hop.Address,
				"a timed-out hop should carry the unanswered address")
			require.Zero(t, hop.RTT,
				"hop %d timed out; its RTT must not carry the probe timeout as a latency",
				hop.Index)
			continue
		}
		run = 0
		require.NotEqual(t, unansweredHopAddress, hop.Address,
			"a hop that answered should carry a real address")
	}

	require.LessOrEqual(t, longestRun, defaultMaxConsecutiveTimeouts,
		"the walk must abandon the path after %d consecutive timeouts", defaultMaxConsecutiveTimeouts)
	require.Less(t, len(hops), 30,
		"the walk must stop early on an unanswered path rather than reaching max_hops")
}

func TestProbesPerHopDefaultsAndClamps(t *testing.T) {
	cases := []struct {
		name string
		cfg  *TracerouteConfig
		want int
	}{
		{"unset uses the traceroute convention", &TracerouteConfig{}, defaultProbesPerHop},
		{"zero is treated as unset", &TracerouteConfig{ProbesPerHop: 0}, defaultProbesPerHop},
		{"negative is treated as unset", &TracerouteConfig{ProbesPerHop: -1}, defaultProbesPerHop},
		{"explicit single probe is honored", &TracerouteConfig{ProbesPerHop: 1}, 1},
		{"explicit higher count is honored", &TracerouteConfig{ProbesPerHop: 5}, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr := newTracerouter(tc.cfg, "example.com", "")
			require.Equal(t, tc.want, tr.probesPerHop())
		})
	}
}

func TestAbortAfterIsConfigurable(t *testing.T) {
	// 0 disables the early abort, leaving max_hops as the only bound. That is
	// distinct from "unset", which must keep the default.
	tr := newTracerouter(&TracerouteConfig{MaxConsecutiveTimeouts: 0}, "example.com", "")
	require.Equal(t, 0, tr.abortAfter())

	tr = newTracerouter(&TracerouteConfig{MaxConsecutiveTimeouts: 8}, "example.com", "")
	require.Equal(t, 8, tr.abortAfter())

	tr = newTracerouter(&TracerouteConfig{MaxConsecutiveTimeouts: -1}, "example.com", "")
	require.Equal(t, defaultMaxConsecutiveTimeouts, tr.abortAfter())
}

func TestHopsAbortedEarly(t *testing.T) {
	silent := func(n int) []HopResult {
		out := make([]HopResult, 0, n)
		for i := 1; i <= n; i++ {
			out = append(out, HopResult{Index: i, Address: unansweredHopAddress, TimedOut: true})
		}
		return out
	}

	t.Run("trailing run at the bound aborts", func(t *testing.T) {
		require.True(t, hopsAbortedEarly(silent(5), 30, 5))
	})
	t.Run("shorter run does not", func(t *testing.T) {
		require.False(t, hopsAbortedEarly(silent(4), 30, 5))
	})
	t.Run("abort disabled never reports early", func(t *testing.T) {
		require.False(t, hopsAbortedEarly(silent(20), 30, 0))
	})
	t.Run("reaching max_hops is not an early abort", func(t *testing.T) {
		require.False(t, hopsAbortedEarly(silent(30), 30, 5))
	})
	t.Run("answered tail is not an abort", func(t *testing.T) {
		hops := append(silent(6), HopResult{Index: 7, Address: "1.2.3.4"})
		require.False(t, hopsAbortedEarly(hops, 30, 5))
	})
}

// quotedProbe builds what an ICMP error quotes back: the probe's IPv4 header
// and the first 8 bytes of its payload.
func quotedProbe(t *testing.T, dst net.IP, proto int, first8 []byte) []byte {
	t.Helper()
	h := ipv4.Header{Version: 4, Len: ipv4.HeaderLen, TotalLen: ipv4.HeaderLen + 8, TTL: 1, Protocol: proto, Src: net.IPv4(10, 0, 0, 1), Dst: dst}
	b, err := h.Marshal()
	require.NoError(t, err)
	return append(b, first8...)
}

func TestMatchesProbeRequiresDestination(t *testing.T) {
	dest, other := net.IPv4(192, 0, 2, 10), net.IPv4(192, 0, 2, 20)

	// UDP: source port 40000 -> 33434.
	udpQuote := []byte{0x9c, 0x40, 0x82, 0x9a, 0, 0, 0, 0}
	udpKey := probeKey{dst: dest, udp: true, srcPort: 40000, dstPort: traceroutePort}
	require.True(t, matchesProbe(quotedProbe(t, dest, 17, udpQuote), udpKey))
	require.False(t, matchesProbe(quotedProbe(t, other, 17, udpQuote), udpKey),
		"same ports toward another destination belong to another trace")
	require.False(t, matchesProbe(quotedProbe(t, dest, 17, udpQuote), probeKey{dst: dest, udp: true, srcPort: 40001, dstPort: traceroutePort}))

	// ICMP echo: type 8, code 0, checksum, id 0x1234, seq 7.
	echoQuote := []byte{8, 0, 0, 0, 0x12, 0x34, 0, 7}
	echoKey := probeKey{dst: dest, echoID: 0x1234, echoSeq: 7}
	require.True(t, matchesProbe(quotedProbe(t, dest, 1, echoQuote), echoKey))
	require.False(t, matchesProbe(quotedProbe(t, other, 1, echoQuote), echoKey),
		"same echo id and seq toward another destination belong to another trace")
	require.False(t, matchesProbe(quotedProbe(t, dest, 1, echoQuote), probeKey{dst: dest, echoID: 0x1234, echoSeq: 8}))

	require.False(t, matchesProbe(nil, echoKey))
	require.False(t, matchesProbe(quotedProbe(t, dest, 1, echoQuote)[:ipv4.HeaderLen+4], echoKey), "short quote")
}

func TestEchoIDIsRandomPerTracerouter(t *testing.T) {
	// Four draws from 65536 values all colliding has odds of 1 in 2^48.
	seen := map[uint16]bool{}
	for range 4 {
		seen[newTracerouter(&TracerouteConfig{}, "example.com", "").echoID] = true
	}
	require.Greater(t, len(seen), 1, "tracerouters must not share an echo ID")
}

// fakeTraceDNS serves A and AAAA answers from records over UDP on loopback and
// returns its address. A name with no record of the asked type gets an empty
// NOERROR answer.
func fakeTraceDNS(t *testing.T, records map[string][]net.IP) string {
	t.Helper()
	pc, err := net.ListenPacket("udp4", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = pc.Close() })

	go func() {
		buf := make([]byte, 512)
		for {
			n, peer, err := pc.ReadFrom(buf)
			if err != nil {
				return
			}
			var p dnsmessage.Parser
			hdr, err := p.Start(buf[:n])
			if err != nil {
				continue
			}
			q, err := p.Question()
			if err != nil {
				continue
			}
			b := dnsmessage.NewBuilder(nil, dnsmessage.Header{ID: hdr.ID, Response: true, Authoritative: true})
			_ = b.StartQuestions()
			_ = b.Question(q)
			_ = b.StartAnswers()
			rh := dnsmessage.ResourceHeader{Name: q.Name, Class: dnsmessage.ClassINET, TTL: 60}
			for _, ip := range records[q.Name.String()] {
				if v4 := ip.To4(); v4 != nil && q.Type == dnsmessage.TypeA {
					_ = b.AResource(rh, dnsmessage.AResource{A: [4]byte(v4)})
				} else if v4 == nil && q.Type == dnsmessage.TypeAAAA {
					_ = b.AAAAResource(rh, dnsmessage.AAAAResource{AAAA: [16]byte(ip.To16())})
				}
			}
			msg, err := b.Finish()
			if err != nil {
				continue
			}
			_, _ = pc.WriteTo(msg, peer)
		}
	}()
	return pc.LocalAddr().String()
}

func TestResolveIPv4(t *testing.T) {
	server := fakeTraceDNS(t, map[string][]net.IP{
		"dual.test.":   {net.ParseIP("2001:db8::1"), net.IPv4(192, 0, 2, 7)},
		"v6only.test.": {net.ParseIP("2001:db8::2")},
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resolve := func(host string) (string, error) {
		return newTracerouter(&TracerouteConfig{}, host, server).resolveIPv4(ctx)
	}

	t.Run("uses the target's DNS server and picks the A record", func(t *testing.T) {
		ip, err := resolve("dual.test")
		require.NoError(t, err)
		require.Equal(t, "192.0.2.7", ip)
	})
	t.Run("IPv6-only name is an explicit IPv4-only error", func(t *testing.T) {
		_, err := resolve("v6only.test")
		require.EqualError(t, err, "traceroute supports IPv4 destinations only: v6only.test has no A record")
	})
	t.Run("IPv6 literal is an explicit IPv4-only error", func(t *testing.T) {
		_, err := resolve("2001:db8::3")
		require.EqualError(t, err, "traceroute supports IPv4 destinations only: 2001:db8::3 has no A record")
	})
	t.Run("missing name stays a resolution error", func(t *testing.T) {
		_, err := resolve("missing.test")
		require.ErrorContains(t, err, "resolving missing.test")
		var dnsErr *net.DNSError
		require.True(t, errors.As(err, &dnsErr) && dnsErr.IsNotFound, "got %v", err)
		require.Equal(t, server, dnsErr.Server, "the error names the server that was asked")
	})
	t.Run("IPv4 literal needs no DNS", func(t *testing.T) {
		ip, err := newTracerouter(&TracerouteConfig{}, "192.0.2.9", "").resolveIPv4(ctx)
		require.NoError(t, err)
		require.Equal(t, "192.0.2.9", ip)
	})
}

func TestMaxHopsAndHopTimeoutClamp(t *testing.T) {
	for _, tc := range []struct{ cfg, want int }{{0, defaultMaxHops}, {-1, defaultMaxHops}, {12, 12}, {255, 255}, {256, maxTTL}, {10000, maxTTL}} {
		require.Equal(t, tc.want, newTracerouter(&TracerouteConfig{MaxHops: tc.cfg}, "h", "").maxHops(), tc.cfg)
	}
	for _, tc := range []struct{ cfg, want time.Duration }{{0, defaultHopTimeout}, {-time.Second, defaultHopTimeout}, {time.Second, time.Second}} {
		require.Equal(t, tc.want, newTracerouter(&TracerouteConfig{Timeout: tc.cfg}, "h", "").hopTimeout(), tc.cfg)
	}
}

func TestHopDeadlineStopsAtCtxDeadline(t *testing.T) {
	tr := newTracerouter(&TracerouteConfig{Timeout: time.Hour}, "h", "")
	require.WithinDuration(t, time.Now().Add(time.Hour), tr.hopDeadline(context.Background()), time.Minute)

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	ctxDeadline, _ := ctx.Deadline()
	require.Equal(t, ctxDeadline, tr.hopDeadline(ctx))
}

// scriptedProbe answers probes from a per-TTL script of attempt outcomes: an
// address answers, "" stays silent. TTLs past the script stay silent.
func scriptedProbe(script map[int][]string, dest string) (probeFunc, *[]int) {
	var sent []int
	attempt := map[int]int{}
	return func(ttl int, _ time.Time) (string, bool, time.Duration, error) {
		sent = append(sent, ttl)
		answers := script[ttl]
		i := attempt[ttl]
		attempt[ttl]++
		if i >= len(answers) || answers[i] == "" {
			return "", false, 0, nil
		}
		return answers[i], answers[i] == dest, time.Millisecond, nil
	}, &sent
}

func TestWalkRetriesOnlySilentHops(t *testing.T) {
	const dest = "192.0.2.1"
	probe, _ := scriptedProbe(map[int][]string{
		1: {"10.0.0.1"},
		2: {"", "", "10.0.0.2"},
		3: {dest},
	}, dest)
	tr := newTracerouter(&TracerouteConfig{ProbesPerHop: 3, MaxConsecutiveTimeouts: 5}, dest, "")

	res, err := tr.walk(context.Background(), dest, probe)
	require.NoError(t, err)
	require.True(t, res.Reached)
	require.False(t, res.AbortedEarly)
	require.Equal(t, []HopResult{
		{Index: 1, Address: "10.0.0.1", RTT: time.Millisecond, Probes: 1},
		{Index: 2, Address: "10.0.0.2", RTT: time.Millisecond, Probes: 3},
		{Index: 3, Address: dest, RTT: time.Millisecond, Probes: 1},
	}, res.Hops)
}

func TestWalkAbortsAfterConsecutiveTimeouts(t *testing.T) {
	probe, sent := scriptedProbe(map[int][]string{1: {"10.0.0.1"}}, "192.0.2.1")
	tr := newTracerouter(&TracerouteConfig{ProbesPerHop: 2, MaxConsecutiveTimeouts: 3, MaxHops: 30}, "192.0.2.1", "")

	res, err := tr.walk(context.Background(), "192.0.2.1", probe)
	require.NoError(t, err)
	require.True(t, res.AbortedEarly)
	require.False(t, res.Reached)
	require.Len(t, res.Hops, 4, "one answered hop, then the 3 silent hops that trigger the abort")
	require.Len(t, *sent, 1+3*2, "each silent hop is retried probes_per_hop times")
}

func TestWalkStopsAtClampedMaxHops(t *testing.T) {
	// Every hop answers but never as the destination, and the abort is off:
	// only the TTL ceiling ends the walk.
	tr := newTracerouter(&TracerouteConfig{MaxHops: 1000, MaxConsecutiveTimeouts: 0}, "192.0.2.1", "")
	res, err := tr.walk(context.Background(), "192.0.2.1", func(int, time.Time) (string, bool, time.Duration, error) {
		return "10.0.0.1", false, 0, nil
	})
	require.NoError(t, err)
	require.Len(t, res.Hops, maxTTL)
}

func TestWalkCancelDoesNotRecordBogusHop(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr := newTracerouter(&TracerouteConfig{ProbesPerHop: 3, MaxConsecutiveTimeouts: 5}, "192.0.2.1", "")

	res, err := tr.walk(ctx, "192.0.2.1", func(ttl int, _ time.Time) (string, bool, time.Duration, error) {
		if ttl == 1 {
			return "10.0.0.1", false, time.Millisecond, nil
		}
		cancel() // the cycle ends while hop 2 is being probed
		return "", false, 0, nil
	})
	require.NoError(t, err)
	require.Equal(t, []HopResult{{Index: 1, Address: "10.0.0.1", RTT: time.Millisecond, Probes: 1}}, res.Hops,
		"hop 2 was cut short by cancellation, not observed as silent")
	require.False(t, res.AbortedEarly)
}

func TestWalkKeepsAnswerThatArrivesAsCtxEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	tr := newTracerouter(&TracerouteConfig{}, "192.0.2.1", "")

	res, err := tr.walk(ctx, "192.0.2.1", func(int, time.Time) (string, bool, time.Duration, error) {
		cancel()
		return "10.0.0.1", false, time.Millisecond, nil
	})
	require.NoError(t, err)
	require.Len(t, res.Hops, 1, "an answer that arrived is a real hop even if ctx ended meanwhile")
}

func TestWalkStopsOnProbeError(t *testing.T) {
	tr := newTracerouter(&TracerouteConfig{}, "192.0.2.1", "")
	boom := errors.New("sendto: no buffer space")
	res, err := tr.walk(context.Background(), "192.0.2.1", func(ttl int, _ time.Time) (string, bool, time.Duration, error) {
		if ttl == 2 {
			return "", false, 0, boom
		}
		return "10.0.0.1", false, 0, nil
	})
	require.ErrorIs(t, err, boom)
	require.Len(t, res.Hops, 1, "hops gathered before the failure are kept")
}

func TestWalkStopsAtUnreachable(t *testing.T) {
	tr := newTracerouter(defaultTracerouteConfig(), "h", "")
	probe := func(ttl int, _ time.Time) (string, bool, time.Duration, error) {
		switch ttl {
		case 1:
			return "10.0.0.1", false, time.Millisecond, nil
		case 2:
			// A router says the network is unreachable: the path ends here.
			return "10.0.0.2", false, 2 * time.Millisecond, errUnreachable
		}
		t.Fatalf("probe sent past the unreachable hop: ttl %d", ttl)
		return "", false, 0, nil
	}
	res, err := tr.walk(context.Background(), "203.0.113.9", probe)
	require.NoError(t, err)
	require.True(t, res.Unreachable)
	require.False(t, res.Reached)
	require.Len(t, res.Hops, 2)
	require.Equal(t, "10.0.0.2", res.Hops[1].Address)
	require.False(t, res.Hops[1].TimedOut)
}

// A router that cannot forward the probe answers destination unreachable,
// which ends the path but is not the destination answering, even when a
// probe mechanism reports it as "reached".
func TestWalkRouterUnreachableIsNotReached(t *testing.T) {
	tr := newTracerouter(defaultTracerouteConfig(), "192.0.2.1", "")
	res, err := tr.walk(context.Background(), "192.0.2.1", func(int, time.Time) (string, bool, time.Duration, error) {
		return "10.0.0.1", true, time.Millisecond, nil
	})
	require.NoError(t, err)
	require.False(t, res.Reached)
	require.Len(t, res.Hops, 1, "the unreachable ends the walk")
}
