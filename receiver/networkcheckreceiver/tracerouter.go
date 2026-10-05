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
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"net"
	"net/url"
	"strings"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

// unansweredHopAddress is the address reported for a hop that did not answer
// within the probe timeout.
const unansweredHopAddress = "*"

// defaultMaxConsecutiveTimeouts bounds how many unanswered hops in a row we
// tolerate before abandoning the trace. Without this a path that never answers
// (a firewall silently dropping probes, for example) walks the full max_hops
// range at the per-hop timeout, which on the defaults is 30 * 3s = 90s inside
// a single scrape. Overridable via traceroute.max_consecutive_timeouts.
const defaultMaxConsecutiveTimeouts = 5

// defaultProbesPerHop matches the convention every traceroute implementation
// follows. Routers rate-limit ICMP time-exceeded generation, so one probe per
// hop regularly misses a router that is answering perfectly well.
const defaultProbesPerHop = 3

// probesPerHop is the configured maximum probes for one hop, clamped to at
// least 1.
func (t *tracerouter) probesPerHop() int {
	if t.cfg.ProbesPerHop <= 0 {
		return defaultProbesPerHop
	}
	return t.cfg.ProbesPerHop
}

// abortAfter is the configured run of silent hops that abandons the trace, or 0
// when the early abort is disabled and max_hops is the only bound.
func (t *tracerouter) abortAfter() int {
	if t.cfg.MaxConsecutiveTimeouts < 0 {
		return defaultMaxConsecutiveTimeouts
	}
	return t.cfg.MaxConsecutiveTimeouts
}

// failureTraceEvery is how many consecutive failing checks pass between
// on_failure traces when no interval is configured. Without a limit a target
// that stays down is traced on every check, and a trace into a dead path is the
// most expensive thing the receiver does: it walks silent hops at the full hop
// timeout until the early abort.
// Deliberate simplification: a fixed cadence; make it configurable if one
// value does not fit every deployment.
const failureTraceEvery = 10

// icmpProtocolIPv4 is the IANA protocol number for ICMP, required by
// icmp.ParseMessage to interpret an IPv4 ICMP message.
const icmpProtocolIPv4 = 1

// errProbeTimeout is returned when no reply matching the probe arrived before
// the hop deadline.
var errProbeTimeout = errors.New("no matching ICMP reply before deadline")

// probeKey identifies the probe that a reply must correspond to. A raw ICMP
// socket receives every ICMP packet the host sees, so a reply has to be matched
// back to the probe that provoked it rather than assumed to belong to whichever
// TTL is currently in flight.
type probeKey struct {
	// dst is the trace destination. Every probe of every trace in the process
	// is seen by every raw socket, so the destination is part of the identity.
	dst net.IP

	// udp is true when the probe was a UDP datagram, false for an ICMP echo.
	udp bool

	srcPort int
	dstPort int

	echoID  int
	echoSeq int
}

// matchesProbe reports whether the original datagram quoted inside an ICMP
// error refers to the probe described by k. ICMP errors echo back the offending
// IP header plus at least its first 8 payload bytes, which is enough to recover
// the destination and the UDP port pair or the ICMP echo identifier.
func matchesProbe(inner []byte, k probeKey) bool {
	hdr, err := ipv4.ParseHeader(inner)
	if err != nil || hdr.Len <= 0 || len(inner) < hdr.Len+8 || !hdr.Dst.Equal(k.dst) {
		return false
	}
	payload := inner[hdr.Len:]
	if k.udp {
		src := int(binary.BigEndian.Uint16(payload[0:2]))
		dst := int(binary.BigEndian.Uint16(payload[2:4]))
		return src == k.srcPort && dst == k.dstPort
	}
	// Quoted ICMP echo header: type, code, checksum, id, seq.
	id := int(binary.BigEndian.Uint16(payload[4:6]))
	seq := int(binary.BigEndian.Uint16(payload[6:8]))
	return id == k.echoID && seq == k.echoSeq
}

// awaitProbeReply reads from conn until a message matching k arrives or the
// deadline passes. Unrelated ICMP traffic - echo replies belonging to this
// receiver's own ping, late replies to earlier TTLs, other processes' ICMP - is
// discarded instead of being attributed to the current hop. reachedDest is true
// when the reply shows the probe arrived at the target rather than expiring in
// transit.
func awaitProbeReply(conn *icmp.PacketConn, deadline time.Time, k probeKey) (from net.Addr, reachedDest bool, err error) {
	buf := make([]byte, 1500)
	for {
		if time.Now().After(deadline) {
			return nil, false, errProbeTimeout
		}
		if err := conn.SetReadDeadline(deadline); err != nil {
			return nil, false, err
		}
		n, peer, readErr := conn.ReadFrom(buf)
		if readErr != nil {
			return nil, false, readErr
		}
		if peer == nil {
			continue
		}
		msg, parseErr := icmp.ParseMessage(icmpProtocolIPv4, buf[:n])
		if parseErr != nil {
			continue
		}
		switch body := msg.Body.(type) {
		case *icmp.TimeExceeded:
			if matchesProbe(body.Data, k) {
				return peer, false, nil
			}
		case *icmp.DstUnreach:
			// The target answering our UDP probe on a closed port means the
			// probe arrived: the path is complete.
			if matchesProbe(body.Data, k) {
				return peer, true, nil
			}
		case *icmp.Echo:
			if ipa, ok := peer.(*net.IPAddr); ok && !k.udp && msg.Type == ipv4.ICMPTypeEchoReply &&
				body.ID == k.echoID && body.Seq == k.echoSeq && ipa.IP.Equal(k.dst) {
				return peer, true, nil
			}
		}
	}
}

// HopResult is the latency measurement for a single traceroute hop.
type HopResult struct {
	Index   int
	Address string
	RTT     time.Duration

	// TimedOut is true when the hop did not answer within the timeout. RTT is
	// meaningless for such a hop (it only reflects how long we waited), so
	// callers must not report it as a latency.
	TimedOut bool

	// Probes is how many probes were sent for this hop. Probing stops at the
	// first reply, so a value above 1 means earlier probes went unanswered —
	// which distinguishes a hop that is merely rate-limiting from one that is
	// consistently silent.
	Probes int
}

// TraceResult is the outcome of a single traceroute run. It carries the path
// itself plus the context needed to tell an incomplete path from a complete
// one: whether the destination actually answered, and whether the trace gave up
// early after a run of silent hops. Metrics only consume Hops; the remaining
// fields exist because a log record describes the trace as a whole.
type TraceResult struct {
	Hops []HopResult

	// DestIP is the address the hostname resolved to for this run. A hostname
	// with several A records can resolve differently between runs.
	DestIP string

	// Method is the probe mechanism actually used: "udp", "icmp", or "native".
	Method string

	// MaxHops is the TTL ceiling this run was bounded by.
	MaxHops int

	// Reached is true when the destination itself answered.
	Reached bool

	// AbortedEarly is true when the trace stopped after maxConsecutiveTimeouts
	// silent hops rather than reaching the destination or the TTL ceiling.
	// Without this, a truncated path is indistinguishable from a short one.
	AbortedEarly bool
}

// tracerouter performs traceroute probes for a single host.
type tracerouter struct {
	cfg  TracerouteConfig
	host string

	// resolver resolves host the way the target's probe does: through the
	// target's DNS server when one is set, the system resolver otherwise.
	resolver *net.Resolver

	// echoID identifies this tracerouter's ICMP echo probes. Each tracerouter
	// picks its own, so concurrent traces in one process do not claim each
	// other's replies.
	echoID uint16

	// failStreak counts consecutive checks that met the on_failure condition.
	// A tracerouter belongs to one target, which is probed by one goroutine at
	// a time, so it needs no locking.
	failStreak int
}

func newTracerouter(cfg TracerouteConfig, endpoint string, dnsServer string) *tracerouter {
	return &tracerouter{
		cfg:      cfg,
		host:     hostFromEndpoint(endpoint),
		resolver: newResolver(dnsServer),
		// #nosec G404 G115 -- correlates probes with their replies, not a secret; the value is in [0, 65535].
		echoID: uint16(rand.IntN(1 << 16)),
	}
}

// newResolver returns a resolver that queries dnsServer, or the system
// resolver when dnsServer is empty.
func newResolver(dnsServer string) *net.Resolver {
	if dnsServer == "" {
		return net.DefaultResolver
	}
	addr := dnsServerAddr(dnsServer)
	return &net.Resolver{
		PreferGo: true,
		// network is "udp", or "tcp" to retry a truncated answer.
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	}
}

// dnsServerAddr turns a configured DNS server into a dialable host:port. The
// port defaults to 53, and an IPv6 address may come with or without brackets.
func dnsServerAddr(server string) string {
	if _, _, err := net.SplitHostPort(server); err == nil {
		return server
	}
	return net.JoinHostPort(strings.Trim(server, "[]"), "53")
}

// hostFromEndpoint extracts the bare host from an endpoint that may be a full
// URL (e.g. "https://example.com/path") or a plain host/IP with or without a
// port. IPv6 literals come back without brackets, ready for a lookup, and
// userinfo is dropped even without a scheme, so a credential never becomes the
// host.
func hostFromEndpoint(endpoint string) string {
	if u, err := url.Parse(endpoint); err == nil && u.Host != "" {
		return u.Hostname()
	}
	if i := strings.LastIndex(endpoint, "@"); i >= 0 {
		endpoint = endpoint[i+1:]
	}
	if h, _, err := net.SplitHostPort(endpoint); err == nil {
		return h
	}
	return strings.Trim(endpoint, "[]")
}

// shouldRun returns true if a traceroute should be performed given the current
// check count and the most recent ping result for the target.
//
// interval traces every Nth check. on_failure additionally traces the first
// failing check of a streak; while the target keeps failing it traces again
// only every failureTraceEvery failing checks, and only when no interval is
// set, since the interval schedule already re-traces a target that stays
// down. A passing check ends the streak.
func (t *tracerouter) shouldRun(checkCount int, result PingResult) bool {
	if !t.cfg.Enabled {
		return false
	}
	run := t.cfg.Interval > 0 && checkCount%t.cfg.Interval == 0

	failing := t.cfg.OnFailure && result.Method == MethodICMP && result.PacketLoss >= t.cfg.FailureThreshold
	if !failing {
		t.failStreak = 0
		return run
	}
	t.failStreak++
	if t.failStreak == 1 {
		return true
	}
	return run || (t.cfg.Interval <= 0 && (t.failStreak-1)%failureTraceEvery == 0)
}

// resolveIPv4 resolves the host to the IPv4 address the trace probes.
// Deliberate simplification: IPv4 only. IPv6 traceroute needs ICMPv6 probes
// and IPV6_RECVERR on Linux; add it when IPv6-only targets need a path.
func (t *tracerouter) resolveIPv4(ctx context.Context) (string, error) {
	ips, err := t.resolver.LookupIP(ctx, "ip4", t.host)
	if err == nil && len(ips) > 0 {
		return ips[0].String(), nil
	}
	host := redactEndpoint(t.host)
	errIPv4Only := fmt.Errorf("traceroute supports IPv4 destinations only: %s has no A record", host)

	var addrErr *net.AddrError
	var dnsErr *net.DNSError
	switch {
	case err == nil, errors.As(err, &addrErr):
		// An IPv6 literal.
		return "", errIPv4Only
	case errors.As(err, &dnsErr) && dnsErr.IsNotFound:
		// A name that exists only as IPv6 is not "not found"; say what is
		// actually wrong.
		if v6, _ := t.resolver.LookupIP(ctx, "ip6", t.host); len(v6) > 0 {
			return "", errIPv4Only
		}
	}
	return "", fmt.Errorf("resolving %s: %w", host, err)
}

// trace performs a traceroute to t.host and returns per-hop results.
// It uses UDP probes by default (method "udp") or ICMP echo probes (method "icmp").
// UDP traceroute does not require root on most Linux kernels.
// ICMP traceroute requires root / CAP_NET_RAW.
func (t *tracerouter) trace(ctx context.Context) (TraceResult, error) {
	method := strings.ToLower(t.cfg.Method)
	if method == "" {
		method = "udp"
	}
	maxHops := t.cfg.MaxHops
	if maxHops <= 0 {
		maxHops = 30
	}

	dest, err := t.resolveIPv4(ctx)
	if err != nil {
		return TraceResult{Method: method, MaxHops: maxHops}, err
	}

	// Some platforms cannot map a path with raw sockets and need a native API
	// instead. Windows is the case that matters today: it does not deliver
	// unsolicited inbound ICMP time-exceeded messages to a raw socket, so both
	// the UDP and ICMP methods below time out on every hop there regardless of
	// privileges. traceNative reports handled=false everywhere else.
	if hops, handled, nativeErr := t.traceNative(ctx, dest); handled {
		return TraceResult{
			Hops:         hops,
			DestIP:       dest,
			Method:       "native",
			MaxHops:      maxHops,
			Reached:      hopsReachedDest(hops, dest),
			AbortedEarly: hopsAbortedEarly(hops, maxHops, t.abortAfter()),
		}, nativeErr
	}

	var res TraceResult
	var traceErr error
	switch method {
	case "icmp":
		res, traceErr = t.traceICMP(ctx, dest)
	default:
		res, traceErr = t.traceUDP(ctx, dest)
	}
	res.DestIP = dest
	res.Method = method
	res.MaxHops = maxHops
	return res, traceErr
}

// hopsReachedDest reports whether the last answering hop was the destination.
// Used for the native path, which reports hops without saying how it finished.
func hopsReachedDest(hops []HopResult, dest string) bool {
	for i := len(hops) - 1; i >= 0; i-- {
		if !hops[i].TimedOut {
			return hops[i].Address == dest
		}
	}
	return false
}

// hopsAbortedEarly reports whether a path ends in the run of silent hops that
// triggers the maxConsecutiveTimeouts bail-out, rather than ending because the
// destination answered or the TTL ceiling was hit.
func hopsAbortedEarly(hops []HopResult, maxHops, abortAfter int) bool {
	if abortAfter <= 0 || len(hops) == 0 || len(hops) >= maxHops {
		return false
	}
	trailing := 0
	for i := len(hops) - 1; i >= 0 && hops[i].TimedOut; i-- {
		trailing++
	}
	return trailing >= abortAfter
}

// traceUDP sends UDP packets with incrementing TTL and listens for ICMP
// time-exceeded responses to map the path.
func (t *tracerouter) traceUDP(ctx context.Context, dest string) (TraceResult, error) {
	destAddr, err := net.ResolveUDPAddr("udp4", net.JoinHostPort(dest, "33434"))
	if err != nil {
		return TraceResult{}, fmt.Errorf("resolving UDP dest: %w", err)
	}

	// Open raw ICMP socket to receive time-exceeded responses.
	icmpConn, err := icmp.ListenPacket("ip4:icmp", "0.0.0.0")
	if err != nil {
		return TraceResult{}, fmt.Errorf("opening ICMP listener for traceroute: %w", err)
	}
	defer func() { _ = icmpConn.Close() }()

	var hops []HopResult
	maxHops := t.cfg.MaxHops
	if maxHops <= 0 {
		maxHops = 30
	}
	consecutiveTimeouts := 0
	var reached, aborted bool

	for ttl := 1; ttl <= maxHops; ttl++ {
		if ctx.Err() != nil {
			break
		}

		hopTimeout := t.cfg.Timeout
		if hopTimeout == 0 {
			hopTimeout = 3 * time.Second
		}

		// Probe until this hop answers or the attempts are exhausted. A hop
		// that replies costs a single probe; only a silent one is retried, so
		// a healthy path generates no more traffic than a single-probe trace.
		var (
			from        net.Addr
			reachedDest bool
			rtt         time.Duration
			probes      int
		)
		for attempt := 0; attempt < t.probesPerHop(); attempt++ {
			if ctx.Err() != nil {
				break
			}
			probes++

			// Send a UDP packet with the given TTL.
			udpConn, dialErr := net.DialUDP("udp4", nil, destAddr)
			if dialErr != nil {
				return TraceResult{Hops: hops, Reached: reached, AbortedEarly: aborted}, fmt.Errorf("dialing UDP: %w", dialErr)
			}
			ipConn := ipv4.NewConn(udpConn)
			if ttlErr := ipConn.SetTTL(ttl); ttlErr != nil {
				_ = udpConn.Close()
				return TraceResult{Hops: hops, Reached: reached, AbortedEarly: aborted}, fmt.Errorf("setting TTL %d: %w", ttl, ttlErr)
			}

			// The source port identifies this probe in the ICMP error that
			// comes back, so it must be read before the socket is closed. It
			// changes per attempt, which is what keeps a late reply to an
			// earlier attempt from being matched here.
			localPort := 0
			if la, ok := udpConn.LocalAddr().(*net.UDPAddr); ok {
				localPort = la.Port
			}

			sent := time.Now()
			if _, writeErr := udpConn.Write([]byte("ping")); writeErr != nil {
				_ = udpConn.Close()
				return TraceResult{Hops: hops, Reached: reached, AbortedEarly: aborted}, fmt.Errorf("sending UDP probe: %w", writeErr)
			}
			_ = udpConn.Close()

			var awaitErr error
			from, reachedDest, awaitErr = awaitProbeReply(icmpConn, time.Now().Add(hopTimeout), probeKey{
				dst:     destAddr.IP,
				udp:     true,
				srcPort: localPort,
				dstPort: destAddr.Port,
			})
			rtt = time.Since(sent)

			if awaitErr == nil && from != nil {
				break
			}
			from = nil
		}

		if from == nil {
			hops = append(hops, HopResult{
				Index:    ttl,
				Address:  unansweredHopAddress,
				TimedOut: true,
				Probes:   probes,
			})
			consecutiveTimeouts++
			if abort := t.abortAfter(); abort > 0 && consecutiveTimeouts >= abort {
				aborted = true
				break
			}
			continue
		}
		consecutiveTimeouts = 0

		hops = append(hops, HopResult{
			Index:   ttl,
			Address: from.String(),
			RTT:     rtt,
			Probes:  probes,
		})

		// Stop when we reach the destination.
		fromHost, _, splitErr := net.SplitHostPort(from.String())
		if splitErr != nil {
			fromHost = from.String()
		}
		if reachedDest || fromHost == dest {
			reached = true
			break
		}
	}

	return TraceResult{Hops: hops, Reached: reached, AbortedEarly: aborted}, nil
}

// traceICMP sends ICMP echo requests with incrementing TTL values and collects
// ICMP time-exceeded responses. Requires root / CAP_NET_RAW.
func (t *tracerouter) traceICMP(ctx context.Context, dest string) (TraceResult, error) {
	conn, err := icmp.ListenPacket("ip4:icmp", "0.0.0.0")
	if err != nil {
		return TraceResult{}, fmt.Errorf("opening raw ICMP socket for traceroute: %w", err)
	}
	defer func() { _ = conn.Close() }()

	destAddr, err := net.ResolveIPAddr("ip4", dest)
	if err != nil {
		return TraceResult{}, fmt.Errorf("resolving ICMP dest: %w", err)
	}

	var hops []HopResult
	maxHops := t.cfg.MaxHops
	if maxHops <= 0 {
		maxHops = 30
	}
	hopTimeout := t.cfg.Timeout
	if hopTimeout == 0 {
		hopTimeout = 3 * time.Second
	}
	consecutiveTimeouts := 0
	var reached, aborted bool
	seq := 0

	for ttl := 1; ttl <= maxHops; ttl++ {
		if ctx.Err() != nil {
			break
		}

		// Probe until this hop answers or the attempts are exhausted. Only a
		// silent hop is retried, so a healthy path costs one probe per hop.
		var (
			from        net.Addr
			reachedDest bool
			rtt         time.Duration
			probes      int
		)
		for attempt := 0; attempt < t.probesPerHop(); attempt++ {
			if ctx.Err() != nil {
				break
			}
			probes++

			// The sequence number is unique per attempt so a late reply to an
			// earlier attempt cannot be matched against this one.
			seq++
			msg := icmp.Message{
				Type: ipv4.ICMPTypeEcho,
				Code: 0,
				Body: &icmp.Echo{ID: int(t.echoID), Seq: seq, Data: []byte("networkcheck")},
			}
			wb, marshalErr := msg.Marshal(nil)
			if marshalErr != nil {
				return TraceResult{Hops: hops, Reached: reached, AbortedEarly: aborted}, fmt.Errorf("marshaling ICMP echo: %w", marshalErr)
			}

			// Use the connection's own IPv4 accessor rather than
			// ipv4.NewPacketConn: that constructor type-asserts to net.Conn
			// without a comma-ok, and *icmp.PacketConn implements
			// net.PacketConn but not net.Conn, so passing one panics.
			p4 := conn.IPv4PacketConn()
			if p4 == nil {
				return TraceResult{Hops: hops, Reached: reached, AbortedEarly: aborted}, fmt.Errorf("ICMP traceroute requires an IPv4 connection")
			}
			if ttlErr := p4.SetTTL(ttl); ttlErr != nil {
				return TraceResult{Hops: hops, Reached: reached, AbortedEarly: aborted}, fmt.Errorf("setting ICMP TTL %d: %w", ttl, ttlErr)
			}

			sent := time.Now()
			if _, writeErr := conn.WriteTo(wb, destAddr); writeErr != nil {
				return TraceResult{Hops: hops, Reached: reached, AbortedEarly: aborted}, fmt.Errorf("sending ICMP probe: %w", writeErr)
			}

			var awaitErr error
			from, reachedDest, awaitErr = awaitProbeReply(conn, time.Now().Add(hopTimeout), probeKey{
				dst:     destAddr.IP,
				echoID:  int(t.echoID),
				echoSeq: seq,
			})
			rtt = time.Since(sent)

			if awaitErr == nil && from != nil {
				break
			}
			from = nil
		}

		if from == nil {
			hops = append(hops, HopResult{
				Index:    ttl,
				Address:  unansweredHopAddress,
				TimedOut: true,
				Probes:   probes,
			})
			consecutiveTimeouts++
			if abort := t.abortAfter(); abort > 0 && consecutiveTimeouts >= abort {
				aborted = true
				break
			}
			continue
		}
		consecutiveTimeouts = 0

		hops = append(hops, HopResult{
			Index:   ttl,
			Address: from.String(),
			RTT:     rtt,
			Probes:  probes,
		})

		if reachedDest || from.String() == destAddr.String() {
			reached = true
			break
		}
	}

	return TraceResult{Hops: hops, Reached: reached, AbortedEarly: aborted}, nil
}
