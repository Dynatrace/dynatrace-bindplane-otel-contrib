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
	"os"
	"strings"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

// unansweredHopAddress is the address reported for a hop that did not answer
// within the probe timeout.
const unansweredHopAddress = "*"

// Defaults for settings left at 0.
const (
	defaultMaxHops             = 30
	defaultHopTimeout          = 3 * time.Second
	defaultProbesPerHop        = 3
	defaultMaxConcurrentTraces = 4

	// defaultMaxConsecutiveTimeouts bounds how many unanswered hops in a row
	// are tolerated before the trace is abandoned. Without it a path that
	// stops answering walks the full max_hops range at the per-hop timeout.
	defaultMaxConsecutiveTimeouts = 5
)

// maxTTL is the largest TTL an IPv4 header can carry, so the deepest hop a
// trace can probe. Config validation rejects a larger max_hops; this clamp is
// the backstop.
const maxTTL = 255

// traceroutePort is the destination port of UDP probes, the conventional
// traceroute base port. Nothing normally listens there, so the destination
// answers with ICMP port unreachable, which is how arrival is detected.
const traceroutePort = 33434

// probePayload is the body of every probe.
var probePayload = []byte("traceroute")

// probesPerHop is the configured maximum probes for one hop, defaulted when
// unset.
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

// maxHops is the configured TTL ceiling, defaulted when unset and clamped to
// what an IPv4 header can carry.
func (t *tracerouter) maxHops() int {
	switch {
	case t.cfg.MaxHops <= 0:
		return defaultMaxHops
	case t.cfg.MaxHops > maxTTL:
		return maxTTL
	}
	return t.cfg.MaxHops
}

// hopTimeout is how long one probe waits for its answer.
func (t *tracerouter) hopTimeout() time.Duration {
	if t.cfg.Timeout <= 0 {
		return defaultHopTimeout
	}
	return t.cfg.Timeout
}

// hopDeadline is when a probe sent now stops waiting: after the hop timeout,
// or at ctx's deadline if that comes first, so a trace never outlives the
// cycle that started it.
func (t *tracerouter) hopDeadline(ctx context.Context) time.Time {
	d := time.Now().Add(t.hopTimeout())
	if cd, ok := ctx.Deadline(); ok && cd.Before(d) {
		return cd
	}
	return d
}

// ctxDone reports whether ctx is cancelled or past its deadline. The deadline
// is compared directly because a probe that waited until exactly that moment
// can return before ctx's own timer has fired.
func ctxDone(ctx context.Context) bool {
	if ctx.Err() != nil {
		return true
	}
	d, ok := ctx.Deadline()
	return ok && !time.Now().Before(d)
}

// icmpProtocolIPv4 is the IANA protocol number for ICMP, required by
// icmp.ParseMessage to interpret an IPv4 ICMP message.
const icmpProtocolIPv4 = 1

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
// deadline passes, returning the answering address, or "" when nothing
// matching arrived in time. Unrelated ICMP traffic - late replies to earlier
// TTLs, other traces, other processes' ICMP - is discarded instead of being
// attributed to the current hop. reachedDest is true when the reply shows the
// probe arrived at the target rather than expiring in transit.
//
// The caller cuts the read deadline short when ctx is cancelled.
func awaitProbeReply(ctx context.Context, conn *icmp.PacketConn, deadline time.Time, k probeKey) (from string, reachedDest bool, err error) {
	if err := conn.SetReadDeadline(deadline); err != nil {
		return "", false, err
	}
	// Checked after arming the deadline: a cancellation that landed earlier
	// already cut the deadline short, and the line above just undid that.
	if ctx.Err() != nil {
		return "", false, nil
	}
	buf := make([]byte, 1500)
	for {
		n, peer, readErr := conn.ReadFrom(buf)
		if errors.Is(readErr, os.ErrDeadlineExceeded) {
			return "", false, nil
		}
		if readErr != nil {
			return "", false, fmt.Errorf("reading ICMP reply: %w", readErr)
		}
		peerAddr, ok := peer.(*net.IPAddr)
		if !ok {
			continue
		}
		if from, reached, matched, err := classifyReply(buf[:n], peerAddr, k); matched {
			return from, reached, err
		}
	}
}

// classifyReply decides what one ICMP message, b as read from the raw socket
// and sent by peer, means for the probe k. matched is false when the message
// is not an answer to k and must be skipped. Otherwise from is the answering
// address, reached is true when the probe arrived at the destination, and err
// is errUnreachable when the path ends at peer short of it.
func classifyReply(b []byte, peer *net.IPAddr, k probeKey) (from string, reached, matched bool, err error) {
	msg, err := icmp.ParseMessage(icmpProtocolIPv4, b)
	if err != nil {
		return "", false, false, nil
	}
	switch body := msg.Body.(type) {
	case *icmp.TimeExceeded:
		if matchesProbe(body.Data, k) {
			return peer.String(), false, true, nil
		}
	case *icmp.DstUnreach:
		if !matchesProbe(body.Data, k) {
			return "", false, false, nil
		}
		// Only the destination answering a UDP probe with port unreachable
		// (code 3) means the probe arrived. Any other unreachable, or one
		// sent by a router or by this host for an address with no route,
		// ends the path short of the destination.
		if k.udp && msg.Code == 3 && peer.IP.Equal(k.dst) {
			return peer.String(), true, true, nil
		}
		return peer.String(), false, true, errUnreachable
	case *icmp.Echo:
		if !k.udp && msg.Type == ipv4.ICMPTypeEchoReply &&
			body.ID == k.echoID && body.Seq == k.echoSeq && peer.IP.Equal(k.dst) {
			return peer.String(), true, true, nil
		}
	}
	return "", false, false, nil
}

// HopResult is the outcome of probing one hop.
type HopResult struct {
	Index   int
	Address string
	RTT     time.Duration

	// TimedOut is true when the hop did not answer within the timeout. RTT is
	// meaningless for such a hop (it only reflects how long we waited), so
	// callers must not report it as a latency.
	TimedOut bool

	// Probes is how many probes were sent for this hop. Probing stops at the
	// first reply, so a value above 1 means earlier probes went unanswered,
	// which distinguishes a hop that is rate-limiting from one that is silent.
	Probes int
}

// TraceResult is the outcome of one trace: the path plus what is needed to
// tell an incomplete path from a complete one.
type TraceResult struct {
	Hops []HopResult

	// DestIP is the address the host resolved to for this run. A hostname
	// with several A records can resolve differently between runs.
	DestIP string

	// Method is the probe mechanism actually used: "udp", "icmp", or "native".
	Method string

	// Reached is true when the destination itself answered.
	Reached bool

	// AbortedEarly is true when the trace stopped after max_consecutive_timeouts
	// silent hops rather than reaching the destination or the TTL ceiling.
	// Without this, a truncated path is indistinguishable from a short one.
	AbortedEarly bool

	// Unreachable is true when a hop answered with an ICMP destination
	// unreachable that does not mean "arrived": a router reporting the host
	// or network unreachable or administratively prohibited, or the local
	// host for an address with no route. The last hop is the one that said
	// so; the destination was not reached.
	Unreachable bool
}

// errUnreachable is returned by a probe whose answer was an ICMP destination
// unreachable other than the destination's own port unreachable: the path
// ends at the answering hop.
var errUnreachable = errors.New("destination unreachable")

// tracerouter traces one target. It is used by one goroutine at a time.
type tracerouter struct {
	cfg  *TracerouteConfig
	host string

	// dnsServer is the target's dns_server as configured, "" for the system
	// resolver. resolver resolves host through it.
	dnsServer string
	resolver  *net.Resolver

	// echoID identifies this tracerouter's ICMP echo probes. Each tracerouter
	// picks its own, so concurrent traces in one process do not claim each
	// other's replies.
	echoID uint16
}

func newTracerouter(cfg *TracerouteConfig, host, dnsServer string) *tracerouter {
	return &tracerouter{
		cfg:       cfg,
		host:      host,
		dnsServer: dnsServer,
		resolver:  newResolver(dnsServer),
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
// port defaults to 53, and an IPv6 address may come with or without brackets;
// splitting rather than looking for a colon keeps bare IPv6 addresses working.
func dnsServerAddr(server string) string {
	if _, _, err := net.SplitHostPort(server); err == nil {
		return server
	}
	return net.JoinHostPort(strings.Trim(server, "[]"), "53")
}

// resolveIPv4 resolves the host to the IPv4 address the trace probes.
// Deliberate simplification: IPv4 only. IPv6 traceroute needs ICMPv6 probes
// and IPV6_RECVERR on Linux; add it when IPv6-only targets need a path.
func (t *tracerouter) resolveIPv4(ctx context.Context) (string, error) {
	ips, err := t.resolver.LookupIP(ctx, "ip4", t.host)
	// The Go resolver names the resolv.conf server it believes it asked; the
	// Dial override sent the query to dns_server instead.
	var dnsErr *net.DNSError
	if t.dnsServer != "" && errors.As(err, &dnsErr) {
		dnsErr.Server = dnsServerAddr(t.dnsServer)
	}
	if err == nil && len(ips) > 0 {
		return ips[0].String(), nil
	}
	errIPv4Only := fmt.Errorf("traceroute supports IPv4 destinations only: %s has no A record", t.host)

	var addrErr *net.AddrError
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
	return "", fmt.Errorf("resolving %s: %w", t.host, err)
}

// trace maps the path to t.host. UDP probes are the default; method "icmp"
// sends ICMP echo requests instead. The mechanism that carries them, and the
// privilege it needs, is per platform: see tracePath in tracerouter_linux.go,
// tracerouter_windows.go and tracerouter_other.go.
func (t *tracerouter) trace(ctx context.Context) (TraceResult, error) {
	method := strings.ToLower(t.cfg.Method)
	if method == "" {
		method = "udp"
	}

	dest, err := t.resolveIPv4(ctx)
	if err != nil {
		return TraceResult{Method: method}, err
	}

	res, err := t.tracePath(ctx, method, dest)
	res.DestIP = dest
	if res.Method == "" {
		res.Method = method
	}
	return res, err
}

// probeFunc sends one probe with the given TTL and waits until deadline for
// the hop to answer. from is the answering address, "" when the hop stayed
// silent; reached is true when the answer shows the probe arrived at the
// destination. A non-nil error is a local failure that ends the trace.
type probeFunc func(ttl int, deadline time.Time) (from string, reached bool, rtt time.Duration, err error)

// walk maps the path to dest one TTL at a time. Every probe mechanism goes
// through it, so retries, the early abort and cancellation behave the same
// whichever one sends the packets.
func (t *tracerouter) walk(ctx context.Context, dest string, probe probeFunc) (TraceResult, error) {
	var res TraceResult
	consecutiveTimeouts := 0

	for ttl := 1; ttl <= t.maxHops(); ttl++ {
		if ctxDone(ctx) {
			break
		}

		// Probe until this hop answers or the attempts are exhausted. A hop
		// that replies costs a single probe; only a silent one is retried, so
		// a healthy path generates no more traffic than a single-probe trace.
		var (
			from    string
			reached bool
			rtt     time.Duration
			probes  int
		)
		for attempt := 0; attempt < t.probesPerHop() && !ctxDone(ctx); attempt++ {
			probes++
			var err error
			from, reached, rtt, err = probe(ttl, t.hopDeadline(ctx))
			if errors.Is(err, errUnreachable) && from != "" {
				res.Hops = append(res.Hops, HopResult{Index: ttl, Address: from, RTT: rtt, Probes: probes})
				res.Unreachable = true
				return res, nil
			}
			if err != nil {
				return res, err
			}
			if from != "" {
				break
			}
		}

		// A cancelled retry loop never gave a silent hop its full chance, so
		// stop rather than record a timeout that was never really observed.
		if from == "" && ctxDone(ctx) {
			break
		}

		if from == "" {
			res.Hops = append(res.Hops, HopResult{
				Index:    ttl,
				Address:  unansweredHopAddress,
				TimedOut: true,
				Probes:   probes,
			})
			consecutiveTimeouts++
			if abort := t.abortAfter(); abort > 0 && consecutiveTimeouts >= abort {
				res.AbortedEarly = true
				break
			}
			continue
		}
		consecutiveTimeouts = 0

		res.Hops = append(res.Hops, HopResult{
			Index:   ttl,
			Address: from,
			RTT:     rtt,
			Probes:  probes,
		})

		if reached || from == dest {
			// Reached only when the destination itself answered: a probe
			// mechanism that misreads a router's answer as "arrived" must not
			// mark the path complete.
			res.Reached = from == dest
			break
		}
	}

	return res, nil
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
// triggers the max_consecutive_timeouts bail-out, rather than ending because
// the destination answered or the TTL ceiling was hit.
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

// traceICMP maps the path with ICMP echo requests. The answers are read from a
// raw ICMP socket, so this needs root or CAP_NET_RAW on every platform.
func (t *tracerouter) traceICMP(ctx context.Context, dest string) (TraceResult, error) {
	conn, err := icmp.ListenPacket("ip4:icmp", "0.0.0.0")
	if err != nil {
		return TraceResult{}, fmt.Errorf("opening raw ICMP socket for traceroute: %w", err)
	}
	defer func() { _ = conn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = conn.SetReadDeadline(time.Now()) })
	defer stop()

	// Use the connection's own IPv4 accessor rather than ipv4.NewPacketConn:
	// that constructor type-asserts to net.Conn without a comma-ok, and
	// *icmp.PacketConn implements net.PacketConn but not net.Conn, so passing
	// one panics.
	p4 := conn.IPv4PacketConn()
	if p4 == nil {
		return TraceResult{}, errors.New("ICMP traceroute requires an IPv4 connection")
	}

	destAddr := &net.IPAddr{IP: net.ParseIP(dest)}
	var seq uint16
	return t.walk(ctx, dest, func(ttl int, deadline time.Time) (string, bool, time.Duration, error) {
		// The sequence number is unique per attempt so a late reply to an
		// earlier attempt cannot be matched against this one.
		seq++
		msg := icmp.Message{
			Type: ipv4.ICMPTypeEcho,
			Body: &icmp.Echo{ID: int(t.echoID), Seq: int(seq), Data: probePayload},
		}
		wb, err := msg.Marshal(nil)
		if err != nil {
			return "", false, 0, fmt.Errorf("marshaling ICMP echo: %w", err)
		}
		if err := p4.SetTTL(ttl); err != nil {
			return "", false, 0, fmt.Errorf("setting ICMP TTL %d: %w", ttl, err)
		}

		sent := time.Now()
		if _, err := conn.WriteTo(wb, destAddr); err != nil {
			return "", false, 0, fmt.Errorf("sending ICMP probe: %w", err)
		}
		from, reached, err := awaitProbeReply(ctx, conn, deadline, probeKey{
			dst:     destAddr.IP,
			echoID:  int(t.echoID),
			echoSeq: int(seq),
		})
		return from, reached, time.Since(sent), err
	})
}
