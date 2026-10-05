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
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	probing "github.com/prometheus-community/pro-bing"
	"golang.org/x/net/icmp"
)

// icmpInterval is the gap between echo requests within one probe. pro-bing
// defaults to 1s, which made a healthy 3-packet probe take 2s.
// Deliberate simplification: one fixed interval for every target; make it a
// per-target setting if a target needs different pacing.
const icmpInterval = 200 * time.Millisecond

// icmpPinger sends ICMP echo requests and collects RTT statistics.
type icmpPinger struct {
	host       string
	count      int
	timeout    time.Duration // per-packet reply budget, also bounds name resolution
	resolver   *net.Resolver
	dnsServer  string // host:port of the target's dns_server, "" for the system resolver
	privileged bool   // true = raw ICMP socket, false = datagram ICMP socket
}

func newICMPPinger(target TargetConfig, privileged bool) *icmpPinger {
	count := target.PingCount
	if count <= 0 {
		count = 3
	}
	timeout := target.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	resolver, server := net.DefaultResolver, ""
	if target.DNSServer != "" {
		// The target names its resolver; ask that server rather than the
		// system's, so dns.server describes the lookup that actually ran.
		server = dnsServerAddr(redactEndpoint(target.DNSServer))
		resolver = &net.Resolver{
			PreferGo: true,
			Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, network, server)
			},
		}
	}
	return &icmpPinger{
		// Userinfo means nothing to ICMP and must not reach error messages.
		host:       redactEndpoint(target.Endpoint),
		count:      count,
		timeout:    timeout,
		resolver:   resolver,
		dnsServer:  server,
		privileged: privileged,
	}
}

// checkICMPMode returns whether ICMP probing is available and whether raw
// (privileged) mode is needed. On macOS without root, datagram ICMP works
// without special privileges.
func checkICMPMode() (available bool, privileged bool) {
	conn, err := icmp.ListenPacket("ip4:icmp", "0.0.0.0")
	if err == nil {
		_ = conn.Close()
		return true, true
	}
	// Raw ICMP unavailable — try datagram (unprivileged) mode via pro-bing.
	p, err := probing.NewPinger("127.0.0.1")
	if err != nil {
		return false, false
	}
	p.SetPrivileged(false)
	p.Count = 1
	p.Timeout = 2 * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- p.RunWithContext(ctx) }()
	select {
	case runErr := <-done:
		return runErr == nil, false
	case <-ctx.Done():
		p.Stop()
		return false, false
	}
}

// ping sends the configured number of echo requests and returns RTT and loss.
// Failing to resolve the host, open the socket or send is reported as a
// measurement with PacketLoss 1 and ErrPhase "dns", "socket" or "send"; an
// error is returned only when ctx ends first.
func (p *icmpPinger) ping(ctx context.Context) (PingResult, error) {
	res := PingResult{Method: MethodICMP, PacketLoss: 1}
	fail := func(phase string, err error) (PingResult, error) {
		if ctx.Err() != nil {
			return PingResult{}, ctx.Err()
		}
		res.ErrPhase, res.ErrMessage = phase, redactErr(err).Error()
		return res, nil
	}

	addr, err := p.resolve(ctx)
	if err != nil {
		return fail("dns", err)
	}
	res.ResolvedIP = addr.IP.String()

	pinger := probing.New("")
	pinger.SetIPAddr(addr)
	pinger.SetPrivileged(p.privileged)
	// Send failures after the first are only logged by pro-bing, as "FATAL";
	// they are captured here instead.
	pinger.SetLogger(probing.NoopLogger{})
	var sendErr error
	pinger.OnSendError = func(_ *probing.Packet, err error) { sendErr = err }
	pinger.Count = p.count
	pinger.Interval = icmpInterval
	// pro-bing stops early only once every reply is in, so this is the
	// duration of any probe that loses a packet: the last request is sent at
	// (count-1)*interval and gets the full per-packet timeout.
	pinger.Timeout = time.Duration(p.count-1)*icmpInterval + p.timeout

	// RunWithContext stops the pinger and returns when ctx ends.
	if err := pinger.RunWithContext(ctx); err != nil {
		if sendErr != nil {
			return fail("send", err)
		}
		if errors.Is(err, os.ErrPermission) {
			err = fmt.Errorf("%w (ICMP needs root, CAP_NET_RAW, or a net.ipv4.ping_group_range that includes this process)", err)
		}
		return fail("socket", err)
	}

	stats := pinger.Statistics()
	if stats.PacketsSent > 0 {
		res.PacketLoss = float64(stats.PacketsSent-stats.PacketsRecv) / float64(stats.PacketsSent)
	}
	res.MinRTT, res.AvgRTT, res.MaxRTT = stats.MinRtt, stats.AvgRtt, stats.MaxRtt
	if stats.PacketsRecv == 0 && sendErr != nil {
		res.ErrPhase, res.ErrMessage = "send", redactErr(sendErr).Error()
	}
	return res, nil
}

// resolve looks the host up within the per-packet timeout, preferring IPv4 as
// pro-bing does. Both families are asked for in one lookup so a dead resolver
// costs one timeout, not two.
func (p *icmpPinger) resolve(ctx context.Context) (*net.IPAddr, error) {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	addrs, err := p.resolver.LookupIPAddr(ctx, p.host)
	var dnsErr *net.DNSError
	if p.dnsServer != "" && errors.As(err, &dnsErr) {
		// The resolver names the resolv.conf server it believes it asked;
		// the Dial override sent the query to dnsServer instead.
		e := *dnsErr
		e.Server = p.dnsServer
		err = &e
	}
	if err != nil {
		return nil, err
	}
	for i := range addrs {
		if addrs[i].IP.To4() != nil {
			return &addrs[i], nil
		}
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("lookup %s: no addresses", p.host)
	}
	return &addrs[0], nil
}
