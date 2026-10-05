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
	"fmt"
	"time"

	probing "github.com/prometheus-community/pro-bing"
	"golang.org/x/net/icmp"
)

// icmpPinger sends ICMP echo requests and collects RTT statistics.
type icmpPinger struct {
	host       string
	count      int
	timeout    time.Duration
	dnsServer  string
	privileged bool // true = raw ICMP (root), false = datagram ICMP (macOS unprivileged)
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
	return &icmpPinger{
		host:       target.Endpoint,
		count:      count,
		timeout:    timeout,
		dnsServer:  target.DNSServer,
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

func (p *icmpPinger) ping(ctx context.Context) (PingResult, error) {
	pinger, err := probing.NewPinger(p.host)
	if err != nil {
		return PingResult{}, fmt.Errorf("creating pinger for %s: %w", p.host, err)
	}
	pinger.SetPrivileged(p.privileged)
	pinger.Count = p.count
	pinger.Timeout = p.timeout * time.Duration(p.count)

	// Run in a goroutine so we can respect context cancellation.
	done := make(chan error, 1)
	go func() {
		done <- pinger.RunWithContext(ctx)
	}()

	select {
	case err := <-done:
		if err != nil {
			return PingResult{}, fmt.Errorf("pinging %s: %w", p.host, err)
		}
	case <-ctx.Done():
		pinger.Stop()
		return PingResult{}, ctx.Err()
	}

	stats := pinger.Statistics()
	loss := 1.0
	if stats.PacketsSent > 0 {
		loss = float64(stats.PacketsSent-stats.PacketsRecv) / float64(stats.PacketsSent)
	}

	return PingResult{
		MinRTT:     stats.MinRtt,
		AvgRTT:     stats.AvgRtt,
		MaxRTT:     stats.MaxRtt,
		PacketLoss: loss,
		Method:     MethodICMP,
	}, nil
}
