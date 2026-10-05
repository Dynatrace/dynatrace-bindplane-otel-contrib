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

//go:build !windows && !linux

package networkcheckreceiver // import "github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver"

import (
	"context"
	"fmt"
	"net"
	"time"

	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

// tracePath maps the path with the portable raw-socket probes. Both methods
// need root here: there is no unprivileged way to receive the ICMP errors.
func (t *tracerouter) tracePath(ctx context.Context, method, dest string) (TraceResult, error) {
	if method == "icmp" {
		return t.traceICMP(ctx, dest)
	}
	return t.traceUDP(ctx, dest)
}

// traceUDP sends UDP datagrams with incrementing TTL and reads the ICMP errors
// they provoke from a raw ICMP socket.
func (t *tracerouter) traceUDP(ctx context.Context, dest string) (TraceResult, error) {
	icmpConn, err := icmp.ListenPacket("ip4:icmp", "0.0.0.0")
	if err != nil {
		return TraceResult{}, fmt.Errorf("opening ICMP listener for traceroute: %w", err)
	}
	defer func() { _ = icmpConn.Close() }()
	stop := context.AfterFunc(ctx, func() { _ = icmpConn.SetReadDeadline(time.Now()) })
	defer stop()

	destAddr := &net.UDPAddr{IP: net.ParseIP(dest), Port: traceroutePort}
	return t.walk(ctx, dest, func(ttl int, deadline time.Time) (string, bool, time.Duration, error) {
		udpConn, err := net.DialUDP("udp4", nil, destAddr)
		if err != nil {
			return "", false, 0, fmt.Errorf("dialing UDP: %w", err)
		}
		// Held open until the answer is in, so no other socket can take the
		// source port that identifies this probe in the ICMP error.
		defer func() { _ = udpConn.Close() }()
		if err := ipv4.NewConn(udpConn).SetTTL(ttl); err != nil {
			return "", false, 0, fmt.Errorf("setting TTL %d: %w", ttl, err)
		}

		sent := time.Now()
		if _, err := udpConn.Write(probePayload); err != nil {
			return "", false, 0, fmt.Errorf("sending UDP probe: %w", err)
		}
		from, reached, err := awaitProbeReply(ctx, icmpConn, deadline, probeKey{
			dst:     destAddr.IP,
			udp:     true,
			srcPort: udpConn.LocalAddr().(*net.UDPAddr).Port,
			dstPort: traceroutePort,
		})
		return from, reached, time.Since(sent), err
	})
}
