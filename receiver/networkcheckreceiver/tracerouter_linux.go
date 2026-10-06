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

//go:build linux

package networkcheckreceiver // import "github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver"

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"time"

	"golang.org/x/net/ipv4"
	"golang.org/x/sys/unix"
)

// tracePath maps the path with UDP probes read through IP_RECVERR, or with
// ICMP echo requests read from a raw socket when method is "icmp".
func (t *tracerouter) tracePath(ctx context.Context, method, dest string) (TraceResult, error) {
	if method == "icmp" {
		return t.traceICMP(ctx, dest)
	}
	return t.traceUDP(ctx, dest)
}

// traceUDP sends UDP datagrams with incrementing TTL, one socket per probe,
// and reads the ICMP error each one provokes from that socket's own error
// queue (IP_RECVERR), the mechanism tracepath uses. The kernel matches the
// error to the socket that sent the probe, so no raw socket is involved and
// UDP traceroute on Linux needs no privilege: no root, no CAP_NET_RAW. It
// behaves the same under root, so it is the only UDP path on Linux.
func (t *tracerouter) traceUDP(ctx context.Context, dest string) (TraceResult, error) {
	raddr := &net.UDPAddr{IP: net.ParseIP(dest), Port: traceroutePort}
	return t.walk(ctx, dest, func(ttl int, deadline time.Time) (string, bool, time.Duration, error) {
		return probeRecvErr(ctx, raddr, ttl, deadline)
	})
}

// probeRecvErr sends one UDP probe with the given TTL and waits until deadline
// for the ICMP error it provokes.
func probeRecvErr(ctx context.Context, raddr *net.UDPAddr, ttl int, deadline time.Time) (from string, reached bool, rtt time.Duration, err error) {
	conn, err := net.DialUDP("udp4", nil, raddr)
	if err != nil {
		return "", false, 0, fmt.Errorf("dialing UDP: %w", err)
	}
	defer func() { _ = conn.Close() }()
	raw, err := conn.SyscallConn()
	if err != nil {
		return "", false, 0, err
	}

	var sockErr error
	if err := raw.Control(func(fd uintptr) {
		sockErr = errors.Join(
			unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_RECVERR, 1),
			unix.SetsockoptInt(int(fd), unix.IPPROTO_IP, unix.IP_TTL, ttl),
		)
	}); err != nil {
		return "", false, 0, err
	}
	if sockErr != nil {
		return "", false, 0, fmt.Errorf("setting IP_RECVERR and TTL %d: %w", ttl, sockErr)
	}

	sent := time.Now()
	if _, err := conn.Write(probePayload); err != nil {
		return "", false, 0, fmt.Errorf("sending UDP probe: %w", err)
	}
	var waitErr error
	if err := raw.Control(func(fd uintptr) {
		from, reached, waitErr = awaitErrQueue(ctx, int(fd), deadline)
	}); err != nil {
		return "", false, 0, err
	}
	return from, reached, time.Since(sent), waitErr
}

// pollSlice bounds each wait on the error queue so a cancelled ctx is noticed.
// Deliberate simplification: cancellation lands within one slice; an eventfd
// in the poll set would make it immediate.
const pollSlice = 50 * time.Millisecond

// awaitErrQueue waits until deadline for an ICMP error on the socket's error
// queue and returns who sent it, or "" when none arrived in time.
//
// Only POLLERR is waited for (it is always reported, so the event mask is
// empty). A UDP reply from the destination is ignored, as it is on the raw
// socket path: nothing normally listens on the traceroute port.
func awaitErrQueue(ctx context.Context, fd int, deadline time.Time) (from string, reached bool, err error) {
	buf := make([]byte, 64) // receives the quoted probe, which is not needed
	oob := make([]byte, unix.CmsgSpace(64))
	fds := []unix.PollFd{{Fd: int32(fd)}} // #nosec G115 -- a file descriptor fits in int32
	for {
		_, oobn, _, _, recvErr := unix.Recvmsg(fd, buf, oob, unix.MSG_ERRQUEUE|unix.MSG_DONTWAIT)
		if recvErr == nil {
			if from, reached, ok := parseRecvErr(oob[:oobn]); ok {
				return from, reached, nil
			}
			continue // a local error rather than an ICMP one; read the next
		}
		if recvErr != unix.EAGAIN && recvErr != unix.EINTR {
			return "", false, fmt.Errorf("reading socket error queue: %w", recvErr)
		}

		wait := min(time.Until(deadline), pollSlice)
		if wait <= 0 || ctx.Err() != nil {
			return "", false, nil
		}
		// +1 rounds up, so a sub-millisecond remainder does not spin.
		if _, pollErr := unix.Poll(fds, int(wait/time.Millisecond)+1); pollErr != nil && pollErr != unix.EINTR {
			return "", false, fmt.Errorf("polling socket error queue: %w", pollErr)
		}
	}
}

// parseRecvErr extracts the ICMP error from an IP_RECVERR control message. Its
// payload is a struct sock_extended_err (errno u32, origin u8, type u8, code
// u8, pad u8, info u32, data u32) followed by the offender: the struct
// sockaddr_in of the host that sent the ICMP error.
func parseRecvErr(oob []byte) (from string, reached bool, ok bool) {
	msgs, err := unix.ParseSocketControlMessage(oob)
	if err != nil {
		return "", false, false
	}
	for _, m := range msgs {
		if m.Header.Level != unix.SOL_IP || m.Header.Type != unix.IP_RECVERR || len(m.Data) < 24 {
			continue
		}
		origin, icmpType := m.Data[4], ipv4.ICMPType(m.Data[5])
		family := binary.NativeEndian.Uint16(m.Data[16:18])
		if origin != unix.SO_EE_ORIGIN_ICMP || family != unix.AF_INET {
			continue
		}
		addr := net.IP(m.Data[20:24]).String()
		switch icmpType {
		case ipv4.ICMPTypeTimeExceeded:
			return addr, false, true
		case ipv4.ICMPTypeDestinationUnreachable:
			// Port unreachable from the destination: the probe arrived.
			return addr, true, true
		}
	}
	return "", false, false
}
