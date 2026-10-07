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

package networkcheckreceiver

import (
	"context"
	"encoding/binary"
	"net"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
	"unsafe"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"
)

// TestLinuxUDPTraceNeedsNoPrivilege traces loopback, whose first hop is the
// destination answering port unreachable. It runs as whatever user runs the
// tests: the UDP path reads errors from the probe socket's own error queue, so
// it must work without root or CAP_NET_RAW.
func TestLinuxUDPTraceNeedsNoPrivilege(t *testing.T) {
	t.Logf("euid=%d raw ICMP allowed=%v", os.Geteuid(), rawICMPAllowed())
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tr := newTracerouter(&TracerouteConfig{Method: "udp", MaxHops: 3, Timeout: time.Second}, "127.0.0.1", "")
	res, err := tr.trace(ctx)
	require.NoError(t, err)
	require.Equal(t, "udp", res.Method)
	require.True(t, res.Reached)
	require.Len(t, res.Hops, 1)
	require.Equal(t, "127.0.0.1", res.Hops[0].Address)
	require.False(t, res.Hops[0].TimedOut)
	require.Positive(t, res.Hops[0].RTT)
}

// TestLinuxICMPTraceNeedsRawSocket pins the privilege split: the ICMP method
// works with a raw socket and fails with a permission error without one.
func TestLinuxICMPTraceNeedsRawSocket(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	tr := newTracerouter(&TracerouteConfig{Method: "icmp", MaxHops: 3, Timeout: time.Second}, "127.0.0.1", "")
	res, err := tr.trace(ctx)
	if !rawICMPAllowed() {
		require.ErrorIs(t, err, os.ErrPermission)
		t.Logf("unprivileged icmp trace: %v", err)
		return
	}
	require.NoError(t, err)
	require.True(t, res.Reached)
	require.Equal(t, "127.0.0.1", res.Hops[0].Address)
}

// TestLinuxUDPTraceHonoursCancel holds the traceroute port on loopback so the
// destination stays silent, then cancels mid-hop: the trace must return
// promptly and must not report the interrupted hop as timed out.
func TestLinuxUDPTraceHonoursCancel(t *testing.T) {
	l, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: traceroutePort})
	if err != nil {
		t.Skipf("traceroute port busy: %v", err)
	}
	defer l.Close()

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(200*time.Millisecond, cancel)
	tr := newTracerouter(&TracerouteConfig{Method: "udp", MaxHops: 3, Timeout: 10 * time.Second}, "127.0.0.1", "")

	start := time.Now()
	res, err := tr.trace(ctx)
	require.NoError(t, err)
	require.Less(t, time.Since(start), time.Second, "cancellation must cut the 10s hop wait short")
	require.Empty(t, res.Hops, "a hop interrupted by cancellation was never observed as silent")
}

// The tests below need a real network path and are opted into with
// TRACEROUTE_NET=1: CI runners commonly drop ICMP, and cloud networks (Azure,
// for one) answer neither for the gateway nor for intermediate hops, so these
// assertions hold only on a network whose routers send time-exceeded.
func requireNetTests(t *testing.T) {
	if os.Getenv("TRACEROUTE_NET") == "" {
		t.Skip("set TRACEROUTE_NET=1 to run traceroute tests against the network")
	}
}

// defaultGateway reads the IPv4 default route's gateway from /proc/net/route.
func defaultGateway(t *testing.T) string {
	b, err := os.ReadFile("/proc/net/route")
	require.NoError(t, err)
	for _, line := range strings.Split(string(b), "\n")[1:] {
		f := strings.Fields(line)
		if len(f) > 2 && f[1] == "00000000" {
			v, err := strconv.ParseUint(f[2], 16, 32)
			require.NoError(t, err)
			ip := make(net.IP, 4)
			binary.LittleEndian.PutUint32(ip, uint32(v))
			return ip.String()
		}
	}
	t.Skip("no IPv4 default route")
	return ""
}

func TestLinuxUDPTraceGateway(t *testing.T) {
	requireNetTests(t)
	gw := defaultGateway(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tr := newTracerouter(&TracerouteConfig{Method: "udp", MaxHops: 3, Timeout: 2 * time.Second, MaxConsecutiveTimeouts: 2}, gw, "")
	res, err := tr.trace(ctx)
	require.NoError(t, err)
	t.Logf("gateway %s: reached=%v hops=%+v", gw, res.Reached, res.Hops)
	require.NotEmpty(t, res.Hops)
	require.False(t, res.Hops[0].TimedOut, "the gateway is one hop away and must answer")
	require.Equal(t, gw, res.Hops[0].Address)
}

func TestLinuxUDPTraceInternet(t *testing.T) {
	requireNetTests(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	tr := newTracerouter(&TracerouteConfig{Method: "udp", MaxHops: 30, Timeout: 2 * time.Second, MaxConsecutiveTimeouts: 3}, "1.1.1.1", "")
	res, err := tr.trace(ctx)
	require.NoError(t, err)
	answered := 0
	for _, h := range res.Hops {
		if !h.TimedOut {
			answered++
		}
	}
	t.Logf("1.1.1.1: reached=%v aborted=%v hops=%+v", res.Reached, res.AbortedEarly, res.Hops)
	// Docker Desktop's NAT collapses the real path, so only the shape is
	// asserted, not a hop count.
	require.Positive(t, answered)
}

// recvErrCmsg builds the control message IP_RECVERR delivers: a struct
// sock_extended_err followed by the offender's struct sockaddr_in.
func recvErrCmsg(typ int32, origin, icmpType, icmpCode uint8, family uint16, offender net.IP) []byte {
	data := make([]byte, 32)
	data[4], data[5], data[6] = origin, icmpType, icmpCode
	binary.NativeEndian.PutUint16(data[16:18], family)
	copy(data[20:24], offender.To4())

	b := make([]byte, unix.CmsgSpace(len(data)))
	h := (*unix.Cmsghdr)(unsafe.Pointer(&b[0]))
	h.Level, h.Type = unix.SOL_IP, typ
	h.SetLen(unix.CmsgLen(len(data)))
	copy(b[unix.CmsgLen(0):], data)
	return b
}

func TestParseRecvErr(t *testing.T) {
	dest, router := net.IPv4(192, 0, 2, 10), net.IPv4(10, 0, 0, 1)
	icmpErr := func(typ, code uint8, from net.IP) []byte {
		return recvErrCmsg(unix.IP_RECVERR, unix.SO_EE_ORIGIN_ICMP, typ, code, unix.AF_INET, from)
	}
	for _, tc := range []struct {
		name                 string
		oob                  []byte
		from                 string // "" = not an ICMP error
		reached, unreachable bool
	}{
		{"time exceeded", icmpErr(11, 0, router), "10.0.0.1", false, false},
		{"port unreachable from the destination", icmpErr(3, 3, dest), "192.0.2.10", true, false},
		{"port unreachable from a router", icmpErr(3, 3, router), "10.0.0.1", false, true},
		{"host unreachable", icmpErr(3, 1, dest), "192.0.2.10", false, true},
		{"other ICMP type", icmpErr(12, 0, router), "", false, false},
		{"local error", recvErrCmsg(unix.IP_RECVERR, unix.SO_EE_ORIGIN_LOCAL, 0, 0, unix.AF_INET, router), "", false, false},
		{"not IPv4", recvErrCmsg(unix.IP_RECVERR, unix.SO_EE_ORIGIN_ICMP, 11, 0, unix.AF_INET6, router), "", false, false},
		{"another control message", recvErrCmsg(unix.IP_TTL, unix.SO_EE_ORIGIN_ICMP, 11, 0, unix.AF_INET, router), "", false, false},
		{"truncated", icmpErr(11, 0, router)[:unix.CmsgLen(8)], "", false, false},
		{"empty", nil, "", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			from, reached, unreachable, ok := parseRecvErr(tc.oob, dest)
			require.Equal(t, tc.from != "", ok)
			require.Equal(t, tc.from, from)
			require.Equal(t, tc.reached, reached)
			require.Equal(t, tc.unreachable, unreachable)
		})
	}
}
