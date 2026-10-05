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
	"os/exec"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
	"golang.org/x/net/icmp"
)

// unreachableIPv4 is TEST-NET-1 (RFC 5737): routable on most hosts, never
// answered.
const unreachableIPv4 = "192.0.2.1"

func icmpTarget(endpoint string, count int, timeout time.Duration) TargetConfig {
	tc := TargetConfig{Method: MethodICMP, PingCount: count}
	tc.Endpoint = endpoint
	tc.Timeout = timeout
	return tc
}

// requireICMP skips when this host cannot send ICMP echo at all, and returns
// the mode the receiver would use.
func requireICMP(t *testing.T) (privileged bool) {
	t.Helper()
	available, privileged := checkICMPMode()
	if !available {
		t.Skip("ICMP echo unavailable on this host")
	}
	return privileged
}

func TestICMPPingResolveFailureIsMeasurement(t *testing.T) {
	s := startFakeDNS(t, "127.0.0.1")
	tc := icmpTarget("nonexistent.invalid", 3, 2*time.Second)
	tc.DNSServer = s.addr

	start := time.Now()
	r, err := newICMPPinger(tc, false).ping(context.Background())
	require.NoError(t, err)
	require.Less(t, time.Since(start), time.Second)

	require.Equal(t, MethodICMP, r.Method)
	require.Equal(t, 1.0, r.PacketLoss)
	require.Equal(t, "dns", r.ErrPhase)
	require.Contains(t, r.ErrMessage, "nonexistent.invalid on "+s.addr, "the error must name the server that was asked")
	require.Contains(t, s.queries(), "udp A nonexistent.invalid.", "the target's dns_server must do the lookup")
}

func TestICMPPingDropsEndpointUserinfo(t *testing.T) {
	s := startFakeDNS(t, "127.0.0.1")
	tc := icmpTarget("user:secret@nonexistent.invalid", 1, time.Second)
	tc.DNSServer = "user:secret@" + s.addr

	r, err := newICMPPinger(tc, false).ping(context.Background())
	require.NoError(t, err)
	require.Equal(t, "dns", r.ErrPhase)
	require.NotContains(t, r.ErrMessage, "secret")
	require.Contains(t, s.queries(), "udp A nonexistent.invalid.")
}

func TestICMPPingResolvesThroughDNSServer(t *testing.T) {
	s := startFakeDNS(t, "127.0.0.1")
	// probe.test exists only in the fake zone, so resolving it at all proves
	// the configured server was used rather than the system resolver.
	tc := icmpTarget("probe.test", 1, 300*time.Millisecond)
	tc.DNSServer = s.addr
	_, privileged := checkICMPMode()

	r, err := newICMPPinger(tc, privileged).ping(context.Background())
	require.NoError(t, err)
	require.Equal(t, "127.0.0.1", r.ResolvedIP)
	require.True(t, slices.Contains(s.queries(), "udp A probe.test."), s.queries())
}

func TestICMPPingSocketPermissionIsMeasurement(t *testing.T) {
	// Raw mode needs root or CAP_NET_RAW; datagram mode is refused only where
	// the capability check already found ICMP unavailable.
	modes := []bool{true}
	if available, _ := checkICMPMode(); !available {
		modes = append(modes, false)
	}
	if conn, err := icmp.ListenPacket("ip4:icmp", "0.0.0.0"); err == nil {
		_ = conn.Close()
		modes = modes[1:]
	}
	if len(modes) == 0 {
		t.Skip("every ICMP socket type is permitted here")
	}

	for _, privileged := range modes {
		r, err := newICMPPinger(icmpTarget("127.0.0.1", 1, time.Second), privileged).ping(context.Background())
		require.NoError(t, err)
		require.Equal(t, 1.0, r.PacketLoss)
		require.Equal(t, "socket", r.ErrPhase, "privileged=%v: %s", privileged, r.ErrMessage)
		require.Contains(t, r.ErrMessage, "ICMP needs root")
		require.Equal(t, "127.0.0.1", r.ResolvedIP)
	}
}

// pro-bing stops early only when every reply is in, so a probe that loses a
// packet lasts its whole Timeout. That used to be count x timeout (15s on the
// defaults); it is now (count-1) x interval + timeout.
func TestICMPPingUnreachableTiming(t *testing.T) {
	privileged := requireICMP(t)

	start := time.Now()
	r, err := newICMPPinger(icmpTarget(unreachableIPv4, 3, time.Second), privileged).ping(context.Background())
	took := time.Since(start)
	require.NoError(t, err)
	require.Equal(t, 1.0, r.PacketLoss)
	if r.ErrPhase != "" {
		t.Skipf("send failed on this host (%s: %s); timing not observable", r.ErrPhase, r.ErrMessage)
	}
	// (3-1) x 200ms + 1s = 1.4s; count x timeout would be 3s. The upper bound
	// is loose because an occasional ~1s stall in the run has been seen on
	// macOS hosts.
	require.GreaterOrEqual(t, took, 1350*time.Millisecond)
	require.Less(t, took, 2700*time.Millisecond)
}

func TestICMPPingLoopbackUsesShortInterval(t *testing.T) {
	privileged := requireICMP(t)
	if exec.Command("/sbin/ping", "-c1", "-t1", "127.0.0.1").Run() != nil {
		t.Skip("loopback ICMP is not answered on this host")
	}

	start := time.Now()
	r, err := newICMPPinger(icmpTarget("127.0.0.1", 3, 2*time.Second), privileged).ping(context.Background())
	require.NoError(t, err)
	require.Zero(t, r.PacketLoss, r.ErrMessage)
	require.Positive(t, r.MaxRTT)
	require.Less(t, time.Since(start), time.Second, "3 packets at a 200ms interval")
}

func TestICMPPingCancelReturnsError(t *testing.T) {
	ignore := goleak.IgnoreCurrent()
	privileged := requireICMP(t)

	ctx, cancel := context.WithCancel(context.Background())
	time.AfterFunc(100*time.Millisecond, cancel)
	start := time.Now()
	_, err := newICMPPinger(icmpTarget(unreachableIPv4, 5, 5*time.Second), privileged).ping(ctx)
	require.ErrorIs(t, err, context.Canceled)
	require.Less(t, time.Since(start), 400*time.Millisecond)
	goleak.VerifyNone(t, ignore)
}
