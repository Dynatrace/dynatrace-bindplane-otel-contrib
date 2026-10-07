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

//go:build !windows

package networkcheckreceiver

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/net/icmp"
	"golang.org/x/net/ipv4"
)

// The raw-socket probe paths need root or CAP_NET_RAW, so these tests skip
// without it. Windows is excluded: it traces through the native API and does
// not deliver ICMP errors to raw sockets.

// rawICMPAllowed reports whether this process may open a raw ICMP socket
// (root or CAP_NET_RAW).
func rawICMPAllowed() bool {
	c, err := icmp.ListenPacket("ip4:icmp", "0.0.0.0")
	if err != nil {
		return false
	}
	_ = c.Close()
	return true
}

func openRawICMP(t *testing.T) *icmp.PacketConn {
	t.Helper()
	c, err := icmp.ListenPacket("ip4:icmp", "0.0.0.0")
	if err != nil {
		t.Skipf("needs a raw ICMP socket (root or CAP_NET_RAW): %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// Loopback is a one-hop path whose destination answers both probe types, so
// each method reaches it at TTL 1. On Linux the udp method reads the probe
// socket's error queue rather than the raw socket.
func TestRawSocketTraceLoopback(t *testing.T) {
	if !rawICMPAllowed() {
		t.Skip("needs a raw ICMP socket (root or CAP_NET_RAW)")
	}
	for _, method := range []string{"udp", "icmp"} {
		t.Run(method, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			tr := newTracerouter(&TracerouteConfig{Method: method, MaxHops: 3, Timeout: 2 * time.Second, ProbesPerHop: 1}, "127.0.0.1", "")
			res, err := tr.trace(ctx)
			require.NoError(t, err)
			require.Equal(t, method, res.Method)
			require.True(t, res.Reached)
			require.Len(t, res.Hops, 1)
			require.Equal(t, "127.0.0.1", res.Hops[0].Address)
			require.False(t, res.Hops[0].TimedOut)
			require.Equal(t, 1, res.Hops[0].Probes)
		})
	}
}

func TestAwaitProbeReply(t *testing.T) {
	dest := net.IPv4(127, 0, 0, 1)
	sendEcho := func(t *testing.T, c *icmp.PacketConn, id, seq int) {
		t.Helper()
		b, err := (&icmp.Message{Type: ipv4.ICMPTypeEcho, Body: &icmp.Echo{ID: id, Seq: seq, Data: probePayload}}).Marshal(nil)
		require.NoError(t, err)
		_, err = c.WriteTo(b, &net.IPAddr{IP: dest})
		require.NoError(t, err)
	}
	ctx := context.Background()
	far := func() time.Time { return time.Now().Add(10 * time.Second) }

	t.Run("matching reply", func(t *testing.T) {
		c := openRawICMP(t)
		sendEcho(t, c, 0x4e43, 1)
		from, reached, err := awaitProbeReply(ctx, c, far(), probeKey{dst: dest, echoID: 0x4e43, echoSeq: 1})
		require.NoError(t, err)
		require.Equal(t, "127.0.0.1", from)
		require.True(t, reached)
	})

	t.Run("unrelated traffic is skipped until the deadline", func(t *testing.T) {
		c := openRawICMP(t)
		sendEcho(t, c, 0x4e43, 2)
		from, reached, err := awaitProbeReply(ctx, c, time.Now().Add(300*time.Millisecond), probeKey{dst: dest, echoID: 0x4e43, echoSeq: 3})
		require.NoError(t, err)
		require.Empty(t, from, "the reply to another sequence number is not this probe's answer")
		require.False(t, reached)
	})

	t.Run("cancelled before waiting", func(t *testing.T) {
		c := openRawICMP(t)
		cancelled, cancel := context.WithCancel(ctx)
		cancel()
		from, _, err := awaitProbeReply(cancelled, c, far(), probeKey{dst: dest})
		require.NoError(t, err)
		require.Empty(t, from)
	})

	t.Run("closed socket", func(t *testing.T) {
		c := openRawICMP(t)
		require.NoError(t, c.Close())
		_, _, err := awaitProbeReply(ctx, c, far(), probeKey{dst: dest})
		require.ErrorIs(t, err, net.ErrClosed)
	})

	t.Run("socket closed while waiting", func(t *testing.T) {
		c := openRawICMP(t)
		time.AfterFunc(50*time.Millisecond, func() { _ = c.Close() })
		_, _, err := awaitProbeReply(ctx, c, far(), probeKey{dst: dest})
		require.ErrorIs(t, err, net.ErrClosed)
		require.ErrorContains(t, err, "reading ICMP reply")
	})
}
