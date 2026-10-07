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

//go:build windows

package networkcheckreceiver

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sys/windows"
)

func TestNativeHop(t *testing.T) {
	// IPAddr holds the octets in network byte order, read in memory order.
	addr := func(ip string) uint32 { return binary.LittleEndian.Uint32(net.ParseIP(ip).To4()) }
	const elapsed = 7 * time.Millisecond
	silent := HopResult{Index: 3, Address: unansweredHopAddress, TimedOut: true, Probes: 2}

	for _, tc := range []struct {
		name    string
		n       uintptr
		sendErr error
		reply   icmpEchoReply
		want    HopResult
		outcome nativeOutcome
		err     error
	}{
		{"destination answered", 1, windows.Errno(0), icmpEchoReply{Address: addr("192.0.2.1"), Status: ipSuccess, RoundTripTime: 12},
			HopResult{Index: 3, Address: "192.0.2.1", RTT: 12 * time.Millisecond, Probes: 2}, nativeReached, nil},
		{"ttl expired in transit", 1, windows.Errno(0), icmpEchoReply{Address: addr("10.0.0.1"), Status: ipTTLExpiredTransit, RoundTripTime: 4},
			HopResult{Index: 3, Address: "10.0.0.1", RTT: 4 * time.Millisecond, Probes: 2}, nativeNext, nil},
		{"zero round trip falls back to elapsed", 1, windows.Errno(0), icmpEchoReply{Address: addr("10.0.0.1"), Status: ipTTLExpiredTransit},
			HopResult{Index: 3, Address: "10.0.0.1", RTT: elapsed, Probes: 2}, nativeNext, nil},
		{"timed out", 0, windows.Errno(ipReqTimedOut), icmpEchoReply{}, silent, nativeNext, nil},
		{"no reply without an error code", 0, windows.Errno(0), icmpEchoReply{}, silent, nativeNext, nil},
		{"no reply with a non-errno error", 0, errors.New("unexpected"), icmpEchoReply{}, silent, nativeNext, nil},
		// IP_DEST_HOST_UNREACHABLE: the router that said so is the last hop;
		// the path cannot continue past it.
		{"other status ends the path", 1, windows.Errno(0), icmpEchoReply{Address: addr("10.0.0.1"), Status: 11003, RoundTripTime: 2},
			HopResult{Index: 3, Address: "10.0.0.1", RTT: 2 * time.Millisecond, Probes: 2}, nativeUnreachable, nil},
		{"local failure", 0, windows.ERROR_INVALID_PARAMETER, icmpEchoReply{}, silent, nativeNext, windows.ERROR_INVALID_PARAMETER},
	} {
		t.Run(tc.name, func(t *testing.T) {
			hop, outcome, err := nativeHop(3, 2, tc.n, tc.sendErr, tc.reply, elapsed)
			require.ErrorIs(t, err, tc.err)
			require.Equal(t, tc.want, hop)
			require.Equal(t, tc.outcome, outcome)
		})
	}
}

// The native path needs no Administrator rights; loopback answers IcmpSendEcho
// at TTL 1 with IP_SUCCESS, so the trace is one reached hop.
func TestNativeTraceLoopback(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	tr := newTracerouter(&TracerouteConfig{Method: "icmp", MaxHops: 3, Timeout: 2 * time.Second, ProbesPerHop: 1}, "127.0.0.1", "")
	res, err := tr.trace(ctx)
	require.NoError(t, err)
	require.Equal(t, "native", res.Method)
	require.True(t, res.Reached)
	require.False(t, res.AbortedEarly)
	require.Len(t, res.Hops, 1)
	require.Equal(t, "127.0.0.1", res.Hops[0].Address)
	require.False(t, res.Hops[0].TimedOut)
	require.Positive(t, res.Hops[0].RTT)
}

func TestNativeTraceRejectsNonIPv4(t *testing.T) {
	_, err := newTracerouter(&TracerouteConfig{}, "", "").traceNative(context.Background(), "2001:db8::1")
	require.ErrorContains(t, err, "requires an IPv4 destination")
}
