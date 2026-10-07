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
	"unsafe"

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
	// IcmpSendEcho reports whole milliseconds and the fallback clock can read
	// 0 for a reply inside the same millisecond, so loopback's RTT is only
	// bounded above.
	require.Less(t, res.Hops[0].RTT, 2*time.Second)
}

// The walk is driven with crafted replies: a hop that answers with anything
// but IP_SUCCESS or IP_TTL_EXPIRED_TRANSIT ends the path short of the
// destination, and the trace reports that, as on the other platforms.
func TestNativeTraceOutcome(t *testing.T) {
	addr := func(ip string) uint32 { return binary.LittleEndian.Uint32(net.ParseIP(ip).To4()) }
	const dest = "192.0.2.1"
	transit := icmpEchoReply{Address: addr("10.0.0.1"), Status: ipTTLExpiredTransit, RoundTripTime: 1}
	for _, tc := range []struct {
		name        string
		last        icmpEchoReply
		reached     bool
		unreachable bool
	}{
		{"destination answers", icmpEchoReply{Address: addr(dest), Status: ipSuccess, RoundTripTime: 2}, true, false},
		// IP_DEST_HOST_UNREACHABLE from the second router.
		{"router cannot forward", icmpEchoReply{Address: addr("10.0.0.2"), Status: 11003, RoundTripTime: 2}, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			orig := sendEcho
			t.Cleanup(func() { sendEcho = orig })
			replies := map[uint8]icmpEchoReply{1: transit, 2: tc.last}
			sendEcho = func(_ uintptr, _ uint32, opts *ipOptionInformation, _, replyBuf []byte, _ int64) (uintptr, error) {
				r, ok := replies[opts.TTL]
				if !ok {
					return 0, windows.Errno(ipReqTimedOut)
				}
				*(*icmpEchoReply)(unsafe.Pointer(&replyBuf[0])) = r
				return 1, nil
			}

			tr := newTracerouter(&TracerouteConfig{MaxHops: 5, Timeout: time.Second, ProbesPerHop: 1}, dest, "")
			res, err := tr.tracePath(context.Background(), "", dest)
			require.NoError(t, err)
			require.Equal(t, tc.reached, res.Reached)
			require.Equal(t, tc.unreachable, res.Unreachable)
			require.False(t, res.AbortedEarly)
			require.Len(t, res.Hops, 2)
			require.Equal(t, "10.0.0.1", res.Hops[0].Address)
			require.Equal(t, 2*time.Millisecond, res.Hops[1].RTT)
		})
	}
}

func TestNativeTraceRejectsNonIPv4(t *testing.T) {
	_, _, err := newTracerouter(&TracerouteConfig{}, "", "").traceNative(context.Background(), "2001:db8::1")
	require.ErrorContains(t, err, "requires an IPv4 destination")
}
