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

package networkcheckreceiver // import "github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver"

import (
	"context"
	"encoding/binary"
	"fmt"
	"net"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// Windows does not deliver unsolicited inbound ICMP time-exceeded messages to a
// raw socket, so the portable UDP and ICMP traceroute methods time out on every
// hop there even when running with Administrator rights. The IP Helper API
// correlates the replies in the kernel and hands them back directly, which is
// how the built-in tracert.exe works. We use the same API here.
var (
	modIphlpapi         = windows.NewLazySystemDLL("iphlpapi.dll")
	procIcmpCreateFile  = modIphlpapi.NewProc("IcmpCreateFile")
	procIcmpCloseHandle = modIphlpapi.NewProc("IcmpCloseHandle")
	procIcmpSendEcho    = modIphlpapi.NewProc("IcmpSendEcho")
)

// Status values returned in ICMP_ECHO_REPLY.Status. See IP_STATUS in ipexport.h.
const (
	ipSuccess           = 0
	ipReqTimedOut       = 11010
	ipTTLExpiredTransit = 11013
)

// ipOptionInformation mirrors IP_OPTION_INFORMATION from ipexport.h. Go lays
// it out exactly as the C compiler does on both 64-bit and 32-bit Windows:
// OptionsData is pointer-aligned, which pads the struct to 16 bytes on 64-bit
// and leaves it at 8 on 32-bit.
type ipOptionInformation struct {
	TTL         uint8
	TOS         uint8
	Flags       uint8
	OptionsSize uint8
	OptionsData uintptr
}

// icmpEchoReply mirrors ICMP_ECHO_REPLY from ipexport.h: 40 bytes on 64-bit
// Windows, 28 on 32-bit, with Go's layout matching C's on both. Only Address,
// Status and RoundTripTime are read back, and they sit at offsets 0-11 on every
// architecture. Data points into the reply buffer and is never dereferenced.
type icmpEchoReply struct {
	Address       uint32
	Status        uint32
	RoundTripTime uint32
	DataSize      uint16
	Reserved      uint16
	Data          uintptr
	Options       ipOptionInformation
}

// tracePath maps the path with the IP Helper API whichever method is
// configured; the method only chooses a probe type on the other platforms.
func (t *tracerouter) tracePath(ctx context.Context, _ string, dest string) (TraceResult, error) {
	hops, unreachable, err := t.traceNative(ctx, dest)
	return TraceResult{
		Hops:         hops,
		Method:       "native",
		Reached:      hopsReachedDest(hops, dest),
		AbortedEarly: hopsAbortedEarly(hops, t.maxHops(), t.abortAfter()),
		Unreachable:  unreachable,
	}, err
}

// traceNative maps the path to dest using IcmpSendEcho with an incrementing
// TTL. It needs no Administrator rights.
// nativeOutcome says how a hop's reply ends, or does not end, the walk.
type nativeOutcome int

const (
	nativeNext        nativeOutcome = iota // continue with the next TTL
	nativeReached                          // the destination answered
	nativeUnreachable                      // a hop reported the path cannot continue
)

func (t *tracerouter) traceNative(ctx context.Context, dest string) (hops []HopResult, unreachable bool, err error) {
	destIP := net.ParseIP(dest).To4()
	if destIP == nil {
		return nil, false, fmt.Errorf("traceroute requires an IPv4 destination, got %q", dest)
	}
	// IPAddr is a DWORD holding the octets in network byte order, which is the
	// same as reading the 4 bytes in memory order on a little-endian host.
	destAddr := binary.LittleEndian.Uint32(destIP)

	handle, _, createErr := procIcmpCreateFile.Call()
	if windows.Handle(handle) == windows.InvalidHandle {
		return nil, false, fmt.Errorf("IcmpCreateFile: %w", createErr)
	}
	defer procIcmpCloseHandle.Call(handle)

	// The reply buffer must hold an ICMP_ECHO_REPLY plus the echoed request
	// data and any ICMP error payload. The API requires at least
	// sizeof(ICMP_ECHO_REPLY) + 8; oversize it so a hop that returns options
	// or a larger error body still fits.
	payload := probePayload
	replyBuf := make([]byte, int(unsafe.Sizeof(icmpEchoReply{}))+len(payload)+256)

	consecutiveTimeouts := 0
	for ttl := 1; ttl <= t.maxHops(); ttl++ {
		if ctxDone(ctx) {
			break
		}

		// Probe until this hop answers or the attempts are exhausted. Only a
		// silent hop is retried, so a healthy path costs one probe per hop.
		hop := HopResult{Index: ttl, Address: unansweredHopAddress, TimedOut: true}
		outcome := nativeNext
		for attempt := 0; attempt < t.probesPerHop(); attempt++ {
			// IcmpSendEcho blocks for its whole timeout and cannot be
			// cancelled, so the wait is cut to ctx's deadline up front.
			waitMs := time.Until(t.hopDeadline(ctx)).Milliseconds()
			if ctxDone(ctx) || waitMs <= 0 {
				break
			}

			// #nosec G115 -- ttl <= maxHops() <= maxTTL (255), and min() keeps it there.
			opts := ipOptionInformation{TTL: uint8(min(ttl, maxTTL))}
			sent := time.Now()
			// #nosec G103 -- the buffers are Go-owned and stay live for this synchronous call; replyBuf is oversized for the reply.
			n, _, sendErr := procIcmpSendEcho.Call(
				handle,
				uintptr(destAddr),
				uintptr(unsafe.Pointer(&payload[0])),
				uintptr(len(payload)),
				uintptr(unsafe.Pointer(&opts)),
				uintptr(unsafe.Pointer(&replyBuf[0])),
				uintptr(len(replyBuf)),
				uintptr(waitMs),
			)
			elapsed := time.Since(sent)

			var reply icmpEchoReply
			if n != 0 {
				// #nosec G103 -- replyBuf holds a full ICMP_ECHO_REPLY (see icmpEchoReply) and only fixed-offset fields are read.
				reply = *(*icmpEchoReply)(unsafe.Pointer(&replyBuf[0]))
			}
			var err error
			if hop, outcome, err = nativeHop(ttl, attempt+1, n, sendErr, reply, elapsed); err != nil {
				return hops, false, err
			}
			if !hop.TimedOut || outcome != nativeNext {
				break
			}
		}

		// A cancelled retry loop leaves the hop silent without it having been
		// given its full chance, so stop rather than recording a silent hop
		// that was never really probed.
		if hop.TimedOut && outcome == nativeNext && ctxDone(ctx) {
			break
		}

		hops = append(hops, hop)
		if outcome == nativeReached {
			break
		}
		if outcome == nativeUnreachable {
			unreachable = true
			break
		}
		if !hop.TimedOut {
			consecutiveTimeouts = 0
			continue
		}
		consecutiveTimeouts++
		if abort := t.abortAfter(); abort > 0 && consecutiveTimeouts >= abort {
			break
		}
	}

	return hops, false, nil
}

// nativeHop interprets one IcmpSendEcho call, the probes-th sent for the hop at
// ttl: n is the reply count it returned, sendErr its error, reply the
// ICMP_ECHO_REPLY it wrote (meaningful only when n != 0) and elapsed how long
// it took. A silent hop comes back TimedOut and may be probed again; outcome says
// true when the walk ends at this hop; err is a local failure that ends the
// trace.
func nativeHop(ttl, probes int, n uintptr, sendErr error, reply icmpEchoReply, elapsed time.Duration) (hop HopResult, outcome nativeOutcome, err error) {
	silent := HopResult{Index: ttl, Address: unansweredHopAddress, TimedOut: true, Probes: probes}

	// A zero reply count means no usable answer. The common case is the hop
	// staying silent, which surfaces as IP_REQ_TIMED_OUT. Anything else is a
	// real failure worth surfacing rather than retried or recorded as a
	// silent hop.
	if n == 0 {
		if errno, ok := sendErr.(windows.Errno); ok && uint32(errno) != ipReqTimedOut && uint32(errno) != 0 {
			return silent, nativeNext, fmt.Errorf("IcmpSendEcho (ttl %d): %w", ttl, sendErr)
		}
		return silent, nativeNext, nil
	}

	// RoundTripTime is whole milliseconds and is frequently reported as 0 for
	// time-exceeded replies, so fall back to the measured elapsed time to
	// avoid publishing a stream of zero-latency hops.
	rtt := time.Duration(reply.RoundTripTime) * time.Millisecond
	if rtt == 0 {
		rtt = elapsed
	}

	var octets [4]byte
	binary.LittleEndian.PutUint32(octets[:], reply.Address)
	hop = HopResult{
		Index:   ttl,
		Address: net.IPv4(octets[0], octets[1], octets[2], octets[3]).String(),
		RTT:     rtt,
		Probes:  probes,
	}
	switch reply.Status {
	case ipSuccess:
		return hop, nativeReached, nil
	case ipTTLExpiredTransit:
		return hop, nativeNext, nil
	default:
		// Unreachable and similar statuses name a real router that cannot
		// forward the probe: the path ends there, short of the destination,
		// as on the other platforms.
		return hop, nativeUnreachable, nil
	}
}
