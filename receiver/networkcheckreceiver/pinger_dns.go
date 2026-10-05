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
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"strings"
	"time"

	"golang.org/x/net/dns/dnsmessage"
)

// dnsPinger sends one DNS query straight to a specific server and measures the
// time to a valid answer.
//
// The query is built by hand rather than going through net.Resolver: the
// resolver asks for A and AAAA together whatever record type is configured,
// answers names such as "localhost" from the hosts file without contacting the
// server, and appends search domains. Each of those makes the probe report on
// something other than the server it names.
type dnsPinger struct {
	server     string // host:port, e.g. "8.8.8.8:53" or "[::1]:53"
	query      string // name as configured; reported as the query name attribute
	fqdn       string // query with the trailing dot that makes it absolute
	recordType string // "A", "AAAA", "CNAME", "MX", "TXT"
	timeout    time.Duration
}

var dnsRecordTypes = map[string]dnsmessage.Type{
	"A":     dnsmessage.TypeA,
	"AAAA":  dnsmessage.TypeAAAA,
	"CNAME": dnsmessage.TypeCNAME,
	"MX":    dnsmessage.TypeMX,
	"TXT":   dnsmessage.TypeTXT,
}

// rcodeNames are the conventional mnemonics, which read better in an error
// message than dnsmessage's Go identifiers.
var rcodeNames = map[dnsmessage.RCode]string{
	dnsmessage.RCodeFormatError:    "FORMERR",
	dnsmessage.RCodeServerFailure:  "SERVFAIL",
	dnsmessage.RCodeNameError:      "NXDOMAIN",
	dnsmessage.RCodeNotImplemented: "NOTIMP",
	dnsmessage.RCodeRefused:        "REFUSED",
}

// errForeignResponse marks a packet that is not the answer to the query in
// flight: garbage, a late reply to an earlier query, or a spoofing attempt.
var errForeignResponse = errors.New("response does not match query")

// errDNSTimeout is reported when an attempt got no valid answer in time.
var errDNSTimeout = errors.New("timeout")

func newDNSPinger(target TargetConfig) *dnsPinger {
	timeout := target.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	recordType := strings.ToUpper(target.DNSRecordType)
	if recordType == "" {
		recordType = "A"
	}
	fqdn := target.DNSQuery
	if !strings.HasSuffix(fqdn, ".") {
		fqdn += "."
	}
	return &dnsPinger{
		// Userinfo means nothing to DNS and must not reach error messages.
		server:     dnsServerAddr(redactEndpoint(target.Endpoint)),
		query:      target.DNSQuery,
		fqdn:       fqdn,
		recordType: recordType,
		timeout:    timeout,
	}
}

// dnsServerAddr turns a configured DNS server into a dialable host:port,
// defaulting the port to 53. Testing for any colon to decide whether a port is
// present breaks every IPv6 address; SplitHostPort does not.
func dnsServerAddr(server string) string {
	if host, port, err := net.SplitHostPort(server); err == nil {
		return net.JoinHostPort(host, port)
	}
	return net.JoinHostPort(strings.TrimSuffix(strings.TrimPrefix(server, "["), "]"), "53")
}

// ping sends the query over UDP, retrying over TCP when the answer comes back
// truncated. A failed or negative answer is a measurement, not an error: only
// cancellation of ctx returns one.
func (p *dnsPinger) ping(ctx context.Context) (PingResult, error) {
	res := PingResult{QueryName: p.query, Method: MethodDNS}

	id := uint16(rand.Uint32())
	var q dnsmessage.Question
	name, err := dnsmessage.NewName(p.fqdn)
	var query []byte
	if err == nil {
		q = dnsmessage.Question{Name: name, Type: dnsRecordTypes[p.recordType], Class: dnsmessage.ClassINET}
		query, err = (&dnsmessage.Message{
			Header:    dnsmessage.Header{ID: id, RecursionDesired: true},
			Questions: []dnsmessage.Question{q},
		}).Pack()
	}
	if err != nil {
		res.ErrPhase, res.ErrMessage = "dns", fmt.Sprintf("invalid query name %q: %v", p.query, err)
		return res, nil
	}

	start := time.Now()
	h, found, err := p.exchange(ctx, "udp", query, q, id)
	if err == nil && h.Truncated {
		h, found, err = p.exchange(ctx, "tcp", query, q, id)
		if err != nil {
			err = fmt.Errorf("truncated and tcp retry failed: %w", err)
		}
	}
	res.QueryDuration = time.Since(start)
	if err != nil && ctx.Err() != nil {
		return PingResult{}, ctx.Err()
	}

	switch {
	case err != nil:
		res.ErrMessage = redactErr(err).Error()
	case h.RCode != dnsmessage.RCodeSuccess:
		res.ErrMessage = "rcode " + rcodeName(h.RCode)
	case !found:
		res.ErrMessage = fmt.Sprintf("no %s records in answer", p.recordType)
	default:
		res.QuerySuccess = true
		return res, nil
	}
	res.ErrPhase = "dns"
	return res, nil
}

// exchange sends query over network ("udp" or "tcp") on a fresh connection and
// waits up to the target timeout, bounded by ctx, for the answer to it.
func (p *dnsPinger) exchange(ctx context.Context, network string, query []byte, q dnsmessage.Question, id uint16) (dnsmessage.Header, bool, error) {
	actx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	h, found, err := roundTrip(actx, network, p.server, query, q, id)
	if err != nil && ctx.Err() == nil && actx.Err() != nil {
		err = errDNSTimeout
	}
	return h, found, err
}

func roundTrip(ctx context.Context, network, server string, query []byte, q dnsmessage.Question, id uint16) (dnsmessage.Header, bool, error) {
	var d net.Dialer
	conn, err := d.DialContext(ctx, network, server)
	if err != nil {
		return dnsmessage.Header{}, false, err
	}
	defer conn.Close()
	// Unblocks the read below when the attempt times out or the probe is
	// cancelled.
	stop := context.AfterFunc(ctx, func() { _ = conn.SetDeadline(time.Unix(1, 0)) })
	defer stop()

	if network == "tcp" {
		// On TCP each message is framed by a two-byte length (RFC 1035 4.2.2).
		if _, err = conn.Write(append(binary.BigEndian.AppendUint16(nil, uint16(len(query))), query...)); err != nil {
			return dnsmessage.Header{}, false, err
		}
		var size [2]byte
		if _, err = io.ReadFull(conn, size[:]); err != nil {
			return dnsmessage.Header{}, false, err
		}
		buf := make([]byte, binary.BigEndian.Uint16(size[:]))
		if _, err = io.ReadFull(conn, buf); err != nil {
			return dnsmessage.Header{}, false, err
		}
		return matchResponse(buf, id, q)
	}

	if _, err = conn.Write(query); err != nil {
		return dnsmessage.Header{}, false, err
	}
	buf := make([]byte, 65535)
	for {
		n, err := conn.Read(buf)
		if err != nil {
			return dnsmessage.Header{}, false, err
		}
		h, found, err := matchResponse(buf[:n], id, q)
		if errors.Is(err, errForeignResponse) {
			continue
		}
		return h, found, err
	}
}

// matchResponse checks that b answers the query identified by id and q, and
// reports whether its answer section holds a record of the requested type. A
// CNAME answer to an A query does not count: the name has no address unless
// the chain ends in one.
func matchResponse(b []byte, id uint16, q dnsmessage.Question) (dnsmessage.Header, bool, error) {
	var p dnsmessage.Parser
	h, err := p.Start(b)
	if err != nil || !h.Response || h.ID != id {
		return h, false, errForeignResponse
	}
	if h.Truncated {
		// Incomplete by definition, and some servers omit the question; the
		// caller retries over TCP.
		return h, false, nil
	}
	qs, err := p.AllQuestions()
	if err != nil || len(qs) != 1 || qs[0].Type != q.Type || qs[0].Class != q.Class ||
		!strings.EqualFold(qs[0].Name.String(), q.Name.String()) {
		return h, false, errForeignResponse
	}
	for {
		ah, err := p.AnswerHeader()
		if errors.Is(err, dnsmessage.ErrSectionDone) {
			return h, false, nil
		}
		if err != nil {
			return h, false, fmt.Errorf("malformed response: %w", err)
		}
		if ah.Type == q.Type {
			return h, true, nil
		}
		if err := p.SkipAnswer(); err != nil {
			return h, false, fmt.Errorf("malformed response: %w", err)
		}
	}
}

func rcodeName(r dnsmessage.RCode) string {
	if n, ok := rcodeNames[r]; ok {
		return n
	}
	return fmt.Sprintf("%d", uint16(r))
}
