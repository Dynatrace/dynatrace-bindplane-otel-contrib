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
	"net"
	"strings"
	"time"
)

// dnsPinger sends a DNS query to a specific server and measures response time.
type dnsPinger struct {
	server     string // DNS server address with port, e.g. "8.8.8.8:53"
	query      string // hostname to resolve
	recordType string // "A", "AAAA", "CNAME", "MX", "TXT"
	timeout    time.Duration
}

func newDNSPinger(target TargetConfig) *dnsPinger {
	timeout := target.Timeout
	if timeout == 0 {
		timeout = 5 * time.Second
	}
	server := target.Endpoint
	if !strings.Contains(server, ":") {
		server = server + ":53"
	}
	recordType := strings.ToUpper(target.DNSRecordType)
	if recordType == "" {
		recordType = "A"
	}
	return &dnsPinger{
		server:     server,
		query:      target.DNSQuery,
		recordType: recordType,
		timeout:    timeout,
	}
}

func (p *dnsPinger) ping(ctx context.Context) (PingResult, error) {
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(dialCtx context.Context, _, _ string) (net.Conn, error) {
			d := net.Dialer{Timeout: p.timeout}
			return d.DialContext(dialCtx, "udp", p.server)
		},
	}

	queryCtx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()

	start := time.Now()
	var lookupErr error
	switch p.recordType {
	case "CNAME":
		_, lookupErr = resolver.LookupCNAME(queryCtx, p.query)
	case "MX":
		_, lookupErr = resolver.LookupMX(queryCtx, p.query)
	case "TXT":
		_, lookupErr = resolver.LookupTXT(queryCtx, p.query)
	default: // A, AAAA
		_, lookupErr = resolver.LookupHost(queryCtx, p.query)
	}
	duration := time.Since(start)

	return PingResult{
		QueryDuration: duration,
		QuerySuccess:  lookupErr == nil,
		QueryName:     p.query,
		Method:        MethodDNS,
	}, nil
}
