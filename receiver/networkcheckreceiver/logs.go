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
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver/internal/metadata"
)

// appendTraceLog appends one record for a completed trace under its own
// resource. A trace that could not run has no path to record; it is reported
// in the scrape error or, for a triggered trace, the collector log.
func appendTraceLog(lb *metadata.LogsBuilder, rb *metadata.ResourceBuilder, res targetResult, trigger metadata.AttributeTracerouteTrigger, observed time.Time) {
	if res.err != nil {
		return
	}
	lr := plog.NewLogRecord()
	buildTraceLogRecord(lr, res.target, res.trace, trigger, res.startedAt, observed)
	lb.AppendLogRecord(lr)
	rb.SetServerAddress(res.target.host)
	lb.EmitForResource(metadata.WithLogsResource(rb.Emit()))
}

// buildTraceLogRecord renders one trace as a single record. The path is the
// unit of meaning, so hops stay together and ordered, including hops that
// never answered, and a route change is a difference between two records.
func buildTraceLogRecord(lr plog.LogRecord, t *target, tr TraceResult, trigger metadata.AttributeTracerouteTrigger, startedAt, observed time.Time) {
	lr.SetTimestamp(pcommon.NewTimestampFromTime(startedAt))
	lr.SetObservedTimestamp(pcommon.NewTimestampFromTime(observed))

	answered, retried := 0, 0
	for _, h := range tr.Hops {
		if !h.TimedOut {
			answered++
		}
		if h.Probes > 1 {
			retried++
		}
	}

	// A trace that reached its destination is routine. One that gave up early,
	// or walked to the TTL ceiling without arriving, is a path problem worth
	// surfacing without making it an error.
	if tr.Reached {
		lr.SetSeverityNumber(plog.SeverityNumberInfo)
		lr.SetSeverityText("INFO")
	} else {
		lr.SetSeverityNumber(plog.SeverityNumberWarn)
		lr.SetSeverityText("WARN")
	}

	attrs := lr.Attributes()
	attrs.EnsureCapacity(10)
	attrs.PutStr("server.address", t.host)
	attrs.PutStr("traceroute.trigger", trigger.String())
	if tr.DestIP != "" {
		attrs.PutStr("server.resolved_ip", tr.DestIP)
	}
	if tr.Method != "" {
		attrs.PutStr("traceroute.method", tr.Method)
	}
	attrs.PutInt("traceroute.hop_count", int64(len(tr.Hops)))
	attrs.PutInt("traceroute.hops_answered", int64(answered))
	attrs.PutInt("traceroute.hops_retried", int64(retried))
	attrs.PutBool("traceroute.reached_dest", tr.Reached)
	attrs.PutBool("traceroute.aborted_early", tr.AbortedEarly)
	if t.dnsServer != "" {
		attrs.PutStr("dns.server", t.dnsServer)
	}

	hops := lr.Body().SetEmptyMap().PutEmptySlice("hops")
	hops.EnsureCapacity(len(tr.Hops))
	for _, h := range tr.Hops {
		m := hops.AppendEmpty().SetEmptyMap()
		m.EnsureCapacity(5)
		m.PutInt("index", int64(h.Index))
		m.PutStr("address", h.Address)
		m.PutBool("timed_out", h.TimedOut)
		m.PutInt("probes", int64(h.Probes))
		// A hop that did not answer has no latency, only the timeout we chose.
		if !h.TimedOut {
			m.PutDouble("rtt_ms", msFloat(h.RTT))
		}
	}
}
