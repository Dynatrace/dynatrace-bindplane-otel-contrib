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
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/receiver/receivertest"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/networkcheckreceiver/internal/metadata"
)

func TestTraceLogHopsRetriedCountsAnsweredHopsOnly(t *testing.T) {
	lb := metadata.NewLogsBuilder(receivertest.NewNopSettings(metadata.Type))
	res := targetResult{
		target:    &target{host: "h"},
		startedAt: time.Now(),
		trace: TraceResult{Hops: []HopResult{
			{Index: 1, Address: "10.0.0.1", RTT: time.Millisecond, Probes: 1},
			{Index: 2, Address: "10.0.0.2", RTT: time.Millisecond, Probes: 3}, // answered after retries
			{Index: 3, Address: "*", TimedOut: true, Probes: 3},               // silent: not a retry
		}},
	}
	appendTraceLog(lb, metadata.NewResourceBuilder(metadata.DefaultResourceAttributesConfig()), res, metadata.AttributeTracerouteTriggerScheduled, time.Now())
	rec := lb.Emit().ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	v, ok := rec.Attributes().Get("traceroute.hops_retried")
	require.True(t, ok)
	require.EqualValues(t, 1, v.Int())
}

func TestTraceLogRecord(t *testing.T) {
	emit := func(res targetResult) plog.Logs {
		lb := metadata.NewLogsBuilder(receivertest.NewNopSettings(metadata.Type))
		appendTraceLog(lb, metadata.NewResourceBuilder(metadata.DefaultResourceAttributesConfig()), res, metadata.AttributeTracerouteTriggerScheduled, time.Now())
		return lb.Emit()
	}
	record := func(t *testing.T, tr TraceResult) plog.LogRecord {
		t.Helper()
		logs := emit(targetResult{target: &target{host: "h"}, startedAt: time.Now(), trace: tr})
		require.Equal(t, 1, logs.LogRecordCount())
		return logs.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0)
	}

	t.Run("reached is INFO", func(t *testing.T) {
		lr := record(t, TraceResult{Reached: true, Hops: []HopResult{{Index: 1, Address: "192.0.2.1", RTT: time.Millisecond, Probes: 1}}})
		require.Equal(t, "INFO", lr.SeverityText())
		require.Equal(t, plog.SeverityNumberInfo, lr.SeverityNumber())
		_, ok := lr.Attributes().Get("traceroute.unreachable")
		require.False(t, ok, "set only when a hop said unreachable")
	})
	t.Run("unreachable is WARN and flagged", func(t *testing.T) {
		lr := record(t, TraceResult{Unreachable: true, Hops: []HopResult{{Index: 1, Address: "10.0.0.1", RTT: time.Millisecond, Probes: 1}}})
		require.Equal(t, "WARN", lr.SeverityText())
		require.Equal(t, plog.SeverityNumberWarn, lr.SeverityNumber())
		v, ok := lr.Attributes().Get("traceroute.unreachable")
		require.True(t, ok)
		require.True(t, v.Bool())
	})
	t.Run("no hops", func(t *testing.T) {
		lr := record(t, TraceResult{})
		require.Equal(t, "WARN", lr.SeverityText())
		n, _ := lr.Attributes().Get("traceroute.hop_count")
		require.Zero(t, n.Int())
		hops, ok := lr.Body().Map().Get("hops")
		require.True(t, ok, "the hops list is present even when empty")
		require.Zero(t, hops.Slice().Len())
	})
	t.Run("a trace that could not run has no record", func(t *testing.T) {
		logs := emit(targetResult{target: &target{host: "h"}, err: errFake})
		require.Zero(t, logs.LogRecordCount())
	})
}
