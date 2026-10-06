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
