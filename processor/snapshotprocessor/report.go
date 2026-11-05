// Copyright Dynatrace LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package snapshotprocessor

import (
	"encoding/json"
	"time"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/pkg/snapshot"
	"go.uber.org/zap"
)

type snapshotReport struct {
	SessionID     string `json:"session_id"`
	TelemetryType string `json:"telemetry_type"`
	// TelemetryPayload is the logs/metrics/traces in OTLP/JSON format
	TelemetryPayload json.RawMessage `json:"telemetry_payload"`
	// Metadata is omitted from the wire format when nil so older decoders
	// keep working unchanged.
	Metadata *snapshotReportMetadata `json:"metadata,omitempty"`
}

// snapshotReportMetadata tells the server how the request was served, so it can
// see which mode the buffer is in and how long a just-in-time collection took.
type snapshotReportMetadata struct {
	Mode   string `json:"mode"`   // "continuous" or "on_demand"
	Filled bool   `json:"filled"` // false: the on-demand fill wait expired and the last known records were served
	// Timings are fractional milliseconds: a continuous-mode request is
	// usually served in well under one.
	FillWaitMs float64 `json:"fill_wait_ms"`
	DurationMs float64 `json:"duration_ms"`
	Records    int     `json:"records"`
}

func newReportMetadata(stats snapshot.RequestStats) *snapshotReportMetadata {
	return &snapshotReportMetadata{
		Mode:       string(stats.Mode),
		Filled:     stats.Filled,
		FillWaitMs: millis(stats.FillWait),
		DurationMs: millis(stats.Duration),
		Records:    stats.Records,
	}
}

func millis(d time.Duration) float64 {
	return float64(d) / float64(time.Millisecond)
}

// newReport builds the reportSnapshot message for one request. telemetryType
// is the request's pipeline type: "logs", "metrics" or "traces".
func newReport(sessionID, telemetryType string, payload []byte, stats snapshot.RequestStats) snapshotReport {
	return snapshotReport{
		SessionID:        sessionID,
		TelemetryType:    telemetryType,
		TelemetryPayload: payload,
		Metadata:         newReportMetadata(stats),
	}
}

// logFields describes the report for the processor's logs.
func (r snapshotReport) logFields() []zap.Field {
	return []zap.Field{
		zap.String("session", r.SessionID),
		zap.String("telemetry_type", r.TelemetryType),
		zap.String("mode", r.Metadata.Mode),
		zap.Bool("filled", r.Metadata.Filled),
		zap.Float64("fill_wait_ms", r.Metadata.FillWaitMs),
		zap.Float64("duration_ms", r.Metadata.DurationMs),
		zap.Int("records", r.Metadata.Records),
	}
}
