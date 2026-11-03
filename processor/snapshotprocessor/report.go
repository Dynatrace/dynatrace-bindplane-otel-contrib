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

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/pkg/snapshot"
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
	Mode       string `json:"mode"`   // "continuous" or "on_demand"
	Filled     bool   `json:"filled"` // false: the on-demand fill wait expired and the last known records were served
	FillWaitMs int64  `json:"fill_wait_ms"`
	DurationMs int64  `json:"duration_ms"`
	Records    int    `json:"records"`
}

func newReportMetadata(stats snapshot.RequestStats) *snapshotReportMetadata {
	return &snapshotReportMetadata{
		Mode:       string(stats.Mode),
		Filled:     stats.Filled,
		FillWaitMs: stats.FillWait.Milliseconds(),
		DurationMs: stats.Duration.Milliseconds(),
		Records:    stats.Records,
	}
}

func logsReport(sessionID string, logsPayload []byte, stats snapshot.RequestStats) snapshotReport {
	return snapshotReport{
		SessionID:        sessionID,
		TelemetryType:    "logs",
		TelemetryPayload: logsPayload,
		Metadata:         newReportMetadata(stats),
	}
}

func metricsReport(sessionID string, metricsPayload []byte, stats snapshot.RequestStats) snapshotReport {
	return snapshotReport{
		SessionID:        sessionID,
		TelemetryType:    "metrics",
		TelemetryPayload: metricsPayload,
		Metadata:         newReportMetadata(stats),
	}
}

func tracesReport(sessionID string, tracesPayload []byte, stats snapshot.RequestStats) snapshotReport {
	return snapshotReport{
		SessionID:        sessionID,
		TelemetryType:    "traces",
		TelemetryPayload: tracesPayload,
		Metadata:         newReportMetadata(stats),
	}
}
