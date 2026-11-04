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
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/opampcustommessages"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/golden"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/pkg/snapshot"
)

func TestProcess_Logs(t *testing.T) {
	factory := NewFactory()
	sink := &consumertest.LogsSink{}

	pSet := processortest.NewNopSettings(componentType)
	p, err := factory.CreateLogs(context.Background(), pSet, factory.CreateDefaultConfig(), sink)
	require.NoError(t, err)

	mockOpamp := &mockOpAMPExtension{
		msgChan: make(chan *protobufs.CustomMessage, 1),
	}

	mockHost := &mockHost{
		extensions: map[component.ID]component.Component{
			component.MustNewID("opamp"): mockOpamp,
		},
	}

	require.NoError(t, p.Start(context.Background(), mockHost))
	t.Cleanup(func() {
		require.NoError(t, p.Shutdown(context.Background()))
	})

	require.Equal(t, "com.bindplane.snapshot", mockOpamp.capability)

	l, err := golden.ReadLogs(filepath.Join("testdata", "logs", "w3c-logs.yaml"))
	require.NoError(t, err)

	require.NoError(t, p.ConsumeLogs(context.Background(), l))

	require.Equal(t, 1, len(sink.AllLogs()))
	require.Equal(t, l, sink.AllLogs()[0])

	// Request buffer
	reqPayload := fmt.Sprintf(`{"processor":%q,"pipeline_type":"logs","session_id":"my-session-id","maximum_payload_size":100000}`, pSet.ID)

	cm := &protobufs.CustomMessage{
		Capability: "com.bindplane.snapshot",
		Type:       "requestSnapshot",
		Data:       []byte(reqPayload),
	}

	mockOpamp.msgChan <- cm

	// Wait for response
	require.Eventually(t, func() bool {
		return mockOpamp.GotMessage()
	}, 5*time.Second, 100*time.Millisecond)

	by, err := os.ReadFile(filepath.Join("testdata", "snapshot", "logs-report.json"))
	require.NoError(t, err)

	var expectedMessageContents map[string]any
	err = json.Unmarshal(by, &expectedMessageContents)
	require.NoError(t, err)

	raw := gunzipBytes(t, mockOpamp.sentMessage)
	var actualMessageContents map[string]any
	err = json.Unmarshal(raw, &actualMessageContents)
	require.NoError(t, err)

	// The wire format is exactly these keys, and a decoder that predates
	// metadata still reads the report.
	require.Equal(t, []string{"metadata", "session_id", "telemetry_payload", "telemetry_type"}, slices.Sorted(maps.Keys(actualMessageContents)))
	var legacy struct {
		SessionID        string          `json:"session_id"`
		TelemetryType    string          `json:"telemetry_type"`
		TelemetryPayload json.RawMessage `json:"telemetry_payload"`
	}
	require.NoError(t, json.Unmarshal(raw, &legacy))
	require.Equal(t, "my-session-id", legacy.SessionID)
	require.Equal(t, "logs", legacy.TelemetryType)
	require.NotEmpty(t, legacy.TelemetryPayload)

	var report snapshotReport
	require.NoError(t, json.Unmarshal(raw, &report))
	payload, err := (&plog.JSONUnmarshaler{}).UnmarshalLogs(report.TelemetryPayload)
	require.NoError(t, err)
	requireContinuousMetadata(t, report.Metadata, payload.LogRecordCount())

	// The golden file holds the payload only; metadata timings vary per run.
	delete(actualMessageContents, "metadata")
	require.Equal(t, expectedMessageContents, actualMessageContents)
	require.Equal(t, "reportSnapshot", mockOpamp.sentMessageType)
}

func TestProcess_Metrics(t *testing.T) {
	factory := NewFactory()
	sink := &consumertest.MetricsSink{}

	pSet := processortest.NewNopSettings(componentType)
	p, err := factory.CreateMetrics(context.Background(), pSet, factory.CreateDefaultConfig(), sink)
	require.NoError(t, err)

	mockOpamp := &mockOpAMPExtension{
		msgChan: make(chan *protobufs.CustomMessage, 1),
	}

	mockHost := &mockHost{
		extensions: map[component.ID]component.Component{
			component.MustNewID("opamp"): mockOpamp,
		},
	}

	require.NoError(t, p.Start(context.Background(), mockHost))
	t.Cleanup(func() {
		require.NoError(t, p.Shutdown(context.Background()))
	})

	require.Equal(t, "com.bindplane.snapshot", mockOpamp.capability)

	m, err := golden.ReadMetrics(filepath.Join("testdata", "metrics", "host-metrics.yaml"))
	require.NoError(t, err)

	require.NoError(t, p.ConsumeMetrics(context.Background(), m))

	require.Equal(t, 1, len(sink.AllMetrics()))
	require.Equal(t, m, sink.AllMetrics()[0])

	// Request buffer
	reqPayload := fmt.Sprintf(`{"processor":%q,"pipeline_type":"metrics","session_id":"my-session-id","maximum_payload_size":100000}`, pSet.ID)

	cm := &protobufs.CustomMessage{
		Capability: "com.bindplane.snapshot",
		Type:       "requestSnapshot",
		Data:       []byte(reqPayload),
	}

	mockOpamp.msgChan <- cm

	// Wait for response
	require.Eventually(t, func() bool {
		return mockOpamp.GotMessage()
	}, 5*time.Second, 100*time.Millisecond)

	by, err := os.ReadFile(filepath.Join("testdata", "snapshot", "metrics-report.json"))
	require.NoError(t, err)

	var expectedMessageContents map[string]any
	err = json.Unmarshal(by, &expectedMessageContents)
	require.NoError(t, err)

	raw := gunzipBytes(t, mockOpamp.sentMessage)
	var actualMessageContents map[string]any
	err = json.Unmarshal(raw, &actualMessageContents)
	require.NoError(t, err)

	var report snapshotReport
	require.NoError(t, json.Unmarshal(raw, &report))
	payload, err := (&pmetric.JSONUnmarshaler{}).UnmarshalMetrics(report.TelemetryPayload)
	require.NoError(t, err)
	requireContinuousMetadata(t, report.Metadata, payload.DataPointCount())

	// The golden file holds the payload only; metadata timings vary per run.
	delete(actualMessageContents, "metadata")
	require.Equal(t, expectedMessageContents, actualMessageContents)
	require.Equal(t, "reportSnapshot", mockOpamp.sentMessageType)
}

func TestProcess_Traces(t *testing.T) {
	factory := NewFactory()
	sink := &consumertest.TracesSink{}

	pSet := processortest.NewNopSettings(componentType)
	p, err := factory.CreateTraces(context.Background(), pSet, factory.CreateDefaultConfig(), sink)
	require.NoError(t, err)

	mockOpamp := &mockOpAMPExtension{
		msgChan: make(chan *protobufs.CustomMessage, 1),
	}

	mockHost := &mockHost{
		extensions: map[component.ID]component.Component{
			component.MustNewID("opamp"): mockOpamp,
		},
	}

	require.NoError(t, p.Start(context.Background(), mockHost))
	t.Cleanup(func() {
		require.NoError(t, p.Shutdown(context.Background()))
	})

	require.Equal(t, "com.bindplane.snapshot", mockOpamp.capability)

	tr, err := golden.ReadTraces(filepath.Join("testdata", "traces", "bindplane-traces.yaml"))
	require.NoError(t, err)

	require.NoError(t, p.ConsumeTraces(context.Background(), tr))

	require.Equal(t, 1, len(sink.AllTraces()))
	require.Equal(t, tr, sink.AllTraces()[0])

	// Request buffer
	reqPayload := fmt.Sprintf(`{"processor":%q,"pipeline_type":"traces","session_id":"my-session-id","maximum_payload_size":100000}`, pSet.ID)

	cm := &protobufs.CustomMessage{
		Capability: "com.bindplane.snapshot",
		Type:       "requestSnapshot",
		Data:       []byte(reqPayload),
	}

	mockOpamp.msgChan <- cm

	// Wait for response
	require.Eventually(t, func() bool {
		return mockOpamp.GotMessage()
	}, 5*time.Second, 100*time.Millisecond)

	by, err := os.ReadFile(filepath.Join("testdata", "snapshot", "traces-report.json"))
	require.NoError(t, err)

	var expectedMessageContents map[string]any
	err = json.Unmarshal(by, &expectedMessageContents)
	require.NoError(t, err)

	raw := gunzipBytes(t, mockOpamp.sentMessage)
	var actualMessageContents map[string]any
	err = json.Unmarshal(raw, &actualMessageContents)
	require.NoError(t, err)

	var report snapshotReport
	require.NoError(t, json.Unmarshal(raw, &report))
	payload, err := (&ptrace.JSONUnmarshaler{}).UnmarshalTraces(report.TelemetryPayload)
	require.NoError(t, err)
	requireContinuousMetadata(t, report.Metadata, payload.SpanCount())

	// The golden file holds the payload only; metadata timings vary per run.
	delete(actualMessageContents, "metadata")
	require.Equal(t, expectedMessageContents, actualMessageContents)
	require.Equal(t, "reportSnapshot", mockOpamp.sentMessageType)
}

func TestProcess_Metrics_PreservesTemporalityWithFiltering(t *testing.T) {
	factory := NewFactory()
	sink := &consumertest.MetricsSink{}

	pSet := processortest.NewNopSettings(componentType)
	p, err := factory.CreateMetrics(context.Background(), pSet, factory.CreateDefaultConfig(), sink)
	require.NoError(t, err)

	mockOpamp := &mockOpAMPExtension{
		msgChan: make(chan *protobufs.CustomMessage, 1),
	}

	mockHost := &mockHost{
		extensions: map[component.ID]component.Component{
			component.MustNewID("opamp"): mockOpamp,
		},
	}

	require.NoError(t, p.Start(context.Background(), mockHost))
	t.Cleanup(func() {
		require.NoError(t, p.Shutdown(context.Background()))
	})

	// Load test metrics with different aggregation temporalities
	m, err := golden.ReadMetrics(filepath.Join("testdata", "metrics", "temporality-metrics.yaml"))
	require.NoError(t, err)

	require.NoError(t, p.ConsumeMetrics(context.Background(), m))

	// Request buffer with search query (this triggers the filtering code path where the bug occurred)
	reqPayload := fmt.Sprintf(`{"processor":%q,"pipeline_type":"metrics","session_id":"filtering-test","search_query":"transmit","maximum_payload_size":100000}`, pSet.ID)

	cm := &protobufs.CustomMessage{
		Capability: "com.bindplane.snapshot",
		Type:       "requestSnapshot",
		Data:       []byte(reqPayload),
	}

	mockOpamp.msgChan <- cm

	// Wait for response
	require.Eventually(t, func() bool {
		return mockOpamp.GotMessage()
	}, 5*time.Second, 100*time.Millisecond)

	// Parse the actual response
	var actualMessageContents map[string]any
	err = json.Unmarshal(gunzipBytes(t, mockOpamp.sentMessage), &actualMessageContents)
	require.NoError(t, err)

	// Verify filtering worked and only "transmit" metric is present
	telemetryPayload := actualMessageContents["telemetry_payload"].(map[string]any)
	resourceMetrics := telemetryPayload["resourceMetrics"].([]any)
	require.Len(t, resourceMetrics, 1, "Should have one resource metric after filtering")

	firstResource := resourceMetrics[0].(map[string]any)
	scopeMetrics := firstResource["scopeMetrics"].([]any)
	require.Len(t, scopeMetrics, 1, "Should have one scope metric")

	firstScope := scopeMetrics[0].(map[string]any)
	metrics := firstScope["metrics"].([]any)
	require.Len(t, metrics, 1, "Should have one metric matching 'transmit' filter")

	// Verify the filtered metric is the correct one and has preserved aggregation temporality
	filteredMetric := metrics[0].(map[string]any)
	require.Equal(t, "system.network.io", filteredMetric["name"])

	sum := filteredMetric["sum"].(map[string]any)
	require.Equal(t, float64(2), sum["aggregationTemporality"], "Aggregation temporality should be preserved as Cumulative (2) even after filtering")
	require.Equal(t, true, sum["isMonotonic"], "IsMonotonic should be preserved after filtering")

	// Verify the data point attributes contain "transmit"
	dataPoints := sum["dataPoints"].([]any)
	require.Len(t, dataPoints, 1, "Should have one data point")

	dataPoint := dataPoints[0].(map[string]any)
	attributes := dataPoint["attributes"].([]any)
	foundTransmit := false
	for _, attrAny := range attributes {
		attr := attrAny.(map[string]any)
		if attr["key"] == "direction" {
			value := attr["value"].(map[string]any)
			if value["stringValue"] == "transmit" {
				foundTransmit = true
				break
			}
		}
	}
	require.True(t, foundTransmit, "Filtered metric should contain 'transmit' attribute")
}

// requireContinuousMetadata asserts the metadata of a report served from a
// buffer that was collecting between requests.
func requireContinuousMetadata(t *testing.T, m *snapshotReportMetadata, records int) {
	t.Helper()
	require.NotNil(t, m)
	require.Equal(t, "continuous", m.Mode)
	require.True(t, m.Filled)
	require.Zero(t, m.FillWaitMs)
	require.GreaterOrEqual(t, m.DurationMs, float64(0))
	require.Equal(t, records, m.Records)
}

// TestReportMetadata checks the metadata for a request that waited the whole
// fill wait and was served the last known records, and that sub-millisecond
// timings are kept rather than truncated.
func TestReportMetadata(t *testing.T) {
	m := newReportMetadata(snapshot.RequestStats{
		Mode:     snapshot.ModeOnDemand,
		Filled:   false,
		FillWait: 1002 * time.Millisecond,
		Duration: 1002*time.Millisecond + 312*time.Microsecond,
		Records:  100,
	})
	require.Equal(t, "on_demand", m.Mode)
	require.False(t, m.Filled)
	require.InDelta(t, 1002, m.FillWaitMs, 1e-9)
	require.InDelta(t, 1002.312, m.DurationMs, 1e-9)
	require.Equal(t, 100, m.Records)

	m = newReportMetadata(snapshot.RequestStats{Mode: snapshot.ModeContinuous, Filled: true, Duration: 312 * time.Microsecond, Records: 7})
	require.Zero(t, m.FillWaitMs)
	require.InDelta(t, 0.312, m.DurationMs, 1e-9)
}

// mockHost for component.Host
type mockHost struct {
	extensions map[component.ID]component.Component
}

func (nh *mockHost) GetFactory(component.Kind, component.Type) component.Factory {
	return nil
}

func (nh *mockHost) GetExtensions() map[component.ID]component.Component {
	return nh.extensions
}

type mockOpAMPExtension struct {
	msgChan chan *protobufs.CustomMessage

	capability string

	gotMessageMux   sync.Mutex
	gotMessage      bool
	sentMessageType string
	sentMessage     []byte
}

// Start implements component.Component::Start
func (m *mockOpAMPExtension) Start(_ context.Context, _ component.Host) error {
	return nil
}

// Shutdown implements component.Component::Shutdown
func (m *mockOpAMPExtension) Shutdown(_ context.Context) error { return nil }

func (m *mockOpAMPExtension) Register(capability string, _ ...opampcustommessages.CustomCapabilityRegisterOption) (handler opampcustommessages.CustomCapabilityHandler, err error) {
	m.capability = capability
	return m, nil
}

func (m *mockOpAMPExtension) Message() <-chan *protobufs.CustomMessage {
	return m.msgChan
}

func (m *mockOpAMPExtension) SendMessage(messageType string, message []byte) (messageSendingChannel chan struct{}, err error) {
	m.gotMessageMux.Lock()
	defer m.gotMessageMux.Unlock()

	if m.gotMessage {
		return
	}
	m.gotMessage = true

	m.sentMessageType = messageType
	m.sentMessage = message
	return
}

func (m *mockOpAMPExtension) GotMessage() bool {
	m.gotMessageMux.Lock()
	defer m.gotMessageMux.Unlock()

	return m.gotMessage
}

func (m *mockOpAMPExtension) Unregister() {}

func gunzipBytes(t *testing.T, b []byte) []byte {
	t.Helper()

	r, err := gzip.NewReader(bytes.NewBuffer(b))
	require.NoError(t, err)
	bOut, err := io.ReadAll(r)
	require.NoError(t, err)

	return bOut
}
