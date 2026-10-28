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

package exabeamexporter // import "github.com/dynatrace/dynatrace-bindplane-otel-contrib/exporter/exabeamexporter"

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/exporter/exabeamexporter/internal/metadata"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/plog"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.uber.org/zap"
)

// benchmarkRecords is the number of log records in each benchmarked batch.
const benchmarkRecords = 10_000

var benchmarkWords = strings.Fields(`connection accepted closed session opened for user from port request
	completed failed timeout retry upstream backend cache miss hit token expired refreshed policy denied
	allowed firewall rule matched src dst proto tcp udp bytes packets duration status ok error warning`)

// benchmarkLine builds a syslog-like line of about size bytes. The content varies per
// record so compression costs and ratios resemble real logs rather than repeated padding.
func benchmarkLine(r *rand.Rand, seq, size int) string {
	var b strings.Builder
	b.Grow(size + 32)
	fmt.Fprintf(&b, "seq=%d host-%02d sshd[%d]: Failed password for user%d from 10.%d.%d.%d port %d ssh2 req=%016x",
		seq, r.IntN(50), 1000+r.IntN(60000), r.IntN(8), r.IntN(256), r.IntN(256), r.IntN(256), 1024+r.IntN(64000), r.Uint64())
	for b.Len() < size {
		b.WriteByte(' ')
		if r.IntN(5) == 0 {
			fmt.Fprintf(&b, "%d", r.IntN(100000))
		} else {
			b.WriteString(benchmarkWords[r.IntN(len(benchmarkWords))])
		}
	}
	return b.String()
}

func benchmarkLogs(recordSize int) plog.Logs {
	r := rand.New(rand.NewPCG(1, 2))
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	rl.Resource().Attributes().PutStr("host.name", "bench-host")
	lrs := rl.ScopeLogs().AppendEmpty().LogRecords()
	for i := range benchmarkRecords {
		lr := lrs.AppendEmpty()
		lr.Body().SetStr(benchmarkLine(r, i, recordSize))
		lr.Attributes().PutStr("log.type", "bench")
	}
	return ld
}

func newBenchTelemetry() (*metadata.TelemetryBuilder, error) {
	return metadata.NewTelemetryBuilder(component.TelemetrySettings{MeterProvider: sdkmetric.NewMeterProvider()})
}

// discardUploader accepts every file without doing anything with it.
type discardUploader struct{}

func (discardUploader) PutObject(context.Context, string, []byte) error { return nil }

func BenchmarkEncode(b *testing.B) {
	for _, size := range []int{200, 1024, 8192} {
		b.Run(fmt.Sprintf("record=%dB", size), func(b *testing.B) {
			enc := newTestEncoder(defaultMaxFileSize)
			ld := benchmarkLogs(size)
			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := enc.encode(context.Background(), ld); err != nil {
					b.Fatal(err)
				}
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*benchmarkRecords), "ns/record")
		})
	}
}

func BenchmarkEncodeRawLogField(b *testing.B) {
	cfg := createDefaultConfig().(*Config)
	cfg.RawLogField = "body"
	enc, err := newEncoder(cfg, connectionSettings{CollectorID: testCollectorID}, componenttest.NewNopTelemetrySettings())
	if err != nil {
		b.Fatal(err)
	}
	ld := benchmarkLogs(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := enc.encode(context.Background(), ld); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*benchmarkRecords), "ns/record")
}

func BenchmarkLogsDataPusher(b *testing.B) {
	tb, err := newBenchTelemetry()
	if err != nil {
		b.Fatal(err)
	}
	exp := &exabeamExporter{
		settings:  testSettings(),
		uploader:  discardUploader{},
		encoder:   newTestEncoder(defaultMaxFileSize),
		logger:    zap.NewNop(),
		telemetry: tb,
	}
	ld := benchmarkLogs(1024)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if err := exp.logsDataPusher(context.Background(), ld); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*benchmarkRecords), "ns/record")
}
