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
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

var testWriteTime = time.Date(2023, 4, 19, 11, 38, 12, 501_999_999, time.FixedZone("x", -7*3600))

// failingRawLogField returns a slice for a string attributes["raw"] and fails at runtime for any other type.
const failingRawLogField = `Split(attributes["raw"], ",")`

func testRecordMetadata() recordMetadata {
	return recordMetadata{
		CollectorName:  "bindplane-gateway",
		CollectorID:    testCollectorID,
		SiteName:       "abcorp",
		SiteID:         "site-1",
		TimezoneOffset: "-0700",
		Forwarder:      "bindplane",
		LogTags:        map[string]string{"department": "HR", "servertype": "PCI"},
	}
}

func newTestEncoder(maxFileSize int64) *encoder {
	return &encoder{
		meta:        testRecordMetadata(),
		maxFileSize: maxFileSize,
		now:         func() time.Time { return testWriteTime },
		logger:      zap.NewNop(),
	}
}

// newTestEncoderWithConfig builds an encoder through newEncoder from a config.
func newTestEncoderWithConfig(t *testing.T, mutate func(*Config)) *encoder {
	t.Helper()
	cfg := createDefaultConfig().(*Config)
	cfg.ConnectionString = testConnectionStringOpaque(t)
	if mutate != nil {
		mutate(cfg)
	}
	require.NoError(t, cfg.Validate())
	s, err := cfg.resolve()
	require.NoError(t, err)
	enc, err := newEncoder(cfg, s, componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	enc.now = func() time.Time { return testWriteTime }
	return enc
}

// gunzipLines decompresses a file and returns its lines without the trailing newline.
func gunzipLines(t *testing.T, file []byte) []string {
	t.Helper()
	zr, err := gzip.NewReader(bytes.NewReader(file))
	require.NoError(t, err)
	raw, err := io.ReadAll(zr)
	require.NoError(t, err)
	require.True(t, bytes.HasSuffix(raw, []byte("\n")), "file must end with a newline")

	var lines []string
	sc := bufio.NewScanner(bytes.NewReader(raw))
	sc.Buffer(make([]byte, 0, 1024), 64*1024*1024)
	for sc.Scan() {
		lines = append(lines, sc.Text())
	}
	require.NoError(t, sc.Err())
	return lines
}

// decodeRecord checks a line is a {"message": {...}} envelope and returns the inner record.
func decodeRecord(t *testing.T, line string) map[string]any {
	t.Helper()
	var envelope map[string]map[string]any
	require.NoError(t, json.Unmarshal([]byte(line), &envelope))
	require.Len(t, envelope, 1, "the envelope has only a message key")
	rec, ok := envelope["message"]
	require.True(t, ok)
	return rec
}

// encodeOne encodes a single log record built by setup and returns the decoded record.
func encodeOne(t *testing.T, enc *encoder, setup func(rl plog.ResourceLogs, lr plog.LogRecord)) (map[string]any, string) {
	t.Helper()
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	setup(rl, lr)

	files, err := enc.encode(context.Background(), ld)
	require.NoError(t, err)
	require.Len(t, files, 1)
	lines := gunzipLines(t, files[0].data)
	require.Len(t, lines, 1)
	return decodeRecord(t, lines[0]), lines[0]
}

func TestEncoderRecordFormat(t *testing.T) {
	raw := `<34>1 2003-10-11T22:14:15.003Z host app - ID47 - "quoted" & <tagged> \u00e9`
	rec, line := encodeOne(t, newTestEncoder(defaultMaxFileSize), func(rl plog.ResourceLogs, lr plog.LogRecord) {
		rl.Resource().Attributes().PutStr("host.name", "web-01")
		lr.Body().SetStr(raw)
	})

	require.Equal(t, map[string]any{
		"@timestamp":      "2023-04-19T18:38:12.501Z",
		"collector_type":  "cribl-logs",
		"collector_name":  "bindplane-gateway",
		"collector_id":    testCollectorID,
		"message":         raw,
		"timezone_offset": "-0700",
		"hostname":        "web-01",
		"forwarder":       "bindplane",
		"siteid":          "site-1",
		"sitename":        "abcorp",
		"logtags":         map[string]any{"department": "HR", "servertype": "PCI"},
	}, rec)

	// Field order follows Exabeam's sample.
	require.True(t, strings.HasPrefix(line, `{"message":{"@timestamp":"2023-04-19T18:38:12.501Z","collector_type":"cribl-logs",`), line)
	keys := []string{"@timestamp", "collector_type", "collector_name", "collector_id", "message", "timezone_offset", "hostname", "forwarder", "siteid", "sitename", "logtags"}
	inner := strings.TrimPrefix(line, `{"message":`)
	last := -1
	for _, k := range keys {
		idx := strings.Index(inner, `"`+k+`":`)
		require.Greater(t, idx, last, "key %s out of order", k)
		last = idx
	}

	// HTML characters are not escaped, so the raw line is readable as-is.
	require.Contains(t, line, `& <tagged>`)
}

func TestEncoderOmitsEmptyFields(t *testing.T) {
	enc := newTestEncoder(defaultMaxFileSize)
	enc.meta = recordMetadata{CollectorID: testCollectorID}

	rec, _ := encodeOne(t, enc, func(_ plog.ResourceLogs, lr plog.LogRecord) {
		lr.Body().SetStr("line")
	})
	require.Equal(t, map[string]any{
		"@timestamp":     "2023-04-19T18:38:12.501Z",
		"collector_type": "cribl-logs",
		"collector_id":   testCollectorID,
		"message":        "line",
	}, rec)
}

func TestEncoderEmptyMessageIsKept(t *testing.T) {
	rec, _ := encodeOne(t, newTestEncoder(defaultMaxFileSize), func(plog.ResourceLogs, plog.LogRecord) {})
	msg, ok := rec["message"]
	require.True(t, ok)
	require.Equal(t, "", msg)
}

func TestEncoderAttributeOverrides(t *testing.T) {
	t.Run("attributes override config and resource", func(t *testing.T) {
		rec, _ := encodeOne(t, newTestEncoder(defaultMaxFileSize), func(rl plog.ResourceLogs, lr plog.LogRecord) {
			rl.Resource().Attributes().PutStr("host.name", "web-01")
			lr.Body().SetStr("line")
			lr.Attributes().PutStr("exabeam_hostname", "attr-host")
			lr.Attributes().PutStr("exabeam_forwarder", "attr-forwarder")
			lr.Attributes().PutStr("exabeam_siteid", "attr-site")
			lr.Attributes().PutStr("exabeam_timezone_offset", "+0530")
			tags := lr.Attributes().PutEmptyMap("exabeam_logtags")
			tags.PutStr("department", "Finance")
			tags.PutInt("tier", 1)
		})
		require.Equal(t, "attr-host", rec["hostname"])
		require.Equal(t, "attr-forwarder", rec["forwarder"])
		require.Equal(t, "attr-site", rec["siteid"])
		require.Equal(t, "+0530", rec["timezone_offset"])
		require.Equal(t, map[string]any{"department": "Finance", "servertype": "PCI", "tier": "1"}, rec["logtags"])
	})

	t.Run("config hostname beats resource host.name", func(t *testing.T) {
		enc := newTestEncoder(defaultMaxFileSize)
		enc.meta.Hostname = "config-host"
		rec, _ := encodeOne(t, enc, func(rl plog.ResourceLogs, _ plog.LogRecord) {
			rl.Resource().Attributes().PutStr("host.name", "web-01")
		})
		require.Equal(t, "config-host", rec["hostname"])
	})

	t.Run("non-string and empty attributes are ignored", func(t *testing.T) {
		rec, _ := encodeOne(t, newTestEncoder(defaultMaxFileSize), func(_ plog.ResourceLogs, lr plog.LogRecord) {
			lr.Attributes().PutInt("exabeam_siteid", 5)
			lr.Attributes().PutStr("exabeam_forwarder", "")
			lr.Attributes().PutStr("exabeam_logtags", "not a map")
		})
		require.Equal(t, "site-1", rec["siteid"])
		require.Equal(t, "bindplane", rec["forwarder"])
		require.Equal(t, map[string]any{"department": "HR", "servertype": "PCI"}, rec["logtags"])
	})

	t.Run("invalid timezone offset attribute is ignored", func(t *testing.T) {
		rec, _ := encodeOne(t, newTestEncoder(defaultMaxFileSize), func(_ plog.ResourceLogs, lr plog.LogRecord) {
			lr.Attributes().PutStr("exabeam_timezone_offset", "UTC-7")
		})
		require.Equal(t, "-0700", rec["timezone_offset"])
	})

	t.Run("empty logtag names from attributes are ignored", func(t *testing.T) {
		rec, _ := encodeOne(t, newTestEncoder(defaultMaxFileSize), func(_ plog.ResourceLogs, lr plog.LogRecord) {
			tags := lr.Attributes().PutEmptyMap("exabeam_logtags")
			tags.PutStr("", "nameless")
			tags.PutStr("app", "payments")
		})
		require.Equal(t, map[string]any{"department": "HR", "servertype": "PCI", "app": "payments"}, rec["logtags"])
	})

	t.Run("logtags from attributes are capped at the limit", func(t *testing.T) {
		rec, _ := encodeOne(t, newTestEncoder(defaultMaxFileSize), func(_ plog.ResourceLogs, lr plog.LogRecord) {
			tags := lr.Attributes().PutEmptyMap("exabeam_logtags")
			// Overriding a configured tag always applies, and doesn't count as a new tag.
			tags.PutStr("department", "Finance")
			for i := range 12 {
				tags.PutStr(fmt.Sprintf("tag%02d", i), "v")
			}
		})
		logTags := rec["logtags"].(map[string]any)
		require.Len(t, logTags, 10)
		require.Equal(t, "Finance", logTags["department"])
		require.Equal(t, "PCI", logTags["servertype"])
		// New tags are added in name order until the limit.
		for i := range 8 {
			require.Contains(t, logTags, fmt.Sprintf("tag%02d", i))
		}
		require.NotContains(t, logTags, "tag08")
	})

	t.Run("logtags from attributes only", func(t *testing.T) {
		enc := newTestEncoder(defaultMaxFileSize)
		enc.meta.LogTags = nil
		rec, _ := encodeOne(t, enc, func(_ plog.ResourceLogs, lr plog.LogRecord) {
			lr.Attributes().PutEmptyMap("exabeam_logtags").PutStr("app", "payments")
		})
		require.Equal(t, map[string]any{"app": "payments"}, rec["logtags"])
	})

	t.Run("records in one batch don't share overrides", func(t *testing.T) {
		ld := plog.NewLogs()
		lrs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
		first := lrs.AppendEmpty()
		first.Attributes().PutStr("exabeam_hostname", "first")
		first.Attributes().PutEmptyMap("exabeam_logtags").PutStr("only", "first")
		lrs.AppendEmpty()

		files, err := newTestEncoder(defaultMaxFileSize).encode(context.Background(), ld)
		require.NoError(t, err)
		lines := gunzipLines(t, files[0].data)
		require.Len(t, lines, 2)
		second := decodeRecord(t, lines[1])
		require.NotContains(t, second, "hostname")
		require.Equal(t, map[string]any{"department": "HR", "servertype": "PCI"}, second["logtags"])
	})
}

func TestEncoderDefaultMessage(t *testing.T) {
	tests := []struct {
		desc     string
		setup    func(lr plog.LogRecord)
		expected string
	}{
		{
			desc:     "string body",
			setup:    func(lr plog.LogRecord) { lr.Body().SetStr("  raw line with spaces\tand tabs  ") },
			expected: "  raw line with spaces\tand tabs  ",
		},
		{
			desc:     "bytes body",
			setup:    func(lr plog.LogRecord) { lr.Body().SetEmptyBytes().FromRaw([]byte("raw bytes line")) },
			expected: "raw bytes line",
		},
		{
			desc:     "map body",
			setup:    func(lr plog.LogRecord) { lr.Body().SetEmptyMap().PutStr("k", "v") },
			expected: `{"k":"v"}`,
		},
		{
			desc: "log.record.original wins over body",
			setup: func(lr plog.LogRecord) {
				lr.Body().SetEmptyMap().PutStr("parsed", "yes")
				lr.Attributes().PutStr("log.record.original", "original raw line")
			},
			expected: "original raw line",
		},
		{
			desc: "non-string log.record.original is ignored",
			setup: func(lr plog.LogRecord) {
				lr.Body().SetStr("body line")
				lr.Attributes().PutInt("log.record.original", 5)
			},
			expected: "body line",
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			rec, _ := encodeOne(t, newTestEncoder(defaultMaxFileSize), func(_ plog.ResourceLogs, lr plog.LogRecord) { tc.setup(lr) })
			require.Equal(t, tc.expected, rec["message"])
		})
	}
}

func TestEncoderRawLogField(t *testing.T) {
	tests := []struct {
		desc        string
		rawLogField string
		setup       func(lr plog.LogRecord)
		expected    string
	}{
		{
			desc:        "body ignores log.record.original",
			rawLogField: "body",
			setup: func(lr plog.LogRecord) {
				lr.Body().SetStr("body line")
				lr.Attributes().PutStr("log.record.original", "original")
			},
			expected: "body line",
		},
		{
			desc:        "attribute",
			rawLogField: `attributes["raw"]`,
			setup: func(lr plog.LogRecord) {
				lr.Body().SetStr("body line")
				lr.Attributes().PutStr("raw", "from attribute")
			},
			expected: "from attribute",
		},
		{
			desc:        "map field is JSON",
			rawLogField: `body["event"]`,
			setup: func(lr plog.LogRecord) {
				lr.Body().SetEmptyMap().PutEmptyMap("event").PutStr("k", "v")
			},
			expected: `{"k":"v"}`,
		},
		{
			desc:        "missing field is empty",
			rawLogField: `attributes["missing"]`,
			setup:       func(lr plog.LogRecord) { lr.Body().SetStr("body line") },
			expected:    "",
		},
		{
			desc:        "converter",
			rawLogField: `Concat([attributes["a"], body], " ")`,
			setup: func(lr plog.LogRecord) {
				lr.Body().SetStr("line")
				lr.Attributes().PutStr("a", "prefix")
			},
			expected: "prefix line",
		},
	}

	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			enc := newTestEncoderWithConfig(t, func(c *Config) { c.RawLogField = tc.rawLogField })
			rec, _ := encodeOne(t, enc, func(_ plog.ResourceLogs, lr plog.LogRecord) { tc.setup(lr) })
			require.Equal(t, tc.expected, rec["message"])
		})
	}
}

func TestEncoderRawLogFieldRuntimeErrorFallsBack(t *testing.T) {
	core, logs := observer.New(zap.WarnLevel)
	enc := newTestEncoderWithConfig(t, func(c *Config) { c.RawLogField = failingRawLogField })
	enc.logger = zap.New(core)

	ld := plog.NewLogs()
	lrs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for i := range 3 {
		lr := lrs.AppendEmpty()
		lr.Body().SetStr(fmt.Sprintf("body %d", i))
		lr.Attributes().PutInt("raw", int64(i))
	}
	original := lrs.AppendEmpty()
	original.Body().SetStr("parsed")
	original.Attributes().PutStr("log.record.original", "original line")
	original.Attributes().PutBool("raw", true)

	files, err := enc.encode(context.Background(), ld)
	require.NoError(t, err)
	var messages []any
	for _, line := range gunzipLines(t, files[0].data) {
		messages = append(messages, decodeRecord(t, line)["message"])
	}
	require.Equal(t, []any{"body 0", "body 1", "body 2", "original line"}, messages, "the default raw log is used")

	warnings := logs.All()
	require.Len(t, warnings, 1)
	require.Equal(t, "raw_log_field failed; used the default raw log", warnings[0].Message)
	require.Equal(t, int64(4), warnings[0].ContextMap()["records"])
	require.Contains(t, warnings[0].ContextMap()["error"], "expected string")
}

func TestNewEncoderInvalidRawLogField(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	cfg.RawLogField = `attributes["unterminated`
	_, err := newEncoder(cfg, connectionSettings{}, componenttest.NewNopTelemetrySettings())
	require.ErrorContains(t, err, "raw_log_field is invalid")
}

func TestRawValueString(t *testing.T) {
	strValue := pcommon.NewValueStr("str value")
	bytesValue := pcommon.NewValueBytes()
	bytesValue.Bytes().FromRaw([]byte("bytes value"))
	intValue := pcommon.NewValueInt(7)
	mapValue := pcommon.NewValueMap()
	mapValue.Map().PutStr("k", "v")
	m := pcommon.NewMap()
	m.PutInt("n", 1)
	s := pcommon.NewSlice()
	s.AppendEmpty().SetStr("a")
	s.AppendEmpty().SetInt(2)

	tests := []struct {
		desc     string
		in       any
		expected string
	}{
		{desc: "nil", in: nil, expected: ""},
		{desc: "string", in: "line", expected: "line"},
		{desc: "bytes", in: []byte("raw bytes"), expected: "raw bytes"},
		{desc: "string value", in: strValue, expected: "str value"},
		{desc: "bytes value", in: bytesValue, expected: "bytes value"},
		{desc: "int value", in: intValue, expected: "7"},
		{desc: "map value", in: mapValue, expected: `{"k":"v"}`},
		{desc: "map", in: m, expected: `{"n":1}`},
		{desc: "slice", in: s, expected: `["a",2]`},
		{desc: "raw map", in: map[string]any{"k": "v"}, expected: `{"k":"v"}`},
		{desc: "raw slice", in: []any{"a", int64(2)}, expected: `["a",2]`},
		{desc: "string slice", in: []string{"a", "b"}, expected: `["a","b"]`},
		{desc: "int slice", in: []int64{1, 2}, expected: `[1,2]`},
		{desc: "typed map", in: map[string]string{"k": "v"}, expected: `{"k":"v"}`},
		{desc: "int", in: int64(42), expected: "42"},
		{desc: "float", in: 1.5, expected: "1.5"},
		{desc: "bool", in: true, expected: "true"},
	}
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			got, err := rawValueString(tc.in)
			require.NoError(t, err)
			require.Equal(t, tc.expected, got)
		})
	}

	t.Run("unmarshalable value", func(t *testing.T) {
		_, err := rawValueString(map[string]any{"nan": math.NaN()})
		require.ErrorContains(t, err, "marshal raw_log_field result")
	})
}

func TestNewEncoderFromConfig(t *testing.T) {
	enc := newTestEncoderWithConfig(t, func(c *Config) {
		c.CollectorName = "gw"
		c.Hostname = "host-a"
		c.Forwarder = "fwd"
		c.LogTags = map[string]string{"env": "prod"}
	})
	rec, _ := encodeOne(t, enc, func(_ plog.ResourceLogs, lr plog.LogRecord) { lr.Body().SetStr("x") })
	require.Equal(t, map[string]any{
		"@timestamp":      "2023-04-19T18:38:12.501Z",
		"collector_type":  "cribl-logs",
		"collector_name":  "gw",
		"collector_id":    testCollectorID,
		"message":         "x",
		"timezone_offset": "-0700",
		"hostname":        "host-a",
		"forwarder":       "fwd",
		"siteid":          "xdfsdfw99999sfsdf",
		"sitename":        "abcorp",
		"logtags":         map[string]any{"env": "prod"},
	}, rec)
}

func TestEncoderAllRecords(t *testing.T) {
	ld := plog.NewLogs()
	for r := range 2 {
		rl := ld.ResourceLogs().AppendEmpty()
		for s := range 2 {
			sl := rl.ScopeLogs().AppendEmpty()
			for l := range 3 {
				sl.LogRecords().AppendEmpty().Body().SetStr(strings.Repeat("x", r*100+s*10+l))
			}
		}
	}

	files, err := newTestEncoder(defaultMaxFileSize).encode(context.Background(), ld)
	require.NoError(t, err)
	require.Len(t, files, 1)
	lines := gunzipLines(t, files[0].data)
	require.Len(t, lines, 12)
	for _, line := range lines {
		require.Equal(t, testCollectorID, decodeRecord(t, line)["collector_id"])
	}
}

func TestEncoderNoRecords(t *testing.T) {
	files, err := newTestEncoder(defaultMaxFileSize).encode(context.Background(), plog.NewLogs())
	require.NoError(t, err)
	require.Empty(t, files)
}

func TestEncoderSplitsAtMaxFileSize(t *testing.T) {
	ld := plog.NewLogs()
	lrs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for range 10 {
		lrs.AppendEmpty().Body().SetStr(strings.Repeat("a", 200))
	}
	// One oversized record still goes out, alone in its own file.
	lrs.AppendEmpty().Body().SetStr(strings.Repeat("b", 2000))

	files, err := newTestEncoder(1000).encode(context.Background(), ld)
	require.NoError(t, err)

	var total int
	var sizes []int
	for _, f := range files {
		lines := gunzipLines(t, f.data)
		size := 0
		for _, l := range lines {
			size += len(l) + 1
		}
		sizes = append(sizes, size)
		total += len(lines)
		if len(lines) > 1 {
			require.LessOrEqual(t, size, 1000)
		}
	}
	require.Equal(t, 11, total)
	require.Greater(t, len(files), 3, "sizes %v", sizes)
	last := gunzipLines(t, files[len(files)-1].data)
	require.Len(t, last, 1)
	require.Equal(t, strings.Repeat("b", 2000), decodeRecord(t, last[0])["message"])
}
