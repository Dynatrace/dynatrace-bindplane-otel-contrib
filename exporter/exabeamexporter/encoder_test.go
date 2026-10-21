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
	"encoding/json"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

var testWriteTime = time.Date(2023, 4, 19, 11, 38, 12, 501_999_999, time.FixedZone("x", -7*3600))

func newTestEncoder(maxFileSize int64) *encoder {
	return &encoder{
		meta: recordMetadata{
			Tags:           `"tag1":"v1", "tag2":"v2"`,
			CollectorName:  "bindplane-gateway",
			CollectorID:    testCollectorID,
			SiteName:       "abcorp",
			SiteID:         "site-1",
			TimezoneOffset: "-0700",
		},
		maxFileSize: maxFileSize,
		now:         func() time.Time { return testWriteTime },
	}
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

func decodeRecord(t *testing.T, line string) map[string]string {
	t.Helper()
	var rec map[string]string
	require.NoError(t, json.Unmarshal([]byte(line), &rec))
	return rec
}

func TestEncoderRecordFormat(t *testing.T) {
	ld := plog.NewLogs()
	lr := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.Body().SetStr(`<34>1 2003-10-11T22:14:15.003Z host app - ID47 - "quoted" & <tagged> \u00e9`)

	files, err := newTestEncoder(defaultMaxFileSize).encode(ld)
	require.NoError(t, err)
	require.Len(t, files, 1)

	lines := gunzipLines(t, files[0])
	require.Len(t, lines, 1)

	// Field order matches Exabeam's spec.
	require.True(t, strings.HasPrefix(lines[0], `{"@timestamp":"2023-04-19T18:38:12.501Z","tags":`), lines[0])
	keys := []string{"@timestamp", "tags", "collector_type", "collector_name", "collector_id", "sitename", "siteid", "timezone_offset", "message"}
	last := -1
	for _, k := range keys {
		idx := strings.Index(lines[0], `"`+k+`":`)
		require.Greater(t, idx, last, "key %s out of order", k)
		last = idx
	}

	require.Equal(t, map[string]string{
		"@timestamp":      "2023-04-19T18:38:12.501Z",
		"tags":            `"tag1":"v1", "tag2":"v2"`,
		"collector_type":  "cribl-logs",
		"collector_name":  "bindplane-gateway",
		"collector_id":    testCollectorID,
		"sitename":        "abcorp",
		"siteid":          "site-1",
		"timezone_offset": "-0700",
		"message":         lr.Body().Str(),
	}, decodeRecord(t, lines[0]))

	// HTML characters are not escaped, so the raw line is readable as-is.
	require.Contains(t, lines[0], `& <tagged>`)
}

func TestEncoderMessage(t *testing.T) {
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
			desc: "map body",
			setup: func(lr plog.LogRecord) {
				lr.Body().SetEmptyMap().PutStr("k", "v")
			},
			expected: `{"k":"v"}`,
		},
		{
			desc:     "empty body",
			setup:    func(plog.LogRecord) {},
			expected: "",
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
			ld := plog.NewLogs()
			tc.setup(ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty())

			files, err := newTestEncoder(defaultMaxFileSize).encode(ld)
			require.NoError(t, err)
			require.Len(t, files, 1)
			lines := gunzipLines(t, files[0])
			require.Len(t, lines, 1)
			require.Equal(t, tc.expected, decodeRecord(t, lines[0])["message"])
		})
	}
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

	files, err := newTestEncoder(defaultMaxFileSize).encode(ld)
	require.NoError(t, err)
	require.Len(t, files, 1)
	lines := gunzipLines(t, files[0])
	require.Len(t, lines, 12)
	for _, line := range lines {
		require.Equal(t, testCollectorID, decodeRecord(t, line)["collector_id"])
	}
}

func TestEncoderNoRecords(t *testing.T) {
	files, err := newTestEncoder(defaultMaxFileSize).encode(plog.NewLogs())
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

	enc := newTestEncoder(1000)
	files, err := enc.encode(ld)
	require.NoError(t, err)

	var total int
	var sizes []int
	for _, f := range files {
		lines := gunzipLines(t, f)
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
	last := gunzipLines(t, files[len(files)-1])
	require.Len(t, last, 1)
	require.Equal(t, strings.Repeat("b", 2000), decodeRecord(t, last[0])["message"])
}
