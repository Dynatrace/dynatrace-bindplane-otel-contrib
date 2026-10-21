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
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
)

// logRecordOriginalAttribute holds the original raw log when a source parsed the body.
const logRecordOriginalAttribute = "log.record.original"

// timestampLayout is the UTC millisecond timestamp Exabeam expects in @timestamp.
const timestampLayout = "2006-01-02T15:04:05.000Z"

// recordMetadata holds the fields copied into every record.
type recordMetadata struct {
	Tags           string
	CollectorName  string
	CollectorID    string
	SiteName       string
	SiteID         string
	TimezoneOffset string
}

// record is one line of an Exabeam file. Field order matches Exabeam's spec.
type record struct {
	Timestamp      string `json:"@timestamp"`
	Tags           string `json:"tags"`
	CollectorType  string `json:"collector_type"`
	CollectorName  string `json:"collector_name"`
	CollectorID    string `json:"collector_id"`
	SiteName       string `json:"sitename"`
	SiteID         string `json:"siteid"`
	TimezoneOffset string `json:"timezone_offset"`
	Message        string `json:"message"`
}

// encoder turns logs into gzipped NDJSON files.
type encoder struct {
	meta        recordMetadata
	maxFileSize int64
	now         func() time.Time
}

func newEncoder(cfg *Config, s connectionSettings) *encoder {
	return &encoder{
		meta: recordMetadata{
			Tags:           cfg.Tags,
			CollectorName:  cfg.CollectorName,
			CollectorID:    s.CollectorID,
			SiteName:       s.SiteName,
			SiteID:         s.SiteID,
			TimezoneOffset: s.TimezoneOffset,
		},
		maxFileSize: cfg.MaxFileSize,
		now:         time.Now,
	}
}

// encode returns one gzipped file per maxFileSize bytes of uncompressed records.
// A single record larger than maxFileSize gets a file of its own.
func (e *encoder) encode(ld plog.Logs) ([][]byte, error) {
	rec := record{
		Timestamp:      e.now().UTC().Format(timestampLayout),
		Tags:           e.meta.Tags,
		CollectorType:  collectorType,
		CollectorName:  e.meta.CollectorName,
		CollectorID:    e.meta.CollectorID,
		SiteName:       e.meta.SiteName,
		SiteID:         e.meta.SiteID,
		TimezoneOffset: e.meta.TimezoneOffset,
	}

	var files [][]byte
	var file bytes.Buffer
	var line bytes.Buffer
	lineEncoder := json.NewEncoder(&line)
	lineEncoder.SetEscapeHTML(false)

	flush := func() error {
		if file.Len() == 0 {
			return nil
		}
		gz, err := gzipBytes(file.Bytes())
		if err != nil {
			return err
		}
		files = append(files, gz)
		file.Reset()
		return nil
	}

	for _, rl := range ld.ResourceLogs().All() {
		for _, sl := range rl.ScopeLogs().All() {
			for _, lr := range sl.LogRecords().All() {
				rec.Message = message(lr)
				line.Reset()
				// Encode writes the trailing newline.
				if err := lineEncoder.Encode(rec); err != nil {
					return nil, fmt.Errorf("encode record: %w", err)
				}
				if file.Len() > 0 && int64(file.Len()+line.Len()) > e.maxFileSize {
					if err := flush(); err != nil {
						return nil, err
					}
				}
				file.Write(line.Bytes())
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}
	return files, nil
}

// message returns the raw log: log.record.original when a source set it, otherwise the body.
func message(lr plog.LogRecord) string {
	if original, ok := lr.Attributes().Get(logRecordOriginalAttribute); ok && original.Type() == pcommon.ValueTypeStr {
		return original.Str()
	}
	body := lr.Body()
	if body.Type() == pcommon.ValueTypeBytes {
		return string(body.Bytes().AsRaw())
	}
	return body.AsString()
}

func gzipBytes(b []byte) ([]byte, error) {
	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	if _, err := zw.Write(b); err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	if err := zw.Close(); err != nil {
		return nil, fmt.Errorf("gzip: %w", err)
	}
	return buf.Bytes(), nil
}
