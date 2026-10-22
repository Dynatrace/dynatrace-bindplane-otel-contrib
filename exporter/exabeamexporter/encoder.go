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
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"slices"
	"time"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/pkg/expr"
	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/ottl/contexts/ottllog"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
)

const (
	// logRecordOriginalAttribute holds the original raw log when a source parsed the body.
	logRecordOriginalAttribute = "log.record.original"

	// hostNameResourceAttribute is the default source of hostname.
	hostNameResourceAttribute = "host.name"

	// Log record attributes that override the configured metadata for that record.
	hostnameAttribute       = "exabeam_hostname"
	forwarderAttribute      = "exabeam_forwarder"
	siteIDAttribute         = "exabeam_siteid"
	timezoneOffsetAttribute = "exabeam_timezone_offset"
	logTagsAttribute        = "exabeam_logtags"
)

// timestampLayout is the UTC millisecond timestamp Exabeam expects in @timestamp.
const timestampLayout = "2006-01-02T15:04:05.000Z"

// recordMetadata holds the configured fields written to every record.
type recordMetadata struct {
	CollectorName  string
	CollectorID    string
	SiteName       string
	SiteID         string
	TimezoneOffset string
	Hostname       string
	Forwarder      string
	LogTags        map[string]string
}

// envelope is one line of an Exabeam file. Exabeam reads the record from the message key.
type envelope struct {
	Message record `json:"message"`
}

// record is the event Exabeam ingests.
// Empty fields are left out. Message is always written.
type record struct {
	Timestamp      string            `json:"@timestamp"`
	CollectorType  string            `json:"collector_type"`
	CollectorName  string            `json:"collector_name,omitempty"`
	CollectorID    string            `json:"collector_id"`
	Message        string            `json:"message"`
	TimezoneOffset string            `json:"timezone_offset,omitempty"`
	Hostname       string            `json:"hostname,omitempty"`
	Forwarder      string            `json:"forwarder,omitempty"`
	SiteID         string            `json:"siteid,omitempty"`
	SiteName       string            `json:"sitename,omitempty"`
	LogTags        map[string]string `json:"logtags,omitempty"`
}

// encodedFile is a gzipped NDJSON file and the index of its first log record in the encoded logs.
type encodedFile struct {
	data        []byte
	firstRecord int
}

// encoder turns logs into gzipped NDJSON files.
type encoder struct {
	meta        recordMetadata
	rawLogField *expr.OTTLExpression[*ottllog.TransformContext]
	maxFileSize int64
	now         func() time.Time
	logger      *zap.Logger
}

func newEncoder(cfg *Config, s connectionSettings, set component.TelemetrySettings) (*encoder, error) {
	e := &encoder{
		meta: recordMetadata{
			CollectorName:  cfg.CollectorName,
			CollectorID:    s.CollectorID,
			SiteName:       s.SiteName,
			SiteID:         s.SiteID,
			TimezoneOffset: s.TimezoneOffset,
			Hostname:       cfg.Hostname,
			Forwarder:      cfg.Forwarder,
			LogTags:        cfg.LogTags,
		},
		maxFileSize: cfg.MaxFileSize,
		now:         time.Now,
		logger:      set.Logger,
	}

	if cfg.RawLogField != "" {
		rawLogField, err := expr.NewOTTLLogRecordExpression(cfg.RawLogField, set)
		if err != nil {
			return nil, fmt.Errorf("raw_log_field is invalid: %w", err)
		}
		e.rawLogField = rawLogField
	}
	return e, nil
}

// encode returns one gzipped file per maxFileSize bytes of uncompressed records.
// A single record larger than maxFileSize gets a file of its own.
func (e *encoder) encode(ctx context.Context, ld plog.Logs) ([]encodedFile, error) {
	timestamp := e.now().UTC().Format(timestampLayout)

	var files []encodedFile
	var file bytes.Buffer
	var line bytes.Buffer
	lineEncoder := json.NewEncoder(&line)
	lineEncoder.SetEscapeHTML(false)

	index, fileStart := 0, 0
	var rawLogFieldFailures int
	var lastRawLogFieldErr error

	flush := func() error {
		if file.Len() == 0 {
			return nil
		}
		gz, err := gzipBytes(file.Bytes())
		if err != nil {
			return err
		}
		files = append(files, encodedFile{data: gz, firstRecord: fileStart})
		file.Reset()
		fileStart = index
		return nil
	}

	for _, rl := range ld.ResourceLogs().All() {
		for _, sl := range rl.ScopeLogs().All() {
			for _, lr := range sl.LogRecords().All() {
				rec, err := e.record(ctx, timestamp, rl, sl, lr)
				if err != nil {
					// The default raw log keeps the record instead of failing the whole batch.
					rawLogFieldFailures++
					lastRawLogFieldErr = err
				}
				line.Reset()
				// Encode writes the trailing newline.
				if err := lineEncoder.Encode(envelope{Message: rec}); err != nil {
					return nil, fmt.Errorf("encode record: %w", err)
				}
				if file.Len() > 0 && int64(file.Len()+line.Len()) > e.maxFileSize {
					if err := flush(); err != nil {
						return nil, err
					}
				}
				file.Write(line.Bytes())
				index++
			}
		}
	}
	if err := flush(); err != nil {
		return nil, err
	}

	if rawLogFieldFailures > 0 {
		e.logger.Warn("raw_log_field failed; used the default raw log",
			zap.Int("records", rawLogFieldFailures),
			zap.Error(lastRawLogFieldErr),
		)
	}
	return files, nil
}

// record builds the record for one log record, applying attribute overrides to the configured metadata.
// When raw_log_field fails, the record uses the default raw log and the error is returned with it.
func (e *encoder) record(ctx context.Context, timestamp string, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) (record, error) {
	message, err := e.message(ctx, rl, sl, lr)
	if err != nil {
		message = defaultMessage(lr)
	}

	attrs := lr.Attributes()
	hostname := e.meta.Hostname
	if hostname == "" {
		hostname = stringAttribute(rl.Resource().Attributes(), hostNameResourceAttribute, "")
	}

	return record{
		Timestamp:      timestamp,
		CollectorType:  collectorType,
		CollectorName:  e.meta.CollectorName,
		CollectorID:    e.meta.CollectorID,
		Message:        message,
		TimezoneOffset: e.timezoneOffset(attrs),
		Hostname:       stringAttribute(attrs, hostnameAttribute, hostname),
		Forwarder:      stringAttribute(attrs, forwarderAttribute, e.meta.Forwarder),
		SiteID:         stringAttribute(attrs, siteIDAttribute, e.meta.SiteID),
		SiteName:       e.meta.SiteName,
		LogTags:        e.logTags(attrs),
	}, err
}

// message returns the raw log: the raw_log_field expression when configured, otherwise
// log.record.original when a source set it, otherwise the body.
func (e *encoder) message(ctx context.Context, rl plog.ResourceLogs, sl plog.ScopeLogs, lr plog.LogRecord) (string, error) {
	if e.rawLogField != nil {
		v, err := e.rawLogField.Execute(ctx, ottllog.NewTransformContext(rl, sl, lr))
		if err != nil {
			return "", fmt.Errorf("execute raw_log_field: %w", err)
		}
		return rawValueString(v)
	}
	return defaultMessage(lr), nil
}

// defaultMessage returns log.record.original when a source set it, otherwise the body.
func defaultMessage(lr plog.LogRecord) string {
	if original, ok := lr.Attributes().Get(logRecordOriginalAttribute); ok && original.Type() == pcommon.ValueTypeStr {
		return original.Str()
	}
	body := lr.Body()
	if body.Type() == pcommon.ValueTypeBytes {
		return string(body.Bytes().AsRaw())
	}
	return body.AsString()
}

// rawValueString converts the result of the raw_log_field expression to the raw log string.
func rawValueString(v any) (string, error) {
	switch v := v.(type) {
	case nil:
		return "", nil
	case string:
		return v, nil
	case []byte:
		return string(v), nil
	case pcommon.Value:
		if v.Type() == pcommon.ValueTypeBytes {
			return string(v.Bytes().AsRaw()), nil
		}
		return v.AsString(), nil
	case pcommon.Map:
		return marshalRaw(v.AsRaw())
	case pcommon.Slice:
		return marshalRaw(v.AsRaw())
	default:
		// OTTL converters return typed slices and maps, such as []string from Split.
		// Write those as JSON, and other values in their plain form.
		switch reflect.ValueOf(v).Kind() {
		case reflect.Slice, reflect.Array, reflect.Map:
			return marshalRaw(v)
		default:
			return fmt.Sprint(v), nil
		}
	}
}

func marshalRaw(v any) (string, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return "", fmt.Errorf("marshal raw_log_field result: %w", err)
	}
	return string(b), nil
}

// timezoneOffset returns attributes["exabeam_timezone_offset"] when it is a valid +HHMM or -HHMM
// offset, otherwise the configured offset.
func (e *encoder) timezoneOffset(attrs pcommon.Map) string {
	if v := stringAttribute(attrs, timezoneOffsetAttribute, ""); timezoneOffsetPattern.MatchString(v) {
		return v
	}
	return e.meta.TimezoneOffset
}

// logTags merges attributes["exabeam_logtags"] over the configured logtags. Attribute values
// replace configured ones. New names are added in name order up to the logtags limit, and
// empty names are ignored, the same rules the configured logtags are held to.
func (e *encoder) logTags(attrs pcommon.Map) map[string]string {
	v, ok := attrs.Get(logTagsAttribute)
	if !ok || v.Type() != pcommon.ValueTypeMap || v.Map().Len() == 0 {
		return e.meta.LogTags
	}

	merged := make(map[string]string, len(e.meta.LogTags)+v.Map().Len())
	maps.Copy(merged, e.meta.LogTags)
	var added []string
	for k, tag := range v.Map().All() {
		switch {
		case k == "":
		case hasKey(e.meta.LogTags, k):
			merged[k] = tag.AsString()
		default:
			added = append(added, k)
		}
	}
	slices.Sort(added)
	for _, k := range added {
		if len(merged) >= maxLogTags {
			break
		}
		tag, _ := v.Map().Get(k)
		merged[k] = tag.AsString()
	}
	return merged
}

func hasKey(m map[string]string, k string) bool {
	_, ok := m[k]
	return ok
}

// stringAttribute returns the non-empty string attribute key, or fallback.
func stringAttribute(attrs pcommon.Map, key, fallback string) string {
	if v, ok := attrs.Get(key); ok && v.Type() == pcommon.ValueTypeStr && v.Str() != "" {
		return v.Str()
	}
	return fallback
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
