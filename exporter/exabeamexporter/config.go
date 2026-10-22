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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/pkg/expr"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configopaque"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.uber.org/zap"
)

var timezoneOffsetPattern = regexp.MustCompile(`^[+-]\d{4}$`)

// maxLogTags is the number of custom fields Exabeam accepts on a Cloud Collector.
const maxLogTags = 10

// Config is the configuration for the exabeam exporter
type Config struct {
	// ConnectionString is the base64-encoded JSON connection string from the Exabeam Cloud Collector.
	// It supplies every connection setting below; any setting given explicitly overrides it.
	ConnectionString configopaque.String `mapstructure:"connection_string"`

	// CollectorID is the ID of the Exabeam Cloud Collector. It is used in object names and copied into every record.
	CollectorID string `mapstructure:"collector_id"`

	// AccessKey is the HMAC access key for the bucket.
	AccessKey configopaque.String `mapstructure:"access_key"`

	// SecretKey is the HMAC secret key for the bucket.
	SecretKey configopaque.String `mapstructure:"secret_key"`

	// BucketName is the name of the bucket Exabeam created for the Cloud Collector.
	BucketName string `mapstructure:"bucket_name"`

	// BucketLocation is the location of the bucket. It is used as the request signing region.
	BucketLocation string `mapstructure:"bucket_location"`

	// SiteName is copied into every record.
	SiteName string `mapstructure:"sitename"`

	// SiteID is copied into every record. It is overridden by attributes["exabeam_siteid"].
	SiteID string `mapstructure:"siteid"`

	// TimezoneOffset is copied into every record, in the form +HHMM or -HHMM.
	// It is overridden by attributes["exabeam_timezone_offset"].
	TimezoneOffset string `mapstructure:"timezone_offset"`

	// CollectorName is copied into every record.
	CollectorName string `mapstructure:"collector_name"`

	// Hostname is written to every record. It defaults to the host.name resource attribute
	// and is overridden by attributes["exabeam_hostname"].
	Hostname string `mapstructure:"hostname"`

	// Forwarder is written to every record. It is overridden by attributes["exabeam_forwarder"].
	Forwarder string `mapstructure:"forwarder"`

	// LogTags are written to every record, merged with attributes["exabeam_logtags"].
	// Exabeam makes each one searchable as m_c_logtags_<name>.
	LogTags map[string]string `mapstructure:"logtags"`

	// RawLogField is an OTTL expression for the raw log written to message.
	// By default it is attributes["log.record.original"] when present, otherwise the body.
	RawLogField string `mapstructure:"raw_log_field"`

	// Endpoint is the S3-compatible endpoint to upload to.
	Endpoint string `mapstructure:"endpoint"`

	// MaxFileSize is the maximum uncompressed size of a file in bytes. A batch larger than this is split into several files.
	MaxFileSize int64 `mapstructure:"max_file_size"`

	// TimeoutConfig configures timeout settings for exporter operations.
	TimeoutConfig exporterhelper.TimeoutConfig `mapstructure:",squash"` // squash ensures fields are correctly decoded in embedded struct.

	// QueueConfig defines the queuing and batching behavior for the exporter. Batching controls file rollover.
	QueueConfig configoptional.Optional[exporterhelper.QueueBatchConfig] `mapstructure:"sending_queue"`

	// BackOffConfig defines the retry behavior for failed operations.
	BackOffConfig configretry.BackOffConfig `mapstructure:"retry_on_failure"`
}

// connectionSettings holds the settings from the connection string merged with the explicit overrides.
type connectionSettings struct {
	CollectorID    string              `json:"collector_id"`
	AccessKey      configopaque.String `json:"access_key"`
	SecretKey      configopaque.String `json:"secret_key"`
	BucketName     string              `json:"bucket_name"`
	BucketLocation string              `json:"bucket_location"`
	SiteName       string              `json:"sitename"`
	SiteID         string              `json:"siteid"`
	TimezoneOffset string              `json:"timezone_offset"`
}

// Validate checks that the configuration is valid
func (c *Config) Validate() error {
	s, err := c.resolve()
	if err != nil {
		return err
	}

	var missing []string
	if s.CollectorID == "" {
		missing = append(missing, "collector_id")
	}
	if s.AccessKey == "" {
		missing = append(missing, "access_key")
	}
	if s.SecretKey == "" {
		missing = append(missing, "secret_key")
	}
	if s.BucketName == "" {
		missing = append(missing, "bucket_name")
	}
	if len(missing) > 0 {
		return fmt.Errorf("missing %s: set connection_string or the individual settings", strings.Join(missing, ", "))
	}

	if s.TimezoneOffset != "" && !timezoneOffsetPattern.MatchString(s.TimezoneOffset) {
		return fmt.Errorf("timezone_offset must be in the form +HHMM or -HHMM, got %q", s.TimezoneOffset)
	}

	u, err := url.Parse(c.Endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("endpoint must be an http or https URL, got %q", c.Endpoint)
	}

	if c.MaxFileSize <= 0 {
		return errors.New("max_file_size must be greater than 0")
	}

	if len(c.LogTags) > maxLogTags {
		return fmt.Errorf("logtags can have at most %d entries, got %d", maxLogTags, len(c.LogTags))
	}
	if _, ok := c.LogTags[""]; ok {
		return errors.New("logtags names must not be empty")
	}

	if c.RawLogField != "" {
		if _, err := expr.NewOTTLLogRecordExpression(c.RawLogField, component.TelemetrySettings{Logger: zap.NewNop()}); err != nil {
			return fmt.Errorf("raw_log_field is invalid: %w", err)
		}
	}

	return nil
}

// resolve decodes the connection string and applies the explicit settings over it.
func (c *Config) resolve() (connectionSettings, error) {
	var s connectionSettings
	if c.ConnectionString != "" {
		decoded, err := decodeConnectionString(string(c.ConnectionString))
		if err != nil {
			return connectionSettings{}, err
		}
		s = decoded
	}

	overrideString(&s.CollectorID, c.CollectorID)
	overrideString(&s.BucketName, c.BucketName)
	overrideString(&s.BucketLocation, c.BucketLocation)
	overrideString(&s.SiteName, c.SiteName)
	overrideString(&s.SiteID, c.SiteID)
	overrideString(&s.TimezoneOffset, c.TimezoneOffset)
	if c.AccessKey != "" {
		s.AccessKey = c.AccessKey
	}
	if c.SecretKey != "" {
		s.SecretKey = c.SecretKey
	}
	return s, nil
}

func overrideString(dst *string, v string) {
	if v != "" {
		*dst = v
	}
}

// decodeConnectionString decodes the base64 JSON connection string Exabeam provides.
// Errors never include the decoded content, since it contains the secret key.
func decodeConnectionString(connectionString string) (connectionSettings, error) {
	connectionString = strings.TrimSpace(connectionString)

	var raw []byte
	var err error
	for _, enc := range []*base64.Encoding{base64.StdEncoding, base64.RawStdEncoding, base64.URLEncoding, base64.RawURLEncoding} {
		raw, err = enc.DecodeString(connectionString)
		if err == nil {
			break
		}
	}
	if err != nil {
		return connectionSettings{}, errors.New("connection_string is not valid base64")
	}

	var s connectionSettings
	if err := json.Unmarshal(raw, &s); err != nil {
		// The field name is safe to report; the value may be the secret key.
		if typeErr, ok := errors.AsType[*json.UnmarshalTypeError](err); ok && typeErr.Field != "" {
			return connectionSettings{}, fmt.Errorf("connection_string field %q must be a string", typeErr.Field)
		}
		return connectionSettings{}, errors.New("connection_string does not decode to a JSON object")
	}
	return s, nil
}
