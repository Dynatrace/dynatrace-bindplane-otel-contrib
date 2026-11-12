// Copyright  Dynatrace LLC
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

// Package sapnetweaverreceiver is an OTel compatible receiver
package sapnetweaverreceiver // import "github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/sapnetweaverreceiver"

import (
	"context"
	"fmt"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/internal/useragent"
	"go.opentelemetry.io/collector/component"

	"github.com/hooklift/gowsdl/soap"
)

// newSoapClient creates the SOAP client. Its requests carry the collector
// distribution from buildInfo in their User-Agent, unless one is configured in
// headers.
func newSoapClient(ctx context.Context, cfg *Config, host component.Host, settings component.TelemetrySettings, buildInfo component.BuildInfo) (*soap.Client, error) {
	httpClient, err := cfg.ClientConfig.ToClient(ctx, host.GetExtensions(), settings)
	if err != nil {
		return nil, fmt.Errorf("failed to create HTTP Client: %w", err)
	}

	opts := []soap.Option{soap.WithBasicAuth(cfg.Username, cfg.Password), soap.WithHTTPClient(httpClient)}
	// The soap client sets its own User-Agent on every request, so wrapping
	// the transport would never apply. Override it with a soap header instead.
	// Headers configured in confighttp are applied later, by the transport, so
	// a configured User-Agent still wins.
	if ua := useragent.String(buildInfo); ua != "" {
		opts = append(opts, soap.WithHTTPHeaders(map[string]string{"User-Agent": ua}))
	}
	return soap.NewClient(cfg.ClientConfig.Endpoint, opts...), nil
}
