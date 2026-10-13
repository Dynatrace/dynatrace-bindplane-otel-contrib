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

package opampgateway

import (
	"errors"
	"net/http"
	"net/url"
	"strings"

	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configtls"
)

// ServerConfig holds the configuration for the upstreamOpAMP server.
type ServerConfig struct {
	Endpoint    string                 `mapstructure:"endpoint"`
	Headers     http.Header            `mapstructure:"headers"`
	TLS         configtls.ClientConfig `mapstructure:"tls,omitempty"`
	Connections int                    `mapstructure:"connections"`
	// GatewayID identifies this gateway to the upstream server. It prefixes the
	// X-Opamp-Gateway-Connection-Id header as "<gateway_id>/<connection id>". When it is
	// empty, the gateway uses the service.instance.id of the collector.
	GatewayID string `mapstructure:"gateway_id"`
}

// Config holds the configuration for the OpAMP gateway extension.
type Config struct {
	Server   ServerConfig            `mapstructure:"server"`
	Listener confighttp.ServerConfig `mapstructure:"listener"`
}

// Validate checks that the configuration is valid.
func (c *Config) Validate() error {
	if c.Server.Endpoint == "" {
		return errors.New("opamp_client endpoint must be specified")
	}
	u, err := url.Parse(c.Server.Endpoint)
	if err != nil {
		return errors.New("opamp_client endpoint is not a valid URL")
	}
	if u.Scheme != "ws" && u.Scheme != "wss" {
		return errors.New("opamp_client endpoint must use ws:// or wss:// scheme")
	}

	if c.Server.Connections < 1 {
		return errors.New("opamp_client connections must be at least 1")
	}

	if strings.Contains(c.Server.GatewayID, "/") {
		return errors.New("opamp_client gateway_id must not contain '/'")
	}

	if c.Listener.NetAddr.Endpoint == "" {
		return errors.New("opamp_server endpoint must be specified")
	}

	return nil
}
