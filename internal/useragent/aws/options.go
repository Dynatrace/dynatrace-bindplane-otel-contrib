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

// Package aws adds the collector distribution to the User-Agent of AWS SDK v2
// clients.
package aws // import "github.com/dynatrace/dynatrace-bindplane-otel-contrib/internal/useragent/aws"

import (
	awsmiddleware "github.com/aws/aws-sdk-go-v2/aws/middleware"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/smithy-go/middleware"
	"go.opentelemetry.io/collector/component"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/internal/useragent"
)

// LoadOptions returns options for config.LoadDefaultConfig that add the
// collector distribution from buildInfo to the SDK's User-Agent as a
// "<name>/<version>" entry. It returns nil when the build info does not name
// the collector.
func LoadOptions(buildInfo component.BuildInfo) []func(*config.LoadOptions) error {
	name, version := useragent.Name(buildInfo), useragent.Version(buildInfo)
	if name == "" || version == "" {
		return nil
	}
	return []func(*config.LoadOptions) error{
		config.WithAPIOptions([]func(*middleware.Stack) error{
			awsmiddleware.AddUserAgentKeyValue(name, version),
		}),
	}
}
