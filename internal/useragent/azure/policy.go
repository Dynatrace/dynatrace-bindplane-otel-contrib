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

// Package azure adds the collector distribution to the User-Agent of Azure SDK
// clients.
//
// The Azure SDK's own hook for this, ClientOptions.Telemetry.ApplicationID,
// truncates the value to 24 characters, which cuts a reverse-DNS collector name
// short. A per-call policy runs after the SDK's telemetry policy has set the
// User-Agent, so it can prepend the full product instead.
package azure // import "github.com/dynatrace/dynatrace-bindplane-otel-contrib/internal/useragent/azure"

import (
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"go.opentelemetry.io/collector/component"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/internal/useragent"
)

// Policies returns the per-call policies that add the collector distribution
// from buildInfo to the User-Agent, for use as ClientOptions.PerCallPolicies.
// It returns nil when the build info does not name the collector.
func Policies(buildInfo component.BuildInfo) []policy.Policy {
	product := useragent.Product(buildInfo)
	if product == "" {
		return nil
	}
	return []policy.Policy{prependPolicy{product: product}}
}

type prependPolicy struct {
	product string
}

// Do implements policy.Policy.
func (p prependPolicy) Do(req *policy.Request) (*http.Response, error) {
	header := req.Raw().Header
	if ua := header.Get("User-Agent"); ua != "" {
		header.Set("User-Agent", p.product+" "+ua)
	} else {
		header.Set("User-Agent", p.product)
	}
	return req.Next()
}
