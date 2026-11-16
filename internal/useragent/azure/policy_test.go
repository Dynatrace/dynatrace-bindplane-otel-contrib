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

package azure

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
)

func TestPolicies(t *testing.T) {
	var got string
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		got = r.Header.Get("User-Agent")
	}))
	defer server.Close()

	buildInfo := component.BuildInfo{Command: "com.dynatrace.bindplane.otel.collector", Version: "v1.4.0"}
	pl := runtime.NewPipeline("azblob", "v1.0.0", runtime.PipelineOptions{}, &policy.ClientOptions{
		PerCallPolicies: Policies(buildInfo),
	})

	req, err := runtime.NewRequest(context.Background(), http.MethodGet, server.URL)
	require.NoError(t, err)
	resp, err := pl.Do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())

	// The full product comes first, followed by the SDK's own telemetry.
	require.True(t, strings.HasPrefix(got, "com.dynatrace.bindplane.otel.collector/v1.4.0 azsdk-go-azblob/v1.0.0"), got)
}

func TestPoliciesWithoutProduct(t *testing.T) {
	require.Nil(t, Policies(component.BuildInfo{}))
}
