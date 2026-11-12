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

package googlecloudstoragerehydrationreceiver

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"google.golang.org/api/option"
)

func Test_userAgentOptions(t *testing.T) {
	t.Run("build info names the collector", func(t *testing.T) {
		opts := userAgentOptions(component.BuildInfo{Command: "otelcol-test", Version: "v1.2.3"})
		want := option.WithUserAgent("otelcol-test/v1.2.3 (" + runtime.GOOS + "/" + runtime.GOARCH + ")")
		require.Equal(t, []option.ClientOption{want}, opts)
	})

	t.Run("build info is empty", func(t *testing.T) {
		require.Empty(t, userAgentOptions(component.BuildInfo{}))
	})
}
