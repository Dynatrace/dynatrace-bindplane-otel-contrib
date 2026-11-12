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

package gcspubsubeventreceiver

import (
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"google.golang.org/api/option"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/gcspubsubeventreceiver/internal/metadata"
)

func TestClientOptions_UserAgent(t *testing.T) {
	newReceiver := func(t *testing.T, buildInfo component.BuildInfo) *logsReceiver {
		t.Helper()
		params := receivertest.NewNopSettings(metadata.Type)
		params.BuildInfo = buildInfo
		cfg := createDefaultConfig().(*Config)
		cfg.CredentialsFile = "creds.json"
		r, err := newLogsReceiver(params, cfg, new(consumertest.LogsSink), nil)
		require.NoError(t, err)
		return r.(*logsReceiver)
	}

	t.Run("build info names the collector", func(t *testing.T) {
		r := newReceiver(t, component.BuildInfo{Command: "otelcol-test", Version: "v1.2.3"})
		require.Equal(t, []option.ClientOption{
			option.WithCredentialsFile("creds.json"),
			option.WithUserAgent("otelcol-test/v1.2.3 (" + runtime.GOOS + "/" + runtime.GOARCH + ")"),
		}, r.clientOptions())
	})

	t.Run("build info is empty", func(t *testing.T) {
		r := newReceiver(t, component.BuildInfo{})
		require.Equal(t, []option.ClientOption{option.WithCredentialsFile("creds.json")}, r.clientOptions())
	})
}
