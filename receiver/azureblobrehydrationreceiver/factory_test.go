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

package azureblobrehydrationreceiver //import "github.com/dynatrace/dynatrace-bindplane-otel-contrib/receiver/azureblobrehydrationreceiver"

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/receiver/receivertest"
	"go.uber.org/zap"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/internal/azureblob"
)

func Test_createDefaultConfig(t *testing.T) {
	expectedCfg := &Config{
		DeleteOnRead: false,
		BatchSize:    defaultBatchSize,
		PageSize:     defaultPageSize,
	}

	componentCfg := createDefaultConfig()
	actualCfg, ok := componentCfg.(*Config)
	require.True(t, ok)
	require.Equal(t, expectedCfg, actualCfg)
}

func Test_createLogsReceiverPassesBuildInfo(t *testing.T) {
	oldfunc := newAzureBlobClient
	t.Cleanup(func() { newAzureBlobClient = oldfunc })

	var gotBuildInfo component.BuildInfo
	newAzureBlobClient = func(_ string, _ int, _ int, _ *zap.Logger, buildInfo component.BuildInfo) (azureblob.BlobClient, error) {
		gotBuildInfo = buildInfo
		return azureblob.NewMockBlobClient(t), nil
	}

	params := receivertest.NewNopSettings(receivertest.NopType)
	params.BuildInfo = component.BuildInfo{Command: "test-collector", Version: "1.2.3"}
	cfg := &Config{
		StartingTime: "2023-10-02T17:00",
		EndingTime:   "2023-10-02T17:01",
	}

	_, err := createLogsReceiver(context.Background(), params, cfg, consumertest.NewNop())
	require.NoError(t, err)
	require.Equal(t, params.BuildInfo, gotBuildInfo)
}
