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

package aws

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
)

func TestLoadOptions(t *testing.T) {
	require.Nil(t, LoadOptions(component.BuildInfo{}))
	require.Nil(t, LoadOptions(component.BuildInfo{Command: "otelcol"}))

	t.Setenv("AWS_CONFIG_FILE", t.TempDir()+"/config")
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", t.TempDir()+"/credentials")
	t.Setenv("AWS_PROFILE", "")

	opts := LoadOptions(component.BuildInfo{Command: "otelcol", Version: "1.0.0"})
	require.Len(t, opts, 1)

	cfg, err := config.LoadDefaultConfig(context.Background(), append(opts, config.WithRegion("us-east-1"))...)
	require.NoError(t, err)
	require.Len(t, cfg.APIOptions, 1, "the user agent middleware is registered on the config")
}
