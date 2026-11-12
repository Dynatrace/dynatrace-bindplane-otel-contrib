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

package awssecuritylakeexporter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.uber.org/zap"
)

func TestNewS3ClientUserAgent(t *testing.T) {
	// Keep the AWS config loader away from the environment it runs in.
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	userAgents := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case userAgents <- r.Header.Get("User-Agent"):
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	buildInfo := component.BuildInfo{Command: "test-collector", Version: "1.2.3"}
	client, err := NewS3Client(context.Background(), "us-east-1", "", server.URL, buildInfo, zap.NewNop())
	require.NoError(t, err)
	require.NoError(t, client.Upload(context.Background(), "bucket", "key", strings.NewReader("data")))

	require.Contains(t, <-userAgents, " test-collector/1.2.3")
}
