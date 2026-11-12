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

package azureblobexporter

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
)

func TestNewAzureBlobClientUserAgent(t *testing.T) {
	userAgents := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		userAgents <- r.Header.Get("User-Agent")
		w.WriteHeader(http.StatusCreated)
	}))
	defer server.Close()

	connectionString := "DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=" + server.URL + "/devstoreaccount1;"
	buildInfo := component.BuildInfo{Command: "test-collector", Version: "1.2.3"}

	client, err := newAzureBlobClient(connectionString, buildInfo)
	require.NoError(t, err)
	require.NoError(t, client.UploadBuffer(context.Background(), "container", "blob.json", []byte("data")))

	userAgent := <-userAgents
	require.True(t, strings.HasPrefix(userAgent, "test-collector/1.2.3 azsdk-go-azblob/"), "User-Agent %q", userAgent)
}
