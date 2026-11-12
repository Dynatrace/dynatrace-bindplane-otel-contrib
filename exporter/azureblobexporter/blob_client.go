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

package azureblobexporter // import "github.com/dynatrace/dynatrace-bindplane-otel-contrib/exporter/azureblobexporter"

import (
	"context"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"go.opentelemetry.io/collector/component"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/internal/useragent/azure"
)

// blobClient is a wrapper for an Azure Blob client to allow mocking for testing.
//
//go:generate mockery --name blobClient --output ./internal/mocks --with-expecter --filename mock_blob_client.go --structname MockBlobClient
type blobClient interface {
	// UploadBuffer uploads a buffer in blocks to a block blob.
	UploadBuffer(context.Context, string, string, []byte) error
}

// azureBlobClient is the azure implementation of the blobClient
type azureBlobClient struct {
	azClient *azblob.Client
}

// newAzureBlobClient creates a new azureBlobClient with the given connection string.
// Requests name the collector distribution from buildInfo in their User-Agent.
func newAzureBlobClient(connectionString string, buildInfo component.BuildInfo) (*azureBlobClient, error) {
	azClient, err := azblob.NewClientFromConnectionString(connectionString, &azblob.ClientOptions{
		ClientOptions: azcore.ClientOptions{PerCallPolicies: azure.Policies(buildInfo)},
	})
	if err != nil {
		return nil, err
	}

	return &azureBlobClient{
		azClient: azClient,
	}, nil
}

func (a *azureBlobClient) UploadBuffer(ctx context.Context, containerName, blobName string, buffer []byte) error {
	_, err := a.azClient.UploadBuffer(ctx, containerName, blobName, buffer, nil)
	return err
}
