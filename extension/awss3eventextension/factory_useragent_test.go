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

package awss3eventextension

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/extension/extensiontest"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/extension/awss3eventextension/internal/metadata"
	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/internal/aws/client"
)

func TestCreateExtensionUserAgent(t *testing.T) {
	// Keep the AWS config loader away from the environment it runs in.
	dir := t.TempDir()
	t.Setenv("AWS_CONFIG_FILE", filepath.Join(dir, "config"))
	t.Setenv("AWS_SHARED_CREDENTIALS_FILE", filepath.Join(dir, "credentials"))
	t.Setenv("AWS_PROFILE", "")
	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")

	var awsConfig aws.Config
	realNewClient := client.NewClient
	t.Cleanup(func() { client.NewClient = realNewClient })
	client.NewClient = func(cfg aws.Config) client.Client {
		awsConfig = cfg
		return realNewClient(cfg)
	}

	set := extensiontest.NewNopSettings(metadata.Type)
	set.BuildInfo = component.BuildInfo{Command: "test-collector", Version: "1.2.3"}
	cfg := createDefaultConfig().(*Config)
	cfg.SQSQueueURL = "https://sqs.us-west-2.amazonaws.com/123456789012/test-queue"
	cfg.Directory = t.TempDir()
	_, err := NewFactory().Create(context.Background(), set, cfg)
	require.NoError(t, err)

	userAgents := make(chan string, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case userAgents <- r.Header.Get("User-Agent"):
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	sqsClient := sqs.NewFromConfig(awsConfig, func(o *sqs.Options) {
		o.BaseEndpoint = aws.String(server.URL)
	})
	// Only the request matters here, not the response.
	_, _ = sqsClient.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{QueueUrl: aws.String(server.URL)})

	require.Contains(t, <-userAgents, " test-collector/1.2.3")
}
