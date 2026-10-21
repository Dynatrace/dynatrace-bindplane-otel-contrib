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

package exabeamexporter // import "github.com/dynatrace/dynatrace-bindplane-otel-contrib/exporter/exabeamexporter"

import (
	"bytes"
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
)

// defaultSigningRegion is used when no bucket location is known. GCS accepts it for SigV4.
const defaultSigningRegion = "auto"

// objectUploader uploads a file to the bucket root
type objectUploader interface {
	PutObject(ctx context.Context, key string, body []byte) error
}

// s3Uploader uploads to the Exabeam GCS bucket through its S3-compatible API with HMAC keys.
type s3Uploader struct {
	client *s3.Client
	bucket string
}

func newS3Uploader(s connectionSettings, endpoint string) *s3Uploader {
	region := s.BucketLocation
	if region == "" {
		region = defaultSigningRegion
	}

	// The client is built from explicit options rather than the default AWS config chain,
	// so credentials or settings from the environment are never picked up.
	client := s3.New(s3.Options{
		Region:       region,
		BaseEndpoint: aws.String(endpoint),
		UsePathStyle: true,
		Credentials: aws.NewCredentialsCache(
			credentials.NewStaticCredentialsProvider(string(s.AccessKey), string(s.SecretKey), ""),
		),
		// GCS rejects the aws-chunked uploads with trailing checksums that the SDK sends by default.
		RequestChecksumCalculation: aws.RequestChecksumCalculationWhenRequired,
		ResponseChecksumValidation: aws.ResponseChecksumValidationWhenRequired,
	})

	return &s3Uploader{client: client, bucket: s.BucketName}
}

// PutObject uploads body as key with the private ACL. It never checks for or creates the bucket.
func (u *s3Uploader) PutObject(ctx context.Context, key string, body []byte) error {
	_, err := u.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(u.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
		ACL:           types.ObjectCannedACLPrivate,
	})
	if err != nil {
		return fmt.Errorf("put object %s: %w", key, err)
	}
	return nil
}
