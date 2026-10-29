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
	"errors"
	"fmt"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"go.opentelemetry.io/collector/consumer/consumererror"
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
		APIOptions:                 []func(*middleware.Stack) error{excludeHeadersFromSigning(unsignedHeaders...)},
	})

	return &s3Uploader{client: client, bucket: s.BucketName}
}

// unsignedHeaders are sent but left out of the SigV4 signature. Google's front end
// rewrites Accept-Encoding before GCS verifies the signature, so signing it causes
// SignatureDoesNotMatch.
var unsignedHeaders = []string{"Accept-Encoding"}

// excludeHeadersFromSigning removes headers from the request just before it is signed
// and restores them just after, so they are sent without being signed.
func excludeHeadersFromSigning(headers ...string) func(*middleware.Stack) error {
	return func(stack *middleware.Stack) error {
		if err := stack.Finalize.Insert(removeHeadersMiddleware(headers), "Signing", middleware.Before); err != nil {
			return fmt.Errorf("insert %s middleware: %w", removeHeadersID, err)
		}
		if err := stack.Finalize.Insert(restoreHeadersMiddleware(), "Signing", middleware.After); err != nil {
			return fmt.Errorf("insert %s middleware: %w", restoreHeadersID, err)
		}
		return nil
	}
}

const (
	removeHeadersID  = "ExabeamRemoveUnsignedHeaders"
	restoreHeadersID = "ExabeamRestoreUnsignedHeaders"
)

// removedHeadersKey holds the headers removed before signing in the request metadata.
type removedHeadersKey struct{}

func removeHeadersMiddleware(headers []string) middleware.FinalizeMiddleware {
	return middleware.FinalizeMiddlewareFunc(removeHeadersID, func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
		req, ok := in.Request.(*smithyhttp.Request)
		if !ok {
			return next.HandleFinalize(ctx, in)
		}
		removed := make(map[string][]string, len(headers))
		for _, h := range headers {
			if v := req.Header.Values(h); len(v) > 0 {
				removed[h] = v
				req.Header.Del(h)
			}
		}
		ctx = middleware.WithStackValue(ctx, removedHeadersKey{}, removed)
		return next.HandleFinalize(ctx, in)
	})
}

func restoreHeadersMiddleware() middleware.FinalizeMiddleware {
	return middleware.FinalizeMiddlewareFunc(restoreHeadersID, func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
		req, ok := in.Request.(*smithyhttp.Request)
		if !ok {
			return next.HandleFinalize(ctx, in)
		}
		if removed, ok := middleware.GetStackValue(ctx, removedHeadersKey{}).(map[string][]string); ok {
			for h, values := range removed {
				for _, v := range values {
					req.Header.Add(h, v)
				}
			}
		}
		return next.HandleFinalize(ctx, in)
	})
}

// PutObject uploads body as key with the private ACL. It never checks for or creates the bucket.
// Errors that retrying can't fix are marked permanent so the exporterhelper drops the data
// instead of retrying it.
func (u *s3Uploader) PutObject(ctx context.Context, key string, body []byte) error {
	_, err := u.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:        aws.String(u.bucket),
		Key:           aws.String(key),
		Body:          bytes.NewReader(body),
		ContentLength: aws.Int64(int64(len(body))),
		ACL:           types.ObjectCannedACLPrivate,
	})
	if err != nil {
		err = fmt.Errorf("put object %s: %w", key, err)
		if isPermanentUploadError(err) {
			return consumererror.NewPermanent(err)
		}
		return err
	}
	return nil
}

// isPermanentUploadError reports whether err is a 4xx response other than 408 Request Timeout
// or 429 Too Many Requests, such as wrong keys (403) or a missing bucket (404). 5xx responses
// and errors without a response, such as network errors, are retried. The SDK has already
// retried 5xx responses itself by the time they get here.
func isPermanentUploadError(err error) bool {
	var respErr interface{ HTTPStatusCode() int }
	if !errors.As(err, &respErr) {
		return false
	}
	code := respErr.HTTPStatusCode()
	return code >= 400 && code < 500 &&
		code != http.StatusRequestTimeout && code != http.StatusTooManyRequests
}
