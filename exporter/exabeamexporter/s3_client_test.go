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
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go/middleware"
	smithyhttp "github.com/aws/smithy-go/transport/http"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/consumererror"
)

// recordedRequest is a request received by fakeS3.
type recordedRequest struct {
	Method string
	Path   string
	Query  string
	Header http.Header
	Body   []byte
}

// fakeS3 is an httptest server that records every request and answers with status.
type fakeS3 struct {
	*httptest.Server
	mu       sync.Mutex
	requests []recordedRequest
	status   int
	// next holds statuses for the next requests, in order, before falling back to status.
	next []int
}

func newFakeS3(t *testing.T) *fakeS3 {
	t.Helper()
	f := &fakeS3{status: http.StatusOK}
	f.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		f.mu.Lock()
		f.requests = append(f.requests, recordedRequest{
			Method: r.Method,
			Path:   r.URL.Path,
			Query:  r.URL.RawQuery,
			Header: r.Header.Clone(),
			Body:   body,
		})
		status := f.status
		if len(f.next) > 0 {
			status, f.next = f.next[0], f.next[1:]
		}
		f.mu.Unlock()
		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?><Error><Code>AccessDenied</Code><Message>denied</Message></Error>`))
			return
		}
		w.Header().Set("ETag", `"abc"`)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(f.Close)
	return f
}

func (f *fakeS3) setStatus(status int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status = status
}

// respondNext answers the next requests with statuses, in order.
func (f *fakeS3) respondNext(statuses ...int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.next = append(f.next, statuses...)
}

func (f *fakeS3) recorded() []recordedRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]recordedRequest(nil), f.requests...)
}

func testSettings() connectionSettings {
	return connectionSettings{
		CollectorID:    testCollectorID,
		AccessKey:      testAccessKey,
		SecretKey:      testSecretKey,
		BucketName:     "exabeam-bucket",
		BucketLocation: "US",
	}
}

func TestS3UploaderPutObject(t *testing.T) {
	srv := newFakeS3(t)
	up := newS3Uploader(testSettings(), srv.URL)

	body := []byte("gzipped file contents")
	require.NoError(t, up.PutObject(context.Background(), "exa-cribl-logs_x.log.gz", body))

	reqs := srv.recorded()
	require.Len(t, reqs, 1, "only the PutObject request is sent; no bucket checks or creation")
	req := reqs[0]

	require.Equal(t, http.MethodPut, req.Method)
	require.Equal(t, "/exabeam-bucket/exa-cribl-logs_x.log.gz", req.Path, "path-style addressing at the bucket root")
	require.Equal(t, "x-id=PutObject", req.Query, "only the SDK's operation marker")
	require.Equal(t, body, req.Body)

	auth := req.Header.Get("Authorization")
	require.True(t, strings.HasPrefix(auth, "AWS4-HMAC-SHA256 Credential="+testAccessKey+"/"), auth)
	require.Contains(t, auth, "/US/s3/aws4_request")
	require.NotContains(t, auth, testSecretKey)

	// Google's front end rewrites Accept-Encoding before GCS checks the signature,
	// so signing it causes SignatureDoesNotMatch.
	requireAcceptEncodingUnsigned(t, req)

	require.Equal(t, "private", req.Header.Get("X-Amz-Acl"))
	require.Empty(t, req.Header.Get("X-Amz-Storage-Class"))

	// GCS's XML API rejects aws-chunked uploads with trailing checksums.
	require.NotContains(t, req.Header.Get("Content-Encoding"), "aws-chunked")
	require.Empty(t, req.Header.Get("X-Amz-Trailer"))
	for k := range req.Header {
		require.False(t, strings.HasPrefix(strings.ToLower(k), "x-amz-checksum-"), "unexpected checksum header %s", k)
	}
}

// requireAcceptEncodingUnsigned checks Accept-Encoding is sent but not signed.
func requireAcceptEncodingUnsigned(t *testing.T, req recordedRequest) {
	t.Helper()
	auth := req.Header.Get("Authorization")
	require.Contains(t, auth, "SignedHeaders=")
	require.NotContains(t, auth, "accept-encoding")
	require.Equal(t, []string{"identity"}, req.Header.Values("Accept-Encoding"), "sent once, unsigned")
}

func TestS3UploaderSDKRetryKeepsHeaderUnsigned(t *testing.T) {
	srv := newFakeS3(t)
	srv.respondNext(http.StatusServiceUnavailable)
	up := newS3Uploader(testSettings(), srv.URL)

	require.NoError(t, up.PutObject(context.Background(), "key.log.gz", []byte("x")))

	reqs := srv.recorded()
	require.Len(t, reqs, 2, "the SDK retries the 503 itself")
	for _, req := range reqs {
		requireAcceptEncodingUnsigned(t, req)
	}
}

func TestS3UploaderEmptyLocationUsesAuto(t *testing.T) {
	srv := newFakeS3(t)
	s := testSettings()
	s.BucketLocation = ""
	up := newS3Uploader(s, srv.URL)

	require.NoError(t, up.PutObject(context.Background(), "key.log.gz", []byte("x")))
	reqs := srv.recorded()
	require.Len(t, reqs, 1)
	require.Contains(t, reqs[0].Header.Get("Authorization"), "/auto/s3/aws4_request")
}

func TestS3UploaderError(t *testing.T) {
	srv := newFakeS3(t)
	srv.setStatus(http.StatusForbidden)
	up := newS3Uploader(testSettings(), srv.URL)

	err := up.PutObject(context.Background(), "key.log.gz", []byte("x"))
	require.ErrorContains(t, err, "AccessDenied")
	require.True(t, consumererror.IsPermanent(err), "wrong keys can't be fixed by retrying")
	require.Len(t, srv.recorded(), 1, "the SDK doesn't retry a 403 either")
	require.Equal(t, http.MethodPut, srv.recorded()[0].Method)
}

func TestIsPermanentUploadError(t *testing.T) {
	respErr := func(code int) error {
		return fmt.Errorf("put object key: %w", &awshttp.ResponseError{
			ResponseError: &smithyhttp.ResponseError{
				Response: &smithyhttp.Response{Response: &http.Response{StatusCode: code}},
				Err:      errors.New("api error"),
			},
		})
	}

	for _, code := range []int{http.StatusBadRequest, http.StatusUnauthorized, http.StatusForbidden, http.StatusNotFound} {
		require.True(t, isPermanentUploadError(respErr(code)), "%d is permanent", code)
	}
	for _, code := range []int{http.StatusRequestTimeout, http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusServiceUnavailable} {
		require.False(t, isPermanentUploadError(respErr(code)), "%d is retried", code)
	}
	require.False(t, isPermanentUploadError(errors.New("connection refused")), "errors without a response are retried")
	require.False(t, isPermanentUploadError(context.DeadlineExceeded))
}

func TestExcludeHeadersFromSigningNeedsSigningStep(t *testing.T) {
	// An SDK that renamed or dropped its Signing step must fail loudly, not send signed headers.
	stack := middleware.NewStack("test", smithyhttp.NewStackRequest)
	err := excludeHeadersFromSigning(unsignedHeaders...)(stack)
	require.ErrorContains(t, err, "insert "+removeHeadersID+" middleware")

	require.NoError(t, stack.Finalize.Add(middleware.FinalizeMiddlewareFunc("Signing", func(ctx context.Context, in middleware.FinalizeInput, next middleware.FinalizeHandler) (middleware.FinalizeOutput, middleware.Metadata, error) {
		return next.HandleFinalize(ctx, in)
	}), middleware.After))
	require.NoError(t, excludeHeadersFromSigning(unsignedHeaders...)(stack))
	require.Equal(t, []string{removeHeadersID, "Signing", restoreHeadersID}, stack.Finalize.List())
}

func TestHeaderMiddlewaresPassThroughNonHTTPRequests(t *testing.T) {
	var reached int
	next := middleware.FinalizeHandlerFunc(func(context.Context, middleware.FinalizeInput) (middleware.FinalizeOutput, middleware.Metadata, error) {
		reached++
		return middleware.FinalizeOutput{}, middleware.Metadata{}, nil
	})
	in := middleware.FinalizeInput{Request: "not an http request"}

	_, _, err := removeHeadersMiddleware(unsignedHeaders).HandleFinalize(context.Background(), in, next)
	require.NoError(t, err)
	_, _, err = restoreHeadersMiddleware().HandleFinalize(context.Background(), in, next)
	require.NoError(t, err)
	require.Equal(t, 2, reached)
}
