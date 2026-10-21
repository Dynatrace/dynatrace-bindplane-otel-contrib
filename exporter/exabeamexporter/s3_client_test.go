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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
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

	require.Equal(t, "private", req.Header.Get("X-Amz-Acl"))
	require.Empty(t, req.Header.Get("X-Amz-Storage-Class"))

	// GCS's XML API rejects aws-chunked uploads with trailing checksums.
	require.NotContains(t, req.Header.Get("Content-Encoding"), "aws-chunked")
	require.Empty(t, req.Header.Get("X-Amz-Trailer"))
	for k := range req.Header {
		require.False(t, strings.HasPrefix(strings.ToLower(k), "x-amz-checksum-"), "unexpected checksum header %s", k)
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
	for _, r := range srv.recorded() {
		require.Equal(t, http.MethodPut, r.Method)
	}
}
