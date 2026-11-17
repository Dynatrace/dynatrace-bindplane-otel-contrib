// Copyright Dynatrace LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package chronicleexporter

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"go.uber.org/zap"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// tokenRequestTimeout bounds each OAuth2 token request. Without it, token
// requests use http.DefaultClient, which has no timeout, and a token endpoint
// that never responds would stall exports indefinitely. Overridden in tests.
var tokenRequestTimeout = 30 * time.Second

// Override for testing
var tokenSource = func(ctx context.Context, cfg *Config) (oauth2.TokenSource, error) {
	// A client with a nil Transport uses http.DefaultTransport, which honors
	// HTTPS_PROXY and NO_PROXY.
	ctx = context.WithValue(ctx, oauth2.HTTPClient, &http.Client{Timeout: tokenRequestTimeout})
	if cfg.TokenURL != "" {
		return privateTokenSource(ctx, cfg)
	}
	creds, err := googleCredentials(ctx, cfg)
	if err != nil {
		return nil, err
	}
	return creds.TokenSource, nil
}

func googleCredentials(ctx context.Context, cfg *Config) (*google.Credentials, error) {
	if cfg.Creds == "" && cfg.CredsFilePath == "" {
		return google.FindDefaultCredentials(ctx, credsScope(cfg))
	}
	credsData, err := credsJSON(cfg)
	if err != nil {
		return nil, err
	}
	return google.CredentialsFromJSON(ctx, credsData, credsScope(cfg))
}

// privateTokenSource returns a token source for a service account key that
// requests access tokens from cfg.TokenURL, such as a Private Service Connect
// endpoint. The JWT assertion audience remains Google's public token endpoint,
// which Google requires regardless of the endpoint the request is sent to.
func privateTokenSource(ctx context.Context, cfg *Config) (oauth2.TokenSource, error) {
	credsData, err := credsJSON(cfg)
	if err != nil {
		return nil, err
	}
	jc, err := google.JWTConfigFromJSON(credsData, credsScope(cfg))
	if err != nil {
		return nil, fmt.Errorf("token_url requires service account credentials: %w", err)
	}
	jc.TokenURL = cfg.TokenURL
	if jc.Audience == "" {
		jc.Audience = google.JWTTokenURL
	}
	return jc.TokenSource(ctx), nil
}

// credsScope returns the OAuth2 scope required by the configured protocol.
func credsScope(cfg *Config) string {
	if cfg.Protocol == protocolHTTPS {
		return httpScope
	}
	return grpcScope
}

// credsJSON returns the credentials JSON from creds or creds_file_path.
func credsJSON(cfg *Config) ([]byte, error) {
	if cfg.Creds != "" {
		return []byte(cfg.Creds), nil
	}
	if cfg.CredsFilePath == "" {
		return nil, errors.New("no credentials configured")
	}
	credsData, err := os.ReadFile(cfg.CredsFilePath)
	if err != nil {
		return nil, fmt.Errorf("read credentials file: %w", err)
	}
	if len(credsData) == 0 {
		return nil, errors.New("credentials file is empty")
	}
	return credsData, nil
}

// unavailableTokenSource reports transient token fetch failures, such as an
// unreachable token endpoint, as codes.Unavailable. gRPC otherwise reports them
// as codes.Unauthenticated, which the gRPC exporter treats as permanent,
// dropping the batch instead of retrying it. A rejection from the token
// endpoint, such as invalid credentials, is returned unchanged.
type unavailableTokenSource struct {
	oauth2.TokenSource
}

func (s unavailableTokenSource) Token() (*oauth2.Token, error) {
	tok, err := s.TokenSource.Token()
	if err != nil && !isTokenRejection(err) {
		return nil, status.Errorf(codes.Unavailable, "fetch OAuth2 token: %v", err)
	}
	return tok, err
}

// isTokenRejection reports whether err is a 4xx response from the token
// endpoint, other than 408 and 429, which retrying will not resolve.
func isTokenRejection(err error) bool {
	var re *oauth2.RetrieveError
	if !errors.As(err, &re) || re.Response == nil {
		return false
	}
	code := re.Response.StatusCode
	return code >= 400 && code < 500 && code != http.StatusRequestTimeout && code != http.StatusTooManyRequests
}

// checkTokenURL fetches a token in the background when token_url is set, and
// logs a warning if it fails. Tokens are otherwise fetched on the first export,
// so a misconfigured token_url would go unnoticed until then. Startup is not
// blocked or failed. The fetched token is cached by ts and reused.
func checkTokenURL(cfg *Config, logger *zap.Logger, ts oauth2.TokenSource) {
	if cfg.TokenURL == "" {
		return
	}
	go func() {
		if _, err := ts.Token(); err != nil {
			logger.Warn("Failed to fetch an OAuth2 token from token_url", zap.String("token_url", cfg.TokenURL), zap.Error(err))
		}
	}()
}
