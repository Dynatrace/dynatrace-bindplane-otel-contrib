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

package networkcheckreceiver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRedactEndpoint(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want string
	}{
		{"no userinfo", "https://example.com/path", "https://example.com/path"},
		// The username used to survive as user:xxxxx@. It is dropped now along
		// with the password, because a token is often the username.
		{"user and password", "https://user:secret@example.com", "https://example.com"},
		{"user only", "https://user@example.com", "https://example.com"},
		{"token as username", "https://tokenAsUsername@github.com/org/repo", "https://github.com/org/repo"},
		{"percent-encoded at in password", "http://user:p%40secret@host", "http://host"},
		{"unencoded at in password", "https://user:p@secret@host", "https://host"},
		{"ipv6 host", "https://user:secret@[::1]:443/x", "https://[::1]:443/x"},
		{"ipv6 host without userinfo", "https://[::1]:443/x", "https://[::1]:443/x"},
		{"bare host", "example.com", "example.com"},
		{"schemeless user and password", "user:secret@host", "host"},
		{"schemeless token", "secret@10.0.0.1", "10.0.0.1"},

		// Unparseable: the password holds a character that ends the authority
		// or is invalid in userinfo. Only the last-"@" fallback can help.
		{"unparseable with space", "http://us er:secret@example.com", "http://example.com"},
		{"unparseable with quote", `https://user:pa"secret@host`, "https://host"},
		{"unparseable with slash", "https://user:secret/x@host", "https://host"},
		{"unparseable with question mark", "https://user:secret?x@host", "https://host"},
		{"unparseable with hash", "https://user:secret#x@host", "https://host"},

		// An "@" outside userinfo used to fall into the truncation branch and
		// rewrite the endpoint to a different host: https://b.c, https://alice.
		{"at in query", "https://h.example/?e=a@b.c", "https://h.example/?e=a@b.c"},
		{"at in path", "https://h.example/p@x", "https://h.example/p@x"},
		{"mastodon-style path", "https://mastodon.example/@alice", "https://mastodon.example/@alice"},
		{"at in fragment", "https://h.example/#f@g", "https://h.example/#f@g"},
		{"credentials and at in query", "https://u:secret@h.example/?e=a@b.c", "https://h.example/?e=a@b.c"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := redactEndpoint(tc.in)
			require.Equal(t, tc.want, got)
			require.NotContains(t, got, "secret", "credential must never survive redaction")
		})
	}
}

// redactEndpoint assumes a URL. Applied to a free-form error message it
// truncated everything before the last "@", discarding the failure detail the
// message exists to carry.
func TestRedactMessagePreservesText(t *testing.T) {
	cases := []struct{ name, in, wantContains, wantAbsent string }{
		{
			name:         "credential in embedded url is stripped",
			in:           `Head "https://admin:hunter2@example.com": dial tcp: connection refused`,
			wantContains: `Head "https://example.com": dial tcp: connection refused`,
			wantAbsent:   "hunter2",
		},
		{
			// The schemeless pattern now strips "user@" here as well. The
			// message still carries its failure detail, which is what this
			// case guards.
			name:         "unrelated at-sign does not truncate the message",
			in:           "lookup user@host failed: no such host",
			wantContains: "lookup host failed: no such host",
			wantAbsent:   "",
		},
		{
			name:         "plain message is untouched",
			in:           "context deadline exceeded",
			wantContains: "context deadline exceeded",
			wantAbsent:   "",
		},
		{
			// The match must stop at the URL authority: an "@" in a query
			// string would otherwise swallow the host on the way to it.
			name:         "at-sign in a query string does not eat the host",
			in:           "GET https://api.example.test?email=user@example.test failed",
			wantContains: "GET https://api.example.test?email=user@example.test failed",
			wantAbsent:   "",
		},
		{
			name:         "at-sign in a fragment does not eat the host",
			in:           "https://host.example#frag@anchor unreachable",
			wantContains: "https://host.example#frag@anchor unreachable",
			wantAbsent:   "",
		},
		{
			name:         "mastodon-style path keeps its host",
			in:           `Get "https://mastodon.example/@alice": 404`,
			wantContains: `Get "https://mastodon.example/@alice": 404`,
			wantAbsent:   "",
		},
		{
			name:         "credential still stripped when a query follows",
			in:           `Get "https://admin:hunter2@example.com/health?verbose=1": timeout`,
			wantContains: "verbose=1",
			wantAbsent:   "hunter2",
		},
		{
			name:         "token as username",
			in:           `Get "https://tokenAsUsername@github.com": EOF`,
			wantContains: `Get "https://github.com": EOF`,
			wantAbsent:   "secret",
		},
		{
			name:         "unencoded at in password",
			in:           "dial https://user:p@secret@host/x refused",
			wantContains: "dial https://host/x refused",
			wantAbsent:   "secret",
		},
		{
			name:         "two urls in one message",
			in:           "Get https://a:secret1@h1/ then https://c:secret2@h2/",
			wantContains: "Get https://h1/ then https://h2/",
			wantAbsent:   "secret",
		},
		{
			name:         "ipv6 host",
			in:           `Get "https://u:secret@[::1]:443/x": refused`,
			wantContains: `Get "https://[::1]:443/x": refused`,
			wantAbsent:   "secret",
		},
		{
			name:         "schemeless user and password",
			in:           "lookup user:secret@host: no such host",
			wantContains: "lookup host: no such host",
			wantAbsent:   "secret",
		},
		{
			name:         "schemeless in a dial error",
			in:           "dial tcp: lookup user:secret@host",
			wantContains: "dial tcp: lookup host",
			wantAbsent:   "secret",
		},
		{
			name:         "schemeless at the start of the message",
			in:           "user:secret@host: unreachable",
			wantContains: "host: unreachable",
			wantAbsent:   "secret",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := redactMessage(tc.in)
			require.Contains(t, got, tc.wantContains)
			if tc.wantAbsent != "" {
				require.NotContains(t, got, tc.wantAbsent)
			}
		})
	}
}

// newRequestErr returns the *url.Error net/http produces for an endpoint
// url.Parse rejects. That error quotes the raw URL, and the characters that
// make the URL unparseable are the same ones that stop the free-text patterns
// short of the "@".
func newRequestErr(t *testing.T, endpoint string) error {
	t.Helper()
	_, err := http.NewRequestWithContext(context.Background(), http.MethodHead, endpoint, nil)
	var ue *url.Error
	require.ErrorAs(t, err, &ue, "endpoint %q must fail to parse", endpoint)
	return err
}

func TestRedactErr(t *testing.T) {
	require.NoError(t, redactErr(nil))

	cases := []struct {
		name   string
		err    error
		want   string // exact text when non-empty
		reason string // must survive when non-empty
	}{
		{
			name:   "quote in password",
			err:    newRequestErr(t, `https://user:pa"secret@host`),
			want:   "parse url: net/url: invalid userinfo",
			reason: "invalid userinfo",
		},
		{
			name:   "space in password",
			err:    newRequestErr(t, "https://user:pa secret@host"),
			reason: "invalid userinfo",
		},
		{
			// url.Parse quotes the would-be port, which is the password.
			name: "slash in password",
			err:  newRequestErr(t, "https://user:secret/x@host"),
			want: `parse url: invalid port "***" after host`,
		},
		{
			name:   "question mark in password",
			err:    newRequestErr(t, "https://user:secret?x@host"),
			reason: "invalid port",
		},
		{
			// url.Parse cuts the fragment first, so the URL in the error is
			// "https://user:secret" with no "@" for any pattern to anchor on.
			name:   "hash in password",
			err:    newRequestErr(t, "https://user:secret#x@host"),
			reason: "invalid port",
		},
		{
			name:   "wrapped url.Error keeps the outer context",
			err:    fmt.Errorf("building request: %w", newRequestErr(t, `https://user:pa"secret@host`)),
			want:   "building request: parse url: net/url: invalid userinfo",
			reason: "building request",
		},
		{
			name: "request error rebuilds op and url",
			err: &url.Error{
				Op:  "Get",
				URL: "https://secretTOKEN@host/x",
				Err: errors.New("dial tcp: lookup user:secret@proxy: no such host"),
			},
			want: "Get https://host/x: dial tcp: lookup proxy: no such host",
		},
		{
			name: "plain error goes through the free-text patterns",
			err:  errors.New(`Head "https://u:secret@h": EOF; then https://a:secret@b`),
			want: `Head "https://h": EOF; then https://b`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := redactErr(tc.err).Error()
			require.NotContains(t, got, "secret", "credential must never survive redaction")
			if tc.want != "" {
				require.Equal(t, tc.want, got)
			}
			if tc.reason != "" {
				require.Contains(t, got, tc.reason, "the failure reason must survive")
			}
		})
	}
}
