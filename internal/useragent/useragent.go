// Copyright  Dynatrace LLC
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

// Package useragent builds the User-Agent values that components send to the
// services they call, from the collector distribution's component.BuildInfo.
// It is distribution-neutral: the name and version come from whichever
// collector the component is built into.
package useragent // import "github.com/dynatrace/dynatrace-bindplane-otel-contrib/internal/useragent"

import (
	"net/http"
	"runtime"
	"strings"

	"go.opentelemetry.io/collector/component"
)

// tchars are the punctuation characters RFC 9110 allows in a token, in
// addition to letters and digits.
const tchars = "!#$%&'*+-.^_`|~"

// Name returns the collector distribution's name as an RFC 9110 token, taken
// from BuildInfo.Command. Command is a path in some distributions, so only its
// base name is used. It returns an empty string when the build info does not
// name the collector.
func Name(buildInfo component.BuildInfo) string {
	return token(strings.TrimSuffix(baseName(buildInfo.Command), ".exe"))
}

// Version returns BuildInfo.Version as an RFC 9110 token, or an empty string
// when it holds nothing usable.
func Version(buildInfo component.BuildInfo) string {
	return token(buildInfo.Version)
}

// Product returns the collector distribution as a User-Agent product,
// "<name>/<version>". It returns an empty string when either part is missing,
// so callers can leave the User-Agent untouched.
func Product(buildInfo component.BuildInfo) string {
	name, version := Name(buildInfo), Version(buildInfo)
	if name == "" || version == "" {
		return ""
	}
	return name + "/" + version
}

// String returns a complete User-Agent value naming the collector distribution
// and the platform, for example:
//
//	com.dynatrace.bindplane.otel.collector/v1.4.0 (linux/amd64)
//
// It returns an empty string when Product does.
func String(buildInfo component.BuildInfo) string {
	product := Product(buildInfo)
	if product == "" {
		return ""
	}
	return product + " (" + runtime.GOOS + "/" + runtime.GOARCH + ")"
}

// NewTransport wraps base so that requests without a User-Agent header are
// sent with userAgent. A User-Agent already on the request, such as one from
// configured headers, is left alone. A nil base means http.DefaultTransport.
// When userAgent is empty, base is returned unchanged.
func NewTransport(base http.RoundTripper, userAgent string) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	if userAgent == "" {
		return base
	}
	return &transport{base: base, userAgent: userAgent}
}

type transport struct {
	base      http.RoundTripper
	userAgent string
}

// RoundTrip implements http.RoundTripper.
func (t *transport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Header.Get("User-Agent") != "" {
		return t.base.RoundTrip(req)
	}
	// A RoundTripper must not modify the request it is given.
	req = req.Clone(req.Context())
	req.Header.Set("User-Agent", t.userAgent)
	return t.base.RoundTrip(req)
}

// baseName returns the last element of a path. Unlike filepath.Base it treats
// both separators on every platform, because the build info of a binary built
// for one platform can be read on another.
func baseName(path string) string {
	if i := strings.LastIndexAny(path, `/\`); i >= 0 {
		return path[i+1:]
	}
	return path
}

// token renders s as an RFC 9110 token, replacing the characters that are not
// allowed in one with a hyphen so that a value carrying spaces or path
// separators cannot split the User-Agent into extra products. It returns an
// empty string for a value that holds nothing usable.
func token(s string) string {
	var b strings.Builder
	b.Grow(len(s))
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9':
			b.WriteRune(r)
		case strings.ContainsRune(tchars, r):
			b.WriteRune(r)
		default:
			b.WriteRune('-')
		}
	}
	return strings.Trim(b.String(), "-.")
}
