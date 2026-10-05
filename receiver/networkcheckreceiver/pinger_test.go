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
	"errors"
	"net"
	"net/url"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestNameDNSServer(t *testing.T) {
	inner := &net.DNSError{Err: "no such host", Name: "h.test", Server: "127.0.0.11:53", IsNotFound: true}
	err := &url.Error{Op: "Get", URL: "http://h.test/", Err: &net.OpError{Op: "dial", Net: "tcp", Err: inner}}

	got := nameDNSServer(err, "[::1]:53")
	require.Same(t, error(err), got, "rewritten in place so the wrapping survives")
	require.Contains(t, got.Error(), `Get "http://h.test/"`)
	require.Contains(t, got.Error(), "on [::1]:53")
	require.NotContains(t, got.Error(), "127.0.0.11")

	plain := errors.New("not a dns error")
	require.Same(t, plain, nameDNSServer(plain, "[::1]:53"))

	untouched := &net.DNSError{Server: "127.0.0.11:53"}
	_ = nameDNSServer(untouched, "")
	require.Equal(t, "127.0.0.11:53", untouched.Server, "no override, no rewrite")
}
