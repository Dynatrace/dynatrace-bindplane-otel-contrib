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
	"regexp"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var objectNamePattern = regexp.MustCompile(`^exa-cribl-logs_[0-9a-f-]{36}_1_\d{4}_\d{2}_\d{2}_\d{15}_\d{16}\.log\.gz$`)

func TestObjectName(t *testing.T) {
	ts := time.Date(2023, 6, 29, 13, 18, 20, 126, time.FixedZone("x", -7*3600))
	require.Equal(t,
		"exa-cribl-logs_a0d78162-e407-4079-b06c-62df4ed0e71a_1_2023_06_29_201820000000126_0000008520776225.log.gz",
		objectName("a0d78162-e407-4079-b06c-62df4ed0e71a", ts, 8520776225),
	)
}

func TestNewObjectName(t *testing.T) {
	seen := map[string]bool{}
	for range 50 {
		name, err := newObjectName(testCollectorID, time.Now())
		require.NoError(t, err)
		require.Regexp(t, objectNamePattern, name)
		require.NotContains(t, name, "/", "objects must be at the bucket root")
		seen[name] = true
	}
	require.Greater(t, len(seen), 45, "random segment should make names unique")
}
