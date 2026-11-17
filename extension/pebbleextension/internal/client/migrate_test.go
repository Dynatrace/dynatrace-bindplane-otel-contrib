// Copyright Dynatrace LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package client

import (
	"testing"

	pebblev1 "github.com/cockroachdb/pebble"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestNewClient_OpensLegacyV1Store writes a store the way the pebble v1 client did
// (default options, FormatMostCompatible) and checks the v2 client can read it.
func TestNewClient_OpensLegacyV1Store(t *testing.T) {
	dir := t.TempDir()

	db, err := pebblev1.Open(dir, &pebblev1.Options{})
	require.NoError(t, err)
	require.NoError(t, db.Set([]byte("key"), []byte("value"), pebblev1.Sync))
	require.NoError(t, db.Close())

	// Twice: the first open migrates, the second must find nothing left to do.
	for range 2 {
		c, err := NewClient(dir, zap.NewNop(), &Options{})
		require.NoError(t, err)
		v, err := c.Get(t.Context(), "key")
		require.NoError(t, err)
		require.Equal(t, []byte("value"), v)
		require.NoError(t, c.Close(t.Context()))
	}
}
