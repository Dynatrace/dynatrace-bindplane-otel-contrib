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

package gateway

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestConnectionAgentsReportsActiveAgentsOnClose(t *testing.T) {
	a := newConnectionAgents()
	require.True(t, a.seen("x"))
	require.True(t, a.seen("y"))
	a.left("y")

	require.True(t, a.has("x"))
	require.False(t, a.has("y"))
	require.ElementsMatch(t, []string{"x"}, a.close())
	require.False(t, a.has("x"), "a closed connection has no agents")
}

func TestConnectionAgentsIgnoresAgentsAfterClose(t *testing.T) {
	a := newConnectionAgents()
	a.close()
	require.False(t, a.seen("x"))
	require.False(t, a.rename("x", "y"))
	require.Empty(t, a.close())
}

func TestConnectionAgentsBlocksOldIDUntilNewIDIsUsed(t *testing.T) {
	a := newConnectionAgents()
	require.True(t, a.seen("x"))
	require.True(t, a.rename("x", "y"))

	require.False(t, a.seen("x"), "a late message with the old ID does not route it back")
	require.False(t, a.has("x"))
	require.True(t, a.has("y"))

	require.True(t, a.seen("y"))
	require.True(t, a.seen("x"), "after the agent uses its new ID, the old ID is free again")
}

func TestConnectionAgentsFollowsRenameChains(t *testing.T) {
	a := newConnectionAgents()
	require.True(t, a.seen("x"))
	require.True(t, a.rename("x", "y"))
	require.True(t, a.rename("y", "z"))

	require.False(t, a.seen("x"))
	require.False(t, a.seen("y"))
	require.True(t, a.seen("z"))
	require.True(t, a.seen("x"))
	require.True(t, a.seen("y"))
}

func TestConnectionAgentsDiscardReportsNothing(t *testing.T) {
	a := newConnectionAgents()
	require.True(t, a.seen("x"))
	a.discard()

	require.False(t, a.seen("x"), "a message read after the discard does not make the agent active")
	require.Empty(t, a.close())
}
