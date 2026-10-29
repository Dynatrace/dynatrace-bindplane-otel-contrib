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
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

// testAgentIDs returns n random agent IDs.
func testAgentIDs(t *testing.T, n int) []agentID {
	t.Helper()
	ids := make([]agentID, n)
	for i := range ids {
		u := uuid.New()
		id, err := newAgentID(u[:])
		require.NoError(t, err)
		ids[i] = id
	}
	return ids
}

// seen returns only the route result of connectionAgents.seen.
func seen(a *connectionAgents, id agentID) bool {
	route, _ := a.seen(id)
	return route
}

// newTestConnectionAgents returns connectionAgents with a clock that the test moves.
func newTestConnectionAgents() (*connectionAgents, *time.Time) {
	now := time.Now()
	a := newConnectionAgents()
	a.now = func() time.Time { return now }
	return a, &now
}

// requireNoPendingRenames makes sure that no rename state stays after the agents used or
// left their IDs.
func requireNoPendingRenames(t *testing.T, a *connectionAgents) {
	t.Helper()
	a.mtx.Lock()
	defer a.mtx.Unlock()
	require.Empty(t, a.renamed)
	require.Empty(t, a.renamedFrom)
}

func TestConnectionAgentsReportsActiveAgentsOnClose(t *testing.T) {
	ids := testAgentIDs(t, 2)
	x, y := ids[0], ids[1]
	a := newConnectionAgents()
	require.True(t, seen(a, x))
	require.True(t, seen(a, y))
	_, _ = a.left(y.String())

	require.True(t, a.has(x.String()))
	require.False(t, a.has(y.String()))
	require.Equal(t, []agentID{x}, a.close())
	require.False(t, a.has(x.String()), "a closed connection has no agents")
}

func TestConnectionAgentsIgnoresAgentsAfterClose(t *testing.T) {
	ids := testAgentIDs(t, 2)
	a := newConnectionAgents()
	a.close()
	require.False(t, seen(a, ids[0]))
	require.False(t, a.rename(ids[0].String(), ids[1]))
	require.Empty(t, a.close())
}

func TestConnectionAgentsBlocksOldIDUntilNewIDIsUsed(t *testing.T) {
	ids := testAgentIDs(t, 2)
	x, y := ids[0], ids[1]
	a := newConnectionAgents()
	require.True(t, seen(a, x))
	require.True(t, a.rename(x.String(), y))

	require.False(t, seen(a, x), "a late message with the old ID does not route it back")
	require.False(t, a.has(x.String()))
	require.True(t, a.has(y.String()))

	require.True(t, seen(a, y))
	require.True(t, seen(a, x), "after the agent uses its new ID, the old ID is free again")
	requireNoPendingRenames(t, a)
}

func TestConnectionAgentsFollowsRenameChains(t *testing.T) {
	ids := testAgentIDs(t, 3)
	x, y, z := ids[0], ids[1], ids[2]
	a := newConnectionAgents()
	require.True(t, seen(a, x))
	require.True(t, a.rename(x.String(), y))
	require.True(t, a.rename(y.String(), z))

	require.False(t, seen(a, x))
	require.False(t, seen(a, y))
	require.True(t, seen(a, z))
	require.True(t, seen(a, x))
	require.True(t, seen(a, y))
	requireNoPendingRenames(t, a)
}

func TestConnectionAgentsUsesTheOldIDAgainAfterTheGracePeriod(t *testing.T) {
	ids := testAgentIDs(t, 2)
	x, y := ids[0], ids[1]
	a, now := newTestConnectionAgents()
	require.True(t, seen(a, x))
	require.True(t, a.rename(x.String(), y))

	*now = now.Add(renameGracePeriod - time.Second)
	require.False(t, seen(a, x), "the old ID is blocked during the grace period")

	// The agent did not apply the new ID, so the gateway stops using the new ID.
	*now = now.Add(2 * time.Second)
	route, dropped := a.seen(x)
	require.True(t, route)
	require.Equal(t, y.String(), dropped)
	require.True(t, a.has(x.String()))
	require.False(t, a.has(y.String()))
	require.Equal(t, []agentID{x}, a.close())
	requireNoPendingRenames(t, a)
}

func TestConnectionAgentsAgentThatLeavesWithItsOldIDIsNotReported(t *testing.T) {
	ids := testAgentIDs(t, 2)
	x, y := ids[0], ids[1]
	a := newConnectionAgents()
	require.True(t, seen(a, x))
	require.True(t, a.rename(x.String(), y))

	newID, ok := a.left(x.String())
	require.True(t, ok)
	require.Equal(t, y.String(), newID)
	require.False(t, a.has(y.String()))
	require.Empty(t, a.close(), "the agent already reported that it left")
	requireNoPendingRenames(t, a)
}

func TestConnectionAgentsAgentThatLeavesWithItsNewIDFreesTheOldID(t *testing.T) {
	ids := testAgentIDs(t, 2)
	x, y := ids[0], ids[1]
	a := newConnectionAgents()
	require.True(t, seen(a, x))
	require.True(t, a.rename(x.String(), y))

	_, ok := a.left(y.String())
	require.False(t, ok)
	requireNoPendingRenames(t, a)
	require.True(t, seen(a, x), "a different agent can get the old ID")
}
