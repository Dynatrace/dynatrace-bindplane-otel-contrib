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

func TestAgentRoutesKeepsOneRouteForEachUpstreamConnection(t *testing.T) {
	upstreamA, upstreamB := &upstreamConnection{id: "a"}, &upstreamConnection{id: "b"}
	onA := &downstreamConnection{upstreamConnection: upstreamA}
	onB := &downstreamConnection{upstreamConnection: upstreamB}

	r := newAgentRoutes()
	r.set("x", onA)
	r.set("x", onB)

	conn, ok := r.get(upstreamA, "x")
	require.True(t, ok)
	require.Same(t, onA, conn)
	conn, ok = r.get(upstreamB, "x")
	require.True(t, ok)
	require.Same(t, onB, conn)

	r.remove("x", onA)
	_, ok = r.get(upstreamA, "x")
	require.False(t, ok, "the route on one upstream connection does not fall back to another")
}

func TestAgentRoutesFallsBackToTheEarlierConnection(t *testing.T) {
	upstream := &upstreamConnection{id: "a"}
	first := &downstreamConnection{upstreamConnection: upstream}
	second := &downstreamConnection{upstreamConnection: upstream}

	r := newAgentRoutes()
	r.set("x", first)
	r.set("x", second)
	conn, _ := r.get(upstream, "x")
	require.Same(t, second, conn, "the connection that used the ID last gets the route")

	r.set("x", first)
	conn, _ = r.get(upstream, "x")
	require.Same(t, first, conn)

	r.removeConnection(first)
	conn, ok := r.get(upstream, "x")
	require.True(t, ok)
	require.Same(t, second, conn)

	r.remove("x", second)
	require.Zero(t, r.size())
}

func TestAgentRoutesRemovesAllRoutesOfAConnection(t *testing.T) {
	upstream := &upstreamConnection{id: "a"}
	conn := &downstreamConnection{upstreamConnection: upstream}

	r := newAgentRoutes()
	r.set("x", conn)
	r.set("y", conn)
	r.removeConnection(conn)

	require.Zero(t, r.size())
	require.Empty(t, r.byConnection)
}
