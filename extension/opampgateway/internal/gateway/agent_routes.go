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
	"slices"
	"sync"
)

// agentRouteKey identifies the session of an agent. The server keeps one session for each
// agent ID on each upstream connection, and sends the messages of the session on that
// upstream connection.
type agentRouteKey struct {
	upstream *upstreamConnection
	agentID  string
}

// agentRoutes maps the session of an agent to the downstream connections that use the agent
// ID on the upstream connection of the session. Agents with a duplicate ID can share one
// session from more than one downstream connection. The connection that used the ID last
// gets the messages of the session. When it stops, the connection before it gets them.
type agentRoutes struct {
	mtx sync.RWMutex

	byKey map[agentRouteKey][]*downstreamConnection

	// byConnection holds the keys of each connection, so that a closed connection is removed
	// without a scan of all the routes.
	byConnection map[*downstreamConnection]map[agentRouteKey]struct{}
}

func newAgentRoutes() *agentRoutes {
	return &agentRoutes{
		byKey:        make(map[agentRouteKey][]*downstreamConnection),
		byConnection: make(map[*downstreamConnection]map[agentRouteKey]struct{}),
	}
}

// get returns the downstream connection for the agent ID on the upstream connection.
func (r *agentRoutes) get(upstream *upstreamConnection, agentID string) (*downstreamConnection, bool) {
	r.mtx.RLock()
	defer r.mtx.RUnlock()

	conns := r.byKey[agentRouteKey{upstream: upstream, agentID: agentID}]
	if len(conns) == 0 {
		return nil, false
	}
	return conns[len(conns)-1], true
}

// set routes the agent ID on the upstream connection of the downstream connection to that
// downstream connection.
func (r *agentRoutes) set(agentID string, conn *downstreamConnection) {
	key := agentRouteKey{upstream: conn.upstreamConnection, agentID: agentID}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	conns := r.byKey[key]
	if n := len(conns); n > 0 && conns[n-1] == conn {
		return
	}
	r.byKey[key] = append(slices.DeleteFunc(conns, func(c *downstreamConnection) bool { return c == conn }), conn)

	keys, ok := r.byConnection[conn]
	if !ok {
		keys = make(map[agentRouteKey]struct{})
		r.byConnection[conn] = keys
	}
	keys[key] = struct{}{}
}

// remove stops routing the agent ID to the downstream connection.
func (r *agentRoutes) remove(agentID string, conn *downstreamConnection) {
	key := agentRouteKey{upstream: conn.upstreamConnection, agentID: agentID}

	r.mtx.Lock()
	defer r.mtx.Unlock()

	r.removeLocked(key, conn)
	if keys, ok := r.byConnection[conn]; ok {
		delete(keys, key)
		if len(keys) == 0 {
			delete(r.byConnection, conn)
		}
	}
}

// removeConnection stops routing all agent IDs to the downstream connection.
func (r *agentRoutes) removeConnection(conn *downstreamConnection) {
	r.mtx.Lock()
	defer r.mtx.Unlock()

	for key := range r.byConnection[conn] {
		r.removeLocked(key, conn)
	}
	delete(r.byConnection, conn)
}

func (r *agentRoutes) removeLocked(key agentRouteKey, conn *downstreamConnection) {
	conns := slices.DeleteFunc(r.byKey[key], func(c *downstreamConnection) bool { return c == conn })
	if len(conns) == 0 {
		delete(r.byKey, key)
		return
	}
	r.byKey[key] = conns
}

// size returns the number of routed sessions.
func (r *agentRoutes) size() int {
	r.mtx.RLock()
	defer r.mtx.RUnlock()
	return len(r.byKey)
}
