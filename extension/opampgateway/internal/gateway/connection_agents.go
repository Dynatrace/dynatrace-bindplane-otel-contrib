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
	"sync"
	"time"
)

// renameGracePeriod is the time after a rename in which a message with the old agent ID does
// not route the old ID. A message that the agent sent before it got the new ID arrives in
// this time. When the agent still uses the old ID after this time, it did not apply the new
// ID, and the gateway uses the old ID again.
const renameGracePeriod = 30 * time.Second

// pendingRename is a rename that the agent did not use yet.
type pendingRename struct {
	newID string
	at    time.Time
}

// connectionAgents tracks the agents on one downstream connection, so that the gateway can
// report them to the upstream server when the connection closes. The connection of an agent
// carries one agent; the connection of a chained gateway carries many.
type connectionAgents struct {
	mtx sync.Mutex

	// active holds the agents to report when the connection closes.
	active map[string]agentID

	// renamed maps an agent ID that the server replaced to the new ID. Until the agent sends
	// a message with the new ID, a late message with the old ID must not make the old ID
	// active again: the server may have given the old ID to a different agent.
	renamed map[string]pendingRename

	// renamedFrom maps a new ID to the old IDs in renamed, so that a message does not scan
	// all of renamed.
	renamedFrom map[string][]string

	closed bool

	now func() time.Time
}

func newConnectionAgents() *connectionAgents {
	return &connectionAgents{
		active:      make(map[string]agentID),
		renamed:     make(map[string]pendingRename),
		renamedFrom: make(map[string][]string),
		now:         time.Now,
	}
}

// seen records a message from the agent. It returns false when the gateway must not route
// the agent ID to this connection. When the agent did not apply a new ID, it also returns
// the new ID, which the gateway must not route anymore.
func (a *connectionAgents) seen(id agentID) (route bool, dropped string) {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	if a.closed {
		return false, ""
	}
	key := id.String()
	// The agent uses its new ID, so its old IDs are free again.
	a.forgetRenamesTo(key)
	if r, ok := a.renamed[key]; ok {
		if a.now().Sub(r.at) < renameGracePeriod {
			return false, ""
		}
		a.forgetRenamesTo(r.newID)
		delete(a.active, r.newID)
		dropped = r.newID
	}
	a.active[key] = id
	return true, dropped
}

// left records that the agent is no longer on the connection, so it is not reported on close.
// When the agent used an old ID that the server replaced, it also returns the new ID, which
// the gateway must not route anymore.
func (a *connectionAgents) left(agentID string) (string, bool) {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	delete(a.active, agentID)
	a.forgetRenamesTo(agentID)
	r, ok := a.renamed[agentID]
	if !ok {
		return "", false
	}
	a.forgetRenamesTo(r.newID)
	delete(a.active, r.newID)
	return r.newID, true
}

// rename records that the server replaced the ID of an agent. It returns false when the
// connection is closed.
func (a *connectionAgents) rename(oldID string, newID agentID) bool {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	if a.closed {
		return false
	}
	newKey := newID.String()
	oldIDs := append(a.renamedFrom[oldID], oldID)
	delete(a.renamedFrom, oldID)
	now := a.now()
	for _, id := range oldIDs {
		if id == newKey {
			continue
		}
		a.renamed[id] = pendingRename{newID: newKey, at: now}
		a.renamedFrom[newKey] = append(a.renamedFrom[newKey], id)
	}
	delete(a.active, oldID)
	a.active[newKey] = newID
	return true
}

// forgetRenamesTo removes the pending renames to the new ID.
func (a *connectionAgents) forgetRenamesTo(newID string) {
	for _, oldID := range a.renamedFrom[newID] {
		delete(a.renamed, oldID)
	}
	delete(a.renamedFrom, newID)
}

// has reports whether the agent is active on an open connection.
func (a *connectionAgents) has(agentID string) bool {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	_, ok := a.active[agentID]
	return ok && !a.closed
}

// close marks the connection closed and returns the agents to report.
func (a *connectionAgents) close() []agentID {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	if a.closed {
		return nil
	}
	a.closed = true
	agentIDs := make([]agentID, 0, len(a.active))
	for _, id := range a.active {
		agentIDs = append(agentIDs, id)
	}
	return agentIDs
}
