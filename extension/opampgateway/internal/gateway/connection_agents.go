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

import "sync"

// connectionAgents tracks the agents on one downstream connection, so that the gateway can
// report them to the upstream server when the connection closes. The connection of an agent
// carries one agent; the connection of a chained gateway carries many.
type connectionAgents struct {
	mtx sync.Mutex

	// active holds the agents to report when the connection closes.
	active map[string]struct{}

	// renamed maps an agent ID that the server replaced to the new ID. Until the agent sends
	// a message with the new ID, a late message with the old ID must not make the old ID
	// active again: the server may have given the old ID to a different agent.
	renamed map[string]string

	closed bool
}

func newConnectionAgents() *connectionAgents {
	return &connectionAgents{
		active:  make(map[string]struct{}),
		renamed: make(map[string]string),
	}
}

// seen records a message from the agent. It returns false when the gateway must not route
// the agent ID to this connection.
func (a *connectionAgents) seen(agentID string) bool {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	if a.closed {
		return false
	}
	// The agent uses its new ID, so its old IDs are free again.
	for oldID, newID := range a.renamed {
		if newID == agentID {
			delete(a.renamed, oldID)
		}
	}
	if _, ok := a.renamed[agentID]; ok {
		return false
	}
	a.active[agentID] = struct{}{}
	return true
}

// left records that the agent is no longer on the connection, so it is not reported on close.
func (a *connectionAgents) left(agentID string) {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	delete(a.active, agentID)
}

// rename records that the server replaced the ID of an agent. It returns false when the
// connection is closed.
func (a *connectionAgents) rename(oldID, newID string) bool {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	if a.closed {
		return false
	}
	for id, target := range a.renamed {
		if target == oldID {
			a.renamed[id] = newID
		}
	}
	delete(a.active, oldID)
	a.renamed[oldID] = newID
	a.active[newID] = struct{}{}
	return true
}

// has reports whether the agent is active on an open connection.
func (a *connectionAgents) has(agentID string) bool {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	_, ok := a.active[agentID]
	return ok && !a.closed
}

// close marks the connection closed and returns the agents to report.
func (a *connectionAgents) close() []string {
	a.mtx.Lock()
	defer a.mtx.Unlock()

	if a.closed {
		return nil
	}
	a.closed = true
	agentIDs := make([]string, 0, len(a.active))
	for id := range a.active {
		agentIDs = append(agentIDs, id)
	}
	return agentIDs
}
