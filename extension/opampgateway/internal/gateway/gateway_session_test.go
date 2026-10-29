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
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/stretchr/testify/require"
)

// The server keeps one session for each agent behind a gateway. These tests cover the
// messages that keep those sessions accurate.

func TestGatewayForwardsAgentDisconnectWhenAgentCloses(t *testing.T) {
	t.Parallel()

	h := newGatewayTestHarness(t, 1)
	agent := h.NewAgent(t)
	sibling := h.NewAgent(t)

	agent.Send(&protobufs.AgentToServer{SequenceNum: 1})
	h.upstream.WaitForAgentMessage(t, agent.ID(), 5*time.Second)
	sibling.Send(&protobufs.AgentToServer{SequenceNum: 1})
	h.upstream.WaitForAgentMessage(t, sibling.ID(), 5*time.Second)

	require.NoError(t, agent.Close())

	msg := h.upstream.WaitForAgentMessage(t, agent.ID(), 5*time.Second)
	require.NotNil(t, msg.Message.GetAgentDisconnect(), "the gateway reports the agent that left")

	// The sibling on the same upstream connection is not affected.
	require.NoError(t, h.upstream.Send(&protobufs.ServerToAgent{InstanceUid: sibling.RawID(), Capabilities: 7}))
	require.Equal(t, uint64(7), sibling.WaitForMessage(t, 5*time.Second).GetCapabilities())
}

func TestGatewayAgentThatNeverSentAMessageIsNotReported(t *testing.T) {
	t.Parallel()

	h := newGatewayTestHarness(t, 1)
	agent := h.NewAgent(t)
	require.NoError(t, agent.Close())

	require.Eventually(t, func() bool {
		_, ok := h.gateway.server.getDownstreamConnection(agent.ID())
		return !ok
	}, 5*time.Second, 50*time.Millisecond)
	h.upstream.RequireNoAgentMessage(t, agent.ID(), 300*time.Millisecond)
}

func TestGatewayRemapsAgentOnAgentIdentification(t *testing.T) {
	t.Parallel()

	h := newGatewayTestHarness(t, 1)
	agent := h.NewAgent(t)
	agent.Send(&protobufs.AgentToServer{SequenceNum: 1})
	h.upstream.WaitForAgentMessage(t, agent.ID(), 5*time.Second)

	newID := uuid.New()
	require.NoError(t, h.upstream.Send(&protobufs.ServerToAgent{
		InstanceUid:         agent.RawID(),
		AgentIdentification: &protobufs.AgentIdentification{NewInstanceUid: newID[:]},
	}))
	require.NotNil(t, agent.WaitForMessage(t, 5*time.Second).GetAgentIdentification())

	require.Eventually(t, func() bool {
		_, oldOK := routedConnection(h.gateway, agent.ID())
		_, newOK := routedConnection(h.gateway, newID.String())
		return !oldOK && newOK
	}, 5*time.Second, 50*time.Millisecond, "the route moves to the new instance UID")

	// A message for the new ID reaches the agent before the agent sends anything with it.
	require.NoError(t, h.upstream.Send(&protobufs.ServerToAgent{InstanceUid: newID[:], Capabilities: 9}))
	require.Equal(t, uint64(9), agent.WaitForMessage(t, 5*time.Second).GetCapabilities())
}

func TestGatewayChainedAgentDisconnectIsReportedOnce(t *testing.T) {
	t.Parallel()

	outer := newGatewayTestHarness(t, 1)
	inner := newChainedGatewayHarness(t, "inner", outer.agentURL, 1)
	upstream := outer.upstream

	agent := inner.NewAgent(t)
	agent.Send(&protobufs.AgentToServer{SequenceNum: 1})
	upstream.WaitForAgentMessage(t, agent.ID(), 5*time.Second)

	require.NoError(t, agent.Close())
	msg := upstream.WaitForAgentMessage(t, agent.ID(), 5*time.Second)
	require.NotNil(t, msg.Message.GetAgentDisconnect())

	require.Eventually(t, func() bool {
		_, ok := routedConnection(outer.gateway, agent.ID())
		return !ok
	}, 5*time.Second, 50*time.Millisecond, "the outer gateway stops routing to the agent")
	upstream.RequireNoAgentMessage(t, agent.ID(), 300*time.Millisecond)
}

func TestGatewayDoesNotReportAgentsAfterUpstreamLoss(t *testing.T) {
	t.Parallel()

	h := newGatewayTestHarness(t, 1)
	agent := h.NewAgent(t)
	agent.Send(&protobufs.AgentToServer{SequenceNum: 1})
	h.upstream.WaitForAgentMessage(t, agent.ID(), 5*time.Second)

	// The server ends the sessions of all agents on the upstream connection when it closes,
	// so a report on the next upstream connection is stale.
	h.upstream.mu.Lock()
	for _, c := range h.upstream.connections {
		_ = c.conn.Close()
	}
	h.upstream.mu.Unlock()

	h.upstream.WaitForConnection(t, 10*time.Second)
	require.Eventually(t, func() bool {
		return h.gateway.server.downstreamConnections.size() == 0
	}, 10*time.Second, 50*time.Millisecond)
	h.upstream.RequireNoAgentMessage(t, agent.ID(), time.Second)
}

func TestGatewayLateMessageWithOldIDIsNotReported(t *testing.T) {
	t.Parallel()

	h := newGatewayTestHarness(t, 1)
	agent := h.NewAgent(t)
	agent.Send(&protobufs.AgentToServer{SequenceNum: 1})
	h.upstream.WaitForAgentMessage(t, agent.ID(), 5*time.Second)

	newID := uuid.New()
	require.NoError(t, h.upstream.Send(&protobufs.ServerToAgent{
		InstanceUid:         agent.RawID(),
		AgentIdentification: &protobufs.AgentIdentification{NewInstanceUid: newID[:]},
	}))
	agent.WaitForMessage(t, 5*time.Second)

	// The agent sends one more message with the old ID before it applies the new one.
	agent.Send(&protobufs.AgentToServer{InstanceUid: agent.RawID(), SequenceNum: 2})
	h.upstream.WaitForAgentMessage(t, agent.ID(), 5*time.Second)
	_, ok := routedConnection(h.gateway, agent.ID())
	require.False(t, ok, "the old ID is not routed to the agent again")

	require.NoError(t, agent.Close())
	msg := h.upstream.WaitForAgentMessage(t, newID.String(), 5*time.Second)
	require.NotNil(t, msg.Message.GetAgentDisconnect())
	h.upstream.RequireNoAgentMessage(t, agent.ID(), 300*time.Millisecond)
}

func TestGatewayReassignmentToTheSameIDKeepsTheRoute(t *testing.T) {
	t.Parallel()

	h := newGatewayTestHarness(t, 1)
	agent := h.NewAgent(t)
	agent.Send(&protobufs.AgentToServer{SequenceNum: 1})
	h.upstream.WaitForAgentMessage(t, agent.ID(), 5*time.Second)

	require.NoError(t, h.upstream.Send(&protobufs.ServerToAgent{
		InstanceUid:         agent.RawID(),
		AgentIdentification: &protobufs.AgentIdentification{NewInstanceUid: agent.RawID()},
	}))
	agent.WaitForMessage(t, 5*time.Second)

	require.NoError(t, h.upstream.Send(&protobufs.ServerToAgent{InstanceUid: agent.RawID(), Capabilities: 5}))
	require.Equal(t, uint64(5), agent.WaitForMessage(t, 5*time.Second).GetCapabilities())
}

func TestGatewayDoesNotReportAnIDThatAnotherConnectionUses(t *testing.T) {
	t.Parallel()

	h := newGatewayTestHarness(t, 1)
	id := uuid.New()
	other := newTestAgent(t, h.agentURL, id[:])
	other.Send(&protobufs.AgentToServer{SequenceNum: 1})
	h.upstream.WaitForAgentMessage(t, other.ID(), 5*time.Second)
	closing := newTestAgent(t, h.agentURL, id[:])
	closing.Send(&protobufs.AgentToServer{SequenceNum: 1})
	h.upstream.WaitForAgentMessage(t, closing.ID(), 5*time.Second)

	require.NoError(t, closing.Close())
	h.upstream.RequireNoAgentMessage(t, other.ID(), 500*time.Millisecond)

	// The route moves to the connection that still uses the ID.
	require.NoError(t, h.upstream.Send(&protobufs.ServerToAgent{InstanceUid: other.RawID(), Capabilities: 3}))
	require.Equal(t, uint64(3), other.WaitForMessage(t, 5*time.Second).GetCapabilities())
}

func TestGatewayDuplicateIDRoutesToTheAgentOnTheUpstreamConnection(t *testing.T) {
	t.Parallel()

	h := newGatewayTestHarness(t, 2)
	id := uuid.New()

	// The pending agent is silent while it waits, so the active agent sends the ID last.
	pending := newTestAgent(t, h.agentURL, id[:])
	pending.Send(&protobufs.AgentToServer{SequenceNum: 1})
	pendingUpstream := h.upstream.WaitForAgentMessage(t, pending.ID(), 5*time.Second).ConnectionID
	active := newTestAgent(t, h.agentURL, id[:])
	active.Send(&protobufs.AgentToServer{SequenceNum: 1})
	activeUpstream := h.upstream.WaitForAgentMessage(t, active.ID(), 5*time.Second).ConnectionID
	require.NotEqual(t, pendingUpstream, activeUpstream, "the agents use different upstream connections")

	// The server reassigns the agent of the session on the upstream connection of the pending agent.
	newID := uuid.New()
	require.NoError(t, h.upstream.SendOn(pendingUpstream, &protobufs.ServerToAgent{
		InstanceUid:         id[:],
		AgentIdentification: &protobufs.AgentIdentification{NewInstanceUid: newID[:]},
	}))
	require.Equal(t, newID[:], pending.WaitForMessage(t, 5*time.Second).GetAgentIdentification().GetNewInstanceUid())

	// The active agent keeps the ID: its next message is the one for it, not the reassignment.
	require.NoError(t, h.upstream.SendOn(activeUpstream, &protobufs.ServerToAgent{InstanceUid: id[:], Capabilities: 5}))
	msg := active.WaitForMessage(t, 5*time.Second)
	require.Nil(t, msg.GetAgentIdentification())
	require.Equal(t, uint64(5), msg.GetCapabilities())
}

func TestGatewayReportsADuplicateIDOnItsOwnUpstreamConnection(t *testing.T) {
	t.Parallel()

	h := newGatewayTestHarness(t, 2)
	id := uuid.New()

	closing := newTestAgent(t, h.agentURL, id[:])
	closing.Send(&protobufs.AgentToServer{SequenceNum: 1})
	closingUpstream := h.upstream.WaitForAgentMessage(t, closing.ID(), 5*time.Second).ConnectionID
	other := newTestAgent(t, h.agentURL, id[:])
	other.Send(&protobufs.AgentToServer{SequenceNum: 1})
	otherUpstream := h.upstream.WaitForAgentMessage(t, other.ID(), 5*time.Second).ConnectionID
	require.NotEqual(t, closingUpstream, otherUpstream, "the agents use different upstream connections")

	// The session on the upstream connection of the closing agent has no other agent, so the
	// gateway reports it, although the agent on the other upstream connection has the same ID.
	require.NoError(t, closing.Close())
	msg := h.upstream.WaitForAgentMessage(t, id.String(), 5*time.Second)
	require.NotNil(t, msg.Message.GetAgentDisconnect())
	require.Equal(t, closingUpstream, msg.ConnectionID)

	// A late message for the ended session does not go to the agent on the other upstream
	// connection.
	require.NoError(t, h.upstream.SendOn(closingUpstream, &protobufs.ServerToAgent{InstanceUid: id[:], Capabilities: 9}))
	require.NoError(t, h.upstream.SendOn(otherUpstream, &protobufs.ServerToAgent{InstanceUid: id[:], Capabilities: 5}))
	require.Equal(t, uint64(5), other.WaitForMessage(t, 5*time.Second).GetCapabilities())
}
