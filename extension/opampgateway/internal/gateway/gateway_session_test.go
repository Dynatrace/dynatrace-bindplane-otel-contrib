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
	"encoding/json"
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
		_, oldOK := h.gateway.server.getAgentConnection(agent.ID())
		_, newOK := h.gateway.server.getAgentConnection(newID.String())
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
		_, ok := outer.gateway.server.getAgentConnection(agent.ID())
		return !ok
	}, 5*time.Second, 50*time.Millisecond, "the outer gateway stops routing to the agent")
	upstream.RequireNoAgentMessage(t, agent.ID(), 300*time.Millisecond)
}

func disconnectAgentMessage(t *testing.T, instanceUID []byte) *protobufs.ServerToAgent {
	t.Helper()
	data, err := json.Marshal(OpampGatewayDisconnectAgent{InstanceUID: instanceUID, RetryAfterSeconds: 300})
	require.NoError(t, err)
	return &protobufs.ServerToAgent{
		CustomMessage: &protobufs.CustomMessage{
			Capability: OpampGatewayCapability,
			Type:       OpampGatewayDisconnectAgentType,
			Data:       data,
		},
	}
}

func TestGatewayAdvertisesDisconnectAgent(t *testing.T) {
	t.Parallel()

	h := newGatewayTestHarness(t, 2)
	require.Eventually(t, func() bool { return len(h.upstream.Headers()) == 2 }, 5*time.Second, 50*time.Millisecond)
	for id, header := range h.upstream.Headers() {
		require.Contains(t, header.Values("X-Opamp-Gateway-Capabilities"), OpampGatewayDisconnectAgentType, id)
	}
}

func TestGatewayDisconnectAgentClosesOnlyThatAgent(t *testing.T) {
	t.Parallel()

	h := newGatewayTestHarness(t, 1)
	agent := h.NewAgent(t)
	sibling := h.NewAgent(t)
	agent.Send(&protobufs.AgentToServer{SequenceNum: 1})
	h.upstream.WaitForAgentMessage(t, agent.ID(), 5*time.Second)
	sibling.Send(&protobufs.AgentToServer{SequenceNum: 1})
	h.upstream.WaitForAgentMessage(t, sibling.ID(), 5*time.Second)

	require.NoError(t, h.upstream.Send(disconnectAgentMessage(t, agent.RawID())))
	agent.WaitForClose(t, 5*time.Second)

	require.NoError(t, h.upstream.Send(&protobufs.ServerToAgent{InstanceUid: sibling.RawID(), Capabilities: 5}))
	require.Equal(t, uint64(5), sibling.WaitForMessage(t, 5*time.Second).GetCapabilities())
}

func TestGatewayDisconnectAgentForUnknownAgentIsIgnored(t *testing.T) {
	t.Parallel()

	h := newGatewayTestHarness(t, 1)
	agent := h.NewAgent(t)
	agent.Send(&protobufs.AgentToServer{SequenceNum: 1})
	h.upstream.WaitForAgentMessage(t, agent.ID(), 5*time.Second)

	unknown := uuid.New()
	require.NoError(t, h.upstream.Send(disconnectAgentMessage(t, unknown[:])))
	require.NoError(t, h.upstream.Send(&protobufs.ServerToAgent{InstanceUid: agent.RawID(), Capabilities: 3}))
	require.Equal(t, uint64(3), agent.WaitForMessage(t, 5*time.Second).GetCapabilities())
}

func TestGatewayChainedDisconnectAgentIsForwarded(t *testing.T) {
	t.Parallel()

	outer := newGatewayTestHarness(t, 1)
	inner := newChainedGatewayHarness(t, "inner", outer.agentURL, 1)

	agent := inner.NewAgent(t)
	sibling := inner.NewAgent(t)
	agent.Send(&protobufs.AgentToServer{SequenceNum: 1})
	outer.upstream.WaitForAgentMessage(t, agent.ID(), 5*time.Second)
	sibling.Send(&protobufs.AgentToServer{SequenceNum: 1})
	outer.upstream.WaitForAgentMessage(t, sibling.ID(), 5*time.Second)

	// The outer gateway must not close the connection of the inner gateway, which carries
	// the sibling too. It forwards the request, and the inner gateway closes the agent.
	require.NoError(t, outer.upstream.Send(disconnectAgentMessage(t, agent.RawID())))
	agent.WaitForClose(t, 5*time.Second)

	require.NoError(t, outer.upstream.Send(&protobufs.ServerToAgent{InstanceUid: sibling.RawID(), Capabilities: 4}))
	require.Equal(t, uint64(4), sibling.WaitForMessage(t, 5*time.Second).GetCapabilities())
}
