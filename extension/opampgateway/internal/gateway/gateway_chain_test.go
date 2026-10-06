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
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/extension/opampgateway/internal/metadata"
	"github.com/gorilla/websocket"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/confignet"
	"go.uber.org/zap/zaptest"
)

// newChainedGatewayHarness starts a gateway whose upstream is another gateway (rather than
// an OpAMP server). It returns a harness whose agents connect to this gateway.
func newChainedGatewayHarness(t *testing.T, name, upstreamURL string, upstreamConnections int) *gatewayTestHarness {
	t.Helper()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	testTel := componenttest.NewTelemetry()
	t.Cleanup(func() {
		require.NoError(t, testTel.Shutdown(context.Background()))
	})

	telemetry, err := metadata.NewTelemetryBuilder(testTel.NewTelemetrySettings())
	require.NoError(t, err)

	settings := Settings{
		UpstreamOpAMPAddress: upstreamURL,
		Headers: http.Header{
			"Authorization": []string{"Secret-Key test-secret"},
		},
		UpstreamConnections: upstreamConnections,
		OpAMPServer:         confighttp.ServerConfig{NetAddr: confignet.AddrConfig{Endpoint: "127.0.0.1:0", Transport: confignet.TransportTypeTCP}},
		// keep the auth timeout short so a broken chain fails the test quickly instead of
		// waiting for the 30s default.
		AuthTimeout: 3 * time.Second,
	}

	gw := New(zaptest.NewLogger(t).Named(name), settings, telemetry)
	require.NoError(t, gw.Start(ctx, componenttest.NewNopHost(), testTel.NewTelemetrySettings()))
	t.Cleanup(func() {
		require.NoError(t, gw.Shutdown(context.Background()))
	})

	require.Eventually(t, func() bool {
		for i := 0; i < upstreamConnections; i++ {
			conn, ok := gw.client.upstreamConnections.get(fmt.Sprintf("upstream-%d", i))
			if !ok || !conn.isConnected() {
				return false
			}
		}
		return true
	}, 5*time.Second, 10*time.Millisecond, "%s upstream connections not all connected", name)

	return &gatewayTestHarness{
		t:        t,
		ctx:      ctx,
		cancel:   cancel,
		gateway:  gw,
		agentURL: fmt.Sprintf("ws://%s%s", gw.server.addr.String(), handlePath),
	}
}

// TestGatewayChainedAgentRoundTrip covers an agent that connects through two gateways:
//
//	agent -> gateway B -> gateway A -> upstream OpAMP server
//
// Gateway B authenticates the agent by sending an OpampGatewayConnect message upstream.
// That message is sent by the gateway itself, so it carries no instance_uid. Gateway A must
// relay it to the upstream server and route the OpampGatewayConnectResult back to B.
func TestGatewayChainedAgentRoundTrip(t *testing.T) {
	t.Parallel()

	gwA := newGatewayTestHarness(t, 1)
	gwB := newChainedGatewayHarness(t, "gateway-b", gwA.agentURL, 1)

	// the agent's handshake only succeeds if the connect request was relayed and answered.
	agent := gwB.NewAgent(t)

	agent.Send(&protobufs.AgentToServer{SequenceNum: 1})
	got := gwA.upstream.WaitForAgentMessage(t, agent.ID(), 5*time.Second)
	require.Equal(t, uint64(1), got.Message.GetSequenceNum())

	require.NoError(t, gwA.upstream.Send(&protobufs.ServerToAgent{
		InstanceUid:  agent.RawID(),
		Capabilities: 42,
	}))
	resp := agent.WaitForMessage(t, 5*time.Second)
	require.Equal(t, uint64(42), resp.GetCapabilities())
}

// TestGatewayChainedThreeDeep covers an agent that connects through three gateways.
func TestGatewayChainedThreeDeep(t *testing.T) {
	t.Parallel()

	gwA := newGatewayTestHarness(t, 1)
	gwB := newChainedGatewayHarness(t, "gateway-b", gwA.agentURL, 1)
	gwC := newChainedGatewayHarness(t, "gateway-c", gwB.agentURL, 1)

	agent := gwC.NewAgent(t)

	agent.Send(&protobufs.AgentToServer{SequenceNum: 7})
	got := gwA.upstream.WaitForAgentMessage(t, agent.ID(), 5*time.Second)
	require.Equal(t, uint64(7), got.Message.GetSequenceNum())

	require.NoError(t, gwA.upstream.Send(&protobufs.ServerToAgent{
		InstanceUid:  agent.RawID(),
		Capabilities: 99,
	}))
	resp := agent.WaitForMessage(t, 5*time.Second)
	require.Equal(t, uint64(99), resp.GetCapabilities())
}

// TestGatewayChainedMultipleAgents makes sure connect requests from several agents on the
// same downstream gateway are each routed back to the right connection.
func TestGatewayChainedMultipleAgents(t *testing.T) {
	t.Parallel()

	gwA := newGatewayTestHarness(t, 1)
	gwB := newChainedGatewayHarness(t, "gateway-b", gwA.agentURL, 1)

	agents := []*testAgent{gwB.NewAgent(t), gwB.NewAgent(t), gwB.NewAgent(t)}
	for i, a := range agents {
		a.Send(&protobufs.AgentToServer{SequenceNum: uint64(i + 1)})
	}
	for i, a := range agents {
		got := gwA.upstream.WaitForAgentMessage(t, a.ID(), 5*time.Second)
		require.Equal(t, uint64(i+1), got.Message.GetSequenceNum())
	}
	for i, a := range agents {
		require.NoError(t, gwA.upstream.Send(&protobufs.ServerToAgent{
			InstanceUid:  a.RawID(),
			Capabilities: uint64(100 + i),
		}))
	}
	for i, a := range agents {
		require.Equal(t, uint64(100+i), a.WaitForMessage(t, 5*time.Second).GetCapabilities())
	}
}

// TestGatewayChainedRejectedConnect makes sure a rejection from the upstream server reaches
// the agent through both gateways instead of timing out.
func TestGatewayChainedRejectedConnect(t *testing.T) {
	t.Parallel()

	gwA := newGatewayTestHarness(t, 1)
	gwB := newChainedGatewayHarness(t, "gateway-b", gwA.agentURL, 1)

	// reject only requests for agents (gateway B's own upstream connection is already up)
	gwA.upstream.rejectConnects.Store(true)

	_, resp, err := dialTestAgent(gwB.agentURL)
	require.Error(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)
}

// dialTestAgent opens a websocket to url and returns the raw dial result so tests can
// inspect a failed handshake.
func dialTestAgent(url string) (*websocket.Conn, *http.Response, error) {
	return websocket.DefaultDialer.Dial(url, nil)
}
