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

	jsoniter "github.com/json-iterator/go"
	"github.com/open-telemetry/opamp-go/protobufs"
)

// When a gateway's upstream is another gateway (rather than the OpAMP server), the upstream
// gateway is the one that receives the OpampGatewayConnect messages that the downstream
// gateway creates to authenticate its agents. Those messages are created by the gateway
// itself, not by an agent, so they carry no instance_uid. The upstream gateway cannot route
// them by agent ID like other messages, so it relays them to the OpAMP server and routes the
// matching OpampGatewayConnectResult back to the downstream gateway by request UID.

// chainedConnects tracks OpampGatewayConnect requests that were relayed upstream on behalf
// of a downstream gateway, so the matching OpampGatewayConnectResult can be routed back to
// the gateway that sent the request.
type chainedConnects struct {
	mtx sync.Mutex

	// requests maps a request UID to the downstream connection awaiting the result.
	requests map[string]*downstreamConnection
}

func newChainedConnects() *chainedConnects {
	return &chainedConnects{
		requests: make(map[string]*downstreamConnection),
	}
}

// add records that the given downstream connection is waiting for the result of the request
// with the given UID.
func (c *chainedConnects) add(requestUID string, conn *downstreamConnection) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	c.requests[requestUID] = conn
}

// take removes and returns the downstream connection waiting for the result of the request
// with the given UID.
func (c *chainedConnects) take(requestUID string) (*downstreamConnection, bool) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	conn, ok := c.requests[requestUID]
	if ok {
		delete(c.requests, requestUID)
	}
	return conn, ok
}

// removeConnection forgets every request that is waiting on the given downstream connection.
// It is called when the connection closes, since nothing is left to deliver the result to.
func (c *chainedConnects) removeConnection(conn *downstreamConnection) {
	c.mtx.Lock()
	defer c.mtx.Unlock()
	for requestUID, waiting := range c.requests {
		if waiting == conn {
			delete(c.requests, requestUID)
		}
	}
}

// chainedConnectRequestUID reports whether the message is an OpampGatewayConnect request that
// a downstream gateway created on behalf of one of its agents. Such a message has no
// instance_uid. Messages that do have an instance_uid are never treated as chained connect
// requests, so they keep going through the regular agent path.
//
// When the message is a connect request but its payload cannot be parsed, the returned
// request UID is empty, because the result cannot be routed back without it.
func chainedConnectRequestUID(m *protobufs.AgentToServer) (requestUID string, isConnect bool) {
	cm := m.GetCustomMessage()
	if len(m.GetInstanceUid()) != 0 || cm == nil {
		return "", false
	}
	if cm.GetCapability() != OpampGatewayCapability || cm.GetType() != OpampGatewayConnectType {
		return "", false
	}

	var connect OpampGatewayConnect
	if err := jsoniter.Unmarshal(cm.GetData(), &connect); err != nil {
		return "", true
	}
	return connect.RequestUID, true
}

// connectResultRequestUID returns the request UID of an OpampGatewayConnectResult custom
// message. It returns false if the message is not a connect result or has no request UID.
func connectResultRequestUID(cm *protobufs.CustomMessage) (string, bool) {
	if cm == nil {
		return "", false
	}
	if cm.GetCapability() != OpampGatewayCapability || cm.GetType() != OpampGatewayConnectResultType {
		return "", false
	}

	var result OpampGatewayConnectResult
	if err := jsoniter.Unmarshal(cm.GetData(), &result); err != nil || result.RequestUID == "" {
		return "", false
	}
	return result.RequestUID, true
}
