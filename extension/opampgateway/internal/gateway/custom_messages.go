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
	"net/http"
	"strings"
)

// OpampGatewayConnect represents an authentication request sent by a gateway on behalf of an
// agent. The gateway sends this message to validate an agent's credentials before allowing
// the agent to connect through the gateway.
type OpampGatewayConnect struct {
	// RequestUID is a unique identifier for this connection request, used to correlate
	// the request with its response.
	RequestUID string `json:"request_uid,omitempty"`

	// RemoteAddress is the remote address of the connection (from the socket connection) of
	// the agent that is connecting to the gateway.
	RemoteAddress string `json:"remote_address"`

	// Headers contains HTTP-style headers the server can use to validate the agent
	// connection.
	Headers http.Header `json:"headers"`
}

// OpampGatewayConnectResult represents the server's response to an OpampGatewayConnect request.
// It indicates whether the agent's credentials were accepted and includes any response headers.
type OpampGatewayConnectResult struct {
	// RequestUID is the unique identifier from the corresponding OpampGatewayConnect request,
	// used to correlate the response with its request.
	RequestUID string `json:"request_uid,omitempty"`

	Accept          bool              `json:"accept"`
	HTTPStatusCode  int               `json:"http_status_code"`
	ResponseHeaders map[string]string `json:"response_headers"`
}

const (
	// OpampGatewayCapability is the capability identifier for opamp-gateway custom messages.
	OpampGatewayCapability = "com.bindplane.opamp-gateway"

	// OpampGatewayConnectType is sent when an agent connects to the gateway. It includes the
	// headers and client certificate that can be validated by the server to allow the
	// connection.
	OpampGatewayConnectType = "connect"

	// OpampGatewayConnectResultType is sent as a response to the connect message. It will
	// either allow or deny the agent connection.
	OpampGatewayConnectResultType = "connectResult"

	// OpampGatewayDisconnectAgentType is sent by the server to make the gateway close the
	// connection of one downstream agent, for example an agent with a duplicate agent ID.
	// The gateway advertises support in the X-Opamp-Gateway-Capabilities header.
	OpampGatewayDisconnectAgentType = "disconnectAgent"

	// gatewayCapabilitiesHeader lists the optional gateway features the server can use.
	gatewayCapabilitiesHeader = "X-Opamp-Gateway-Capabilities"
)

// hasGatewayCapability reports whether the comma-separated X-Opamp-Gateway-Capabilities
// header lists the capability.
func hasGatewayCapability(header http.Header, capability string) bool {
	for _, value := range header.Values(gatewayCapabilitiesHeader) {
		for _, c := range strings.Split(value, ",") {
			if strings.TrimSpace(c) == capability {
				return true
			}
		}
	}
	return false
}

// OpampGatewayDisconnectAgent asks the gateway to close the connection of one downstream
// agent.
type OpampGatewayDisconnectAgent struct {
	// InstanceUID is the instance UID of the agent, as in AgentToServer.instance_uid.
	InstanceUID []byte `json:"instance_uid"`

	// RetryAfterSeconds is how long the server rejects reconnects of the agent. The server
	// enforces it; the gateway only passes the value on to a chained gateway.
	RetryAfterSeconds int `json:"retry_after_seconds"`
}
