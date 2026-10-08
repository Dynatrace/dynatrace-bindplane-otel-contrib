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

// Package gateway implements the OpAMP gateway that proxies messages between
// downstream agents and an upstream OpAMP server.
package gateway

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/extension/opampgateway/internal/metadata"
	"github.com/gorilla/websocket"
	"github.com/open-telemetry/opamp-go/protobufs"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
)

var (
	attrUpstream   = attribute.String("direction", "upstream")
	attrDownstream = attribute.String("direction", "downstream")

	directionUpstream   = metric.WithAttributeSet(attribute.NewSet(attrUpstream))
	directionDownstream = metric.WithAttributeSet(attribute.NewSet(attrDownstream))
)

// Settings contains the configuration needed by the gateway implementation.
type Settings struct {
	UpstreamOpAMPAddress string
	Headers              http.Header
	TLSConfig            *tls.Config
	UpstreamConnections  int
	OpAMPServer          confighttp.ServerConfig
	AuthTimeout          time.Duration
	BuildInfo            component.BuildInfo
	// AgentID identifies the collector hosting the gateway. When non-empty it
	// prefixes the X-Opamp-Gateway-Connection-Id header sent upstream.
	AgentID string
}

// Gateway is the core OpAMP gateway implementation that manages upstream and downstream
// connections, forwarding messages between agents and the upstream OpAMP server.
type Gateway struct {
	logger *zap.Logger

	server *server
	client *client

	telemetry *metadata.TelemetryBuilder
}

// New creates a new Gateway with the given settings.
func New(logger *zap.Logger, settings Settings, t *metadata.TelemetryBuilder) *Gateway {
	logger = logger.Named("opamp-gateway")
	g := &Gateway{
		logger:    logger,
		telemetry: t,
	}
	g.client = newClient(settings, g.telemetry, ConnectionCallbacks[*upstreamConnection]{
		OnMessage: func(ctx context.Context, conn *upstreamConnection, messageType int, msg *message) error {
			// The upstream connection is shared by many agents. Per-message
			// errors are logged and swallowed so that a single bad message
			// cannot tear down the connection and disconnect every agent.
			if err := g.HandleUpstreamMessage(ctx, conn, messageType, msg); err != nil {
				g.logger.Warn("upstream message handling failed",
					zap.Error(err),
					zap.String("upstream_connection_id", conn.id),
					zap.Int("message_number", msg.number),
				)
			}
			return nil
		},
		OnError: g.HandleUpstreamError,
		OnClose: g.HandleUpstreamClose,
	}, logger)
	g.server = newServer(settings.OpAMPServer, settings.AuthTimeout, g.telemetry, g.client, ConnectionCallbacks[*downstreamConnection]{
		OnMessage: g.HandleDownstreamMessage,
		OnError:   g.HandleDownstreamError,
		OnClose:   g.HandleDownstreamClose,
	}, logger)
	return g
}

// Start starts the gateway client and server.
func (g *Gateway) Start(ctx context.Context, host component.Host, telemetrySettings component.TelemetrySettings) error {
	g.client.Start(ctx)
	return g.server.Start(ctx, host, telemetrySettings)
}

// Shutdown stops the gateway client and server.
func (g *Gateway) Shutdown(ctx context.Context) error {
	g.client.Stop(ctx)
	return g.server.Stop(ctx)
}

// --------------------------------------------------------------------------------------
// Downstream callbacks

// HandleDownstreamMessage handles message sent from a downstream connection to the server.
func (g *Gateway) HandleDownstreamMessage(_ context.Context, connection *downstreamConnection, messageType int, msg *message) error {
	g.logger.Info("HandleDownstreamMessage", zap.String("downstream_connection_id", connection.id), zap.Int("message_number", msg.number), zap.Int("message_type", messageType))
	if messageType != websocket.BinaryMessage {
		err := fmt.Errorf("unexpected message type: %v, must be binary message", messageType)
		g.logger.Error("Cannot process a message from WebSocket", zap.Error(err), zap.Int("message_number", msg.number), zap.Int("message_type", messageType), zap.String("message_bytes", string(msg.data)))
		return err
	}

	message := protobufs.AgentToServer{}
	err := decodeWSMessage(msg.data, &message)
	if err != nil {
		return fmt.Errorf("cannot decode message from WebSocket: %w", err)
	}

	// A downstream gateway sends an OpampGatewayConnect message for each agent that connects
	// to it. The gateway creates that message itself, so it has no instance_uid and must not
	// go through the agent ID parsing below. Relay it to the upstream server instead.
	if requestUID, ok := chainedConnectRequestUID(&message); ok {
		return g.relayChainedConnect(connection, msg, requestUID)
	}

	agentID, err := parseAgentID(message.GetInstanceUid())
	if err != nil {
		return fmt.Errorf("cannot parse agent ID: %w", err)
	}

	// register the downstream connection for the agent ID so we can forward messages to the
	// agent from the server
	g.server.setAgentConnection(agentID, connection)

	// find the upstream connection
	upstreamConnection := connection.upstreamConnection

	// send the message to the upstream connection
	logMsg := fmt.Sprintf("%s => %s", connection.id, upstreamConnection.id)
	logUpstreamMessage(g.logger, logMsg, agentID, msg.number, len(msg.data), &message)
	if err := upstreamConnection.send(msg); err != nil {
		return fmt.Errorf("send upstream: %w", err)
	}
	g.telemetry.OpampgatewayMessages.Add(context.Background(), 1, directionUpstream)
	g.telemetry.OpampgatewayMessagesBytes.Add(context.Background(), int64(len(msg.data)), directionUpstream)
	return nil
}

// HandleDownstreamError handles an error from a downstream connection.
func (g *Gateway) HandleDownstreamError(_ context.Context, _ *downstreamConnection, err error) {
	g.logger.Error("HandleDownstreamError", zap.Error(err))
}

// HandleDownstreamClose handles the closing of a downstream connection.
func (g *Gateway) HandleDownstreamClose(_ context.Context, connection *downstreamConnection) error {
	g.logger.Info("HandleDownstreamClose", zap.String("downstream_connection_id", connection.id))
	g.client.unassignUpstreamConnection(connection.id)
	g.server.removeDownstreamConnection(connection)
	return nil
}

// --------------------------------------------------------------------------------------
// Upstream callbacks

// HandleUpstreamMessage handles a message sent from the upstream connection to
// a downstream connection. Errors are non-fatal — the caller is responsible
// for logging and discarding them to protect the shared upstream connection.
func (g *Gateway) HandleUpstreamMessage(_ context.Context, connection *upstreamConnection, messageType int, message *message) error {
	g.logger.Debug("HandleUpstreamMessage", zap.String("upstream_connection_id", connection.id), zap.Int("message_number", message.number), zap.Int("message_type", messageType), zap.String("message_bytes", string(message.data)))
	if messageType != websocket.BinaryMessage {
		return fmt.Errorf("unexpected message type: %v, must be binary message", messageType)
	}

	m := protobufs.ServerToAgent{}
	if err := decodeWSMessage(message.data, &m); err != nil {
		return fmt.Errorf("failed to decode ws message: %w", err)
	}

	// cache CustomCapabilities from the upstream server. The opamp-go server
	// only sends these once per WebSocket connection (on the first response),
	// which typically arrives as part of the auth response. We cache them so
	// they can be injected into messages forwarded to downstream agents.
	if caps := m.GetCustomCapabilities(); caps != nil {
		g.logger.Info("caching CustomCapabilities from upstream", zap.Strings("capabilities", caps.GetCapabilities()))
		connection.setCustomCapabilities(caps)
	}

	// Check if this is an authentication response
	if g.server.handleAuthResponse(m.GetCustomMessage()) {
		g.logger.Debug("handled auth response", zap.Int("message_number", message.number))
		return nil
	}

	// The result of a connect request that was relayed on behalf of a downstream gateway is
	// not addressed to an agent. Route it to the gateway that sent the request by request UID.
	if handled, err := g.relayChainedConnectResult(connection, message, &m); handled {
		return err
	}

	agentID, err := parseAgentID(m.GetInstanceUid())
	if err != nil {
		return fmt.Errorf("invalid instance_uid (length=%d, fields=%v): %w",
			len(m.GetInstanceUid()), serverToAgentFields(&m), err)
	}

	// find the downstream connection from the server
	conn, ok := g.server.getAgentConnection(agentID)
	if !ok {
		// downstream connection no longer exists. just ignore the message for now.
		return nil
	}

	// inject cached CustomCapabilities into the first message forwarded to each downstream
	// agent.
	message = g.injectCustomCapabilities(connection, conn, &m, message)

	// forward the message to the downstream connection
	msg := fmt.Sprintf("%s <= %s", conn.id, connection.id)
	logDownstreamMessage(g.logger, msg, agentID, message.number, len(message.data), &m)
	if err := conn.send(message); err != nil {
		return fmt.Errorf("send to downstream agent %s (%s): %w", agentID, conn.id, err)
	}
	g.telemetry.OpampgatewayMessages.Add(context.Background(), 1, directionDownstream)
	g.telemetry.OpampgatewayMessagesBytes.Add(context.Background(), int64(len(message.data)), directionDownstream)
	return nil
}

// relayChainedConnect relays an OpampGatewayConnect request from a downstream gateway to the
// upstream server. The request is forwarded unchanged on the upstream connection assigned to
// the downstream gateway, and the request UID is recorded so the result can be routed back.
func (g *Gateway) relayChainedConnect(connection *downstreamConnection, msg *message, requestUID string) error {
	if requestUID == "" {
		return errors.New("cannot relay gateway connect request without a request_uid")
	}

	upstreamConnection := connection.upstreamConnection

	// record the request before sending it so the result can never arrive before the request
	// is known
	g.server.chainedConnects.add(requestUID, connection)

	logMsg := fmt.Sprintf("%s => %s", connection.id, upstreamConnection.id)
	g.logger.Info(logMsg,
		zap.String("request_uid", requestUID),
		zap.String("type", OpampGatewayConnectType),
		zap.Int("message.number", msg.number),
		zap.Int("message.bytes", len(msg.data)),
	)
	if err := upstreamConnection.send(msg); err != nil {
		g.server.chainedConnects.take(requestUID)
		return fmt.Errorf("send upstream: %w", err)
	}
	g.telemetry.OpampgatewayMessages.Add(context.Background(), 1, directionUpstream)
	g.telemetry.OpampgatewayMessagesBytes.Add(context.Background(), int64(len(msg.data)), directionUpstream)
	return nil
}

// relayChainedConnectResult forwards an OpampGatewayConnectResult to the downstream gateway
// that sent the matching request. It returns true if the message was the result of a relayed
// request, in which case the returned error is the outcome of forwarding it.
func (g *Gateway) relayChainedConnectResult(upstream *upstreamConnection, msg *message, m *protobufs.ServerToAgent) (bool, error) {
	requestUID, ok := connectResultRequestUID(m.GetCustomMessage())
	if !ok {
		return false, nil
	}
	conn, ok := g.server.chainedConnects.take(requestUID)
	if !ok {
		return false, nil
	}

	// The result is usually the first message a downstream gateway receives on its connection,
	// so it needs the upstream's CustomCapabilities like any other first message.
	msg = g.injectCustomCapabilities(upstream, conn, m, msg)

	logMsg := fmt.Sprintf("%s <= %s", conn.id, upstream.id)
	g.logger.Info(logMsg,
		zap.String("request_uid", requestUID),
		zap.String("type", OpampGatewayConnectResultType),
		zap.Int("message.number", msg.number),
		zap.Int("message.bytes", len(msg.data)),
	)
	if err := conn.send(msg); err != nil {
		return true, fmt.Errorf("send connect result to downstream gateway %s: %w", conn.id, err)
	}
	g.telemetry.OpampgatewayMessages.Add(context.Background(), 1, directionDownstream)
	g.telemetry.OpampgatewayMessagesBytes.Add(context.Background(), int64(len(msg.data)), directionDownstream)
	return true, nil
}

// injectCustomCapabilities makes sure the first message forwarded to a downstream connection
// carries the CustomCapabilities of the upstream server. The opamp-go server only sends them
// once per WebSocket connection, so they are cached on the upstream connection and shared
// with each downstream connection here. It returns the message to forward, which is a new
// message when the capabilities were added.
func (g *Gateway) injectCustomCapabilities(upstream *upstreamConnection, downstream *downstreamConnection, m *protobufs.ServerToAgent, msg *message) *message {
	if m.CustomCapabilities != nil {
		downstream.sentCustomCapabilities.Store(true)
		return msg
	}
	if downstream.sentCustomCapabilities.Load() {
		return msg
	}
	caps := upstream.getCustomCapabilities()
	if caps == nil {
		return msg
	}

	m.CustomCapabilities = caps
	updated, err := encodeWSMessage(m)
	if err != nil {
		g.logger.Error("failed to update message with CustomCapabilities", zap.Error(err))
		return msg
	}
	downstream.sentCustomCapabilities.Store(true)
	return newMessage(msg.number, updated)
}

// HandleUpstreamError handles an error from an upstream connection.
func (g *Gateway) HandleUpstreamError(_ context.Context, _ *upstreamConnection, err error) {
	g.logger.Error("HandleUpstreamError", zap.Error(err))
}

// HandleUpstreamClose handles the closing of an upstream connection.
func (g *Gateway) HandleUpstreamClose(_ context.Context, connection *upstreamConnection) error {
	g.logger.Info("HandleUpstreamClose", zap.String("upstream_connection_id", connection.id))
	// close all downstream connections associated with this upstream connection
	downstreamConnectionIDs := g.client.connectionAssignments.removeDownstreamConnectionIDs(connection.id)
	g.server.closeDownstreamConnections(downstreamConnectionIDs)
	return nil
}

// --------------------------------------------------------------------------------------
// logging helpers

func logDownstreamMessage(logger *zap.Logger, msg string, agentID string, messageNumber int, messageBytes int, message *protobufs.ServerToAgent) {
	logger.Info(msg,
		zap.String("agent.id", agentID),
		zap.Int("message.number", messageNumber),
		zap.Int("message.bytes", messageBytes),
		zap.Strings("components", downstreamMessageComponents(message)),
		zap.Uint64("flags", message.Flags),
	)
}

func logUpstreamMessage(logger *zap.Logger, msg string, agentID string, messageNumber int, messageBytes int, message *protobufs.AgentToServer) {
	logger.Info(msg,
		zap.String("agent.id", agentID),
		zap.Int("message.number", messageNumber),
		zap.Int("message.bytes", messageBytes),
		zap.Strings("components", upstreamMessageComponents(message)),
		zap.Uint64("sequenceNum", message.SequenceNum),
		zap.Uint64("flags", message.Flags),
	)
}

// serverToAgentFields returns the names of all top-level fields that are set
// on the message. This is used for diagnostic logging so operators can identify
// what the server sent without leaking message contents.
func serverToAgentFields(m *protobufs.ServerToAgent) []string {
	var fields []string
	if len(m.GetInstanceUid()) > 0 {
		fields = append(fields, "instance_uid")
	}
	fields = append(fields, includeComponent(nil, m.GetErrorResponse(), "error_response")...)
	fields = append(fields, includeComponent(nil, m.GetRemoteConfig(), "remote_config")...)
	fields = append(fields, includeComponent(nil, m.GetConnectionSettings(), "connection_settings")...)
	fields = append(fields, includeComponent(nil, m.GetPackagesAvailable(), "packages_available")...)
	fields = append(fields, includeComponent(nil, m.GetAgentIdentification(), "agent_identification")...)
	fields = append(fields, includeComponent(nil, m.GetCommand(), "command")...)
	fields = append(fields, includeComponent(nil, m.GetCustomCapabilities(), "custom_capabilities")...)
	fields = append(fields, includeComponent(nil, m.GetCustomMessage(), "custom_message")...)
	if m.GetFlags() != 0 {
		fields = append(fields, "flags")
	}
	if m.GetCapabilities() != 0 {
		fields = append(fields, "capabilities")
	}
	return fields
}

func downstreamMessageComponents(serverToAgent *protobufs.ServerToAgent) []string {
	var components []string
	components = includeComponent(components, serverToAgent.ErrorResponse, "ErrorResponse")
	components = includeComponent(components, serverToAgent.RemoteConfig, "RemoteConfig")
	components = includeComponent(components, serverToAgent.ConnectionSettings, "ConnectionSettings")
	components = includeComponent(components, serverToAgent.PackagesAvailable, "PackagesAvailable")
	components = includeComponent(components, serverToAgent.AgentIdentification, "AgentIdentification")
	components = includeComponent(components, serverToAgent.Command, "Command")
	components = includeComponent(components, serverToAgent.CustomCapabilities, "CustomCapabilities")
	components = includeCustomMessage(components, serverToAgent.CustomMessage)
	return components
}

// upstreamMessageComponents returns the names of the components in the message
func upstreamMessageComponents(agentToServer *protobufs.AgentToServer) []string {
	var components []string
	components = includeComponent(components, agentToServer.AgentDescription, "AgentDescription")
	components = includeComponent(components, agentToServer.EffectiveConfig, "EffectiveConfig")
	components = includeComponent(components, agentToServer.RemoteConfigStatus, "RemoteConfigStatus")
	components = includeComponent(components, agentToServer.PackageStatuses, "PackageStatuses")
	components = includeComponent(components, agentToServer.AvailableComponents, "AvailableComponents")
	components = includeComponent(components, agentToServer.Health, "Health")
	components = includeCustomMessage(components, agentToServer.CustomMessage)
	return components
}

func includeComponent[T any](components []string, msg *T, name string) []string {
	if msg != nil {
		components = append(components, name)
	}
	return components
}

func includeCustomMessage(components []string, msg *protobufs.CustomMessage) []string {
	if msg != nil {
		components = append(components, msg.Type)
	}
	return components
}
