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
	"sync"
	"time"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/extension/opampgateway/internal/metadata"
	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/extension/opampgateway/internal/version"
	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

// noUpstreamLogInterval is how often the lack of an available upstream connection is logged at
// Warn. Every agent that connects while the upstream is unreachable hits this condition, so it
// is rate limited to keep an outage from flooding the log.
const noUpstreamLogInterval = time.Minute

// UpstreamConnectionAssigner assigns and unassigns upstream connections for downstream connections.
type UpstreamConnectionAssigner interface {
	AssignUpstreamConnection(downstreamConnectionID string) (*upstreamConnection, error)
	UnassignUpstreamConnection(downstreamConnectionID string)
}

type client struct {
	logger *zap.Logger
	dialer websocket.Dialer

	pool *connectionPool
	// upstreamConnections is a set of connections to the upstream OpAMP server.
	upstreamConnections *connections[*upstreamConnection]

	connectionAssignments *connectionAssignments

	callbacks ConnectionCallbacks[*upstreamConnection]

	headers          http.Header
	userAgent        string
	upstreamEndpoint string
	connectionCount  int

	clientConnectionsWg     *sync.WaitGroup
	clientConnectionsCancel context.CancelFunc

	noUpstreamLog *logLimiter

	telemetry *metadata.TelemetryBuilder
}

func newClient(settings Settings, telemetry *metadata.TelemetryBuilder, callbacks ConnectionCallbacks[*upstreamConnection], logger *zap.Logger) *client {
	logger = logger.Named("client")
	pool := newConnectionPool(settings.UpstreamConnections, logger)
	connections := newConnections[*upstreamConnection]()
	connectionAssignments := newConnectionAssignments(connections, pool)
	dialer := *websocket.DefaultDialer
	dialer.TLSClientConfig = settings.TLSConfig
	return &client{
		logger:                logger,
		dialer:                dialer,
		pool:                  pool,
		upstreamConnections:   connections,
		connectionAssignments: connectionAssignments,
		callbacks:             callbacks,
		headers:               settings.Headers,
		userAgent:             version.UserAgent(settings.BuildInfo),
		upstreamEndpoint:      settings.UpstreamOpAMPAddress,
		connectionCount:       settings.UpstreamConnections,
		clientConnectionsWg:   &sync.WaitGroup{},
		noUpstreamLog:         newLogLimiter(noUpstreamLogInterval),
		telemetry:             telemetry,
	}
}

// Start begins connecting to the upstream OpAMP server. It resets internal
// state so the client can be restarted after a previous Stop (e.g. during
// collector hot-reload).
func (c *client) Start(ctx context.Context) {
	// Reset state so the client can be restarted after a previous Stop.
	c.clientConnectionsWg = &sync.WaitGroup{}
	c.pool = newConnectionPool(c.connectionCount, c.logger)
	c.upstreamConnections = newConnections[*upstreamConnection]()
	c.connectionAssignments = newConnectionAssignments(c.upstreamConnections, c.pool)

	ctx, c.clientConnectionsCancel = context.WithCancel(ctx)
	c.clientConnectionsWg.Add(c.connectionCount)
	go c.startClientConnections(ctx)
}

func (c *client) startClientConnections(ctx context.Context) {
	for i := 0; i < c.connectionCount; i++ {
		// generate a unique id for the connection
		id := fmt.Sprintf("upstream-%d", i)

		clientConnection := newUpstreamConnection(c.dialer, c.telemetry, upstreamConnectionSettings{
			endpoint:  c.upstreamEndpoint,
			headers:   c.headers,
			userAgent: c.userAgent,
		}, id, c.logger)

		c.pool.add(clientConnection)
		c.upstreamConnections.set(id, clientConnection)

		go func() {
			defer c.clientConnectionsWg.Done()

			c.telemetry.OpampgatewayConnections.Add(context.Background(), 1, directionUpstream)
			defer c.telemetry.OpampgatewayConnections.Add(context.Background(), -1, directionUpstream)

			// cleanup function to remove the connection from the pool and connections map
			defer func() {
				c.upstreamConnections.remove(clientConnection.id)
				c.pool.remove(clientConnection)
				c.logger.Info("upstream connection shutdown", zap.String(keyUpstreamConnectionID, clientConnection.id), zap.Int(keyDownstreamConnectionCt, clientConnection.downstreamCount()))
			}()

			// start the connection
			clientConnection.start(ctx, ConnectionCallbacks[*upstreamConnection]{
				OnMessage: c.callbacks.OnMessage,
				OnError:   c.callbacks.OnError,
				OnClose: func(ctx context.Context, connection *upstreamConnection) error {
					c.logger.Debug("upstream connection closed", zap.String(keyUpstreamConnectionID, connection.id), zap.Int(keyDownstreamConnectionCt, clientConnection.downstreamCount()))
					return c.callbacks.OnClose(ctx, connection)
				},
			})
		}()
	}
}

func (c *client) Stop(_ context.Context) {
	if c.clientConnectionsCancel != nil {
		c.clientConnectionsCancel()
	}
	c.clientConnectionsWg.Wait()
	c.logger.Info("client stopped")
}

// --------------------------------------------------------------------------------------
// upstream connection management

func (c *client) assignedUpstreamConnection(downstreamConnectionID string) (*upstreamConnection, error) {
	conn, exists := c.connectionAssignments.assignedUpstreamConnection(downstreamConnectionID)
	if !exists {
		fields := []zap.Field{
			zap.String(keyDownstreamConnectionID, downstreamConnectionID),
			zap.Int(keyUpstreamConnectionCnt, c.pool.size()),
		}
		if ok, suppressed := c.noUpstreamLog.allow(time.Now()); ok {
			c.logger.Warn("no upstream connection available", append(fields, zap.Int(keySuppressedCount, suppressed))...)
		} else {
			c.logger.Debug("no upstream connection available", fields...)
		}
		return nil, fmt.Errorf("no upstream connection available for downstream connection %s: %w", downstreamConnectionID, ErrNoUpstreamConnectionsAvailable)
	}
	c.logger.Debug("assigned upstream connection", zap.String(keyDownstreamConnectionID, downstreamConnectionID), zap.String(keyUpstreamConnectionID, conn.id))
	return conn, nil
}

func (c *client) unassignUpstreamConnection(downstreamConnectionID string) {
	c.connectionAssignments.unassignDownstreamConnection(downstreamConnectionID)
}

// --------------------------------------------------------------------------------------
// UpstreamConnectionAssigner

func (c *client) AssignUpstreamConnection(downstreamConnectionID string) (*upstreamConnection, error) {
	return c.assignedUpstreamConnection(downstreamConnectionID)
}

func (c *client) UnassignUpstreamConnection(downstreamConnectionID string) {
	c.unassignUpstreamConnection(downstreamConnectionID)
}
