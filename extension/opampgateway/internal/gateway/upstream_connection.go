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
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/extension/opampgateway/internal/metadata"
	"github.com/gorilla/websocket"
	"github.com/open-telemetry/opamp-go/protobufs"
	"go.uber.org/zap"
)

type upstreamConnection struct {
	id       string
	settings upstreamConnectionSettings
	dialer   websocket.Dialer

	writeChan       chan *message
	readerErrorChan chan error

	writerDone chan struct{}

	count atomic.Int32

	telemetry *metadata.TelemetryBuilder
	logger    *zap.Logger

	connected atomic.Bool

	// customCapabilities caches the CustomCapabilities received from the
	// upstream server. The opamp-go server only sends CustomCapabilities
	// once per WebSocket connection (on the first response), so the gateway
	// must cache and inject them into messages forwarded to downstream agents.
	customCapabilitiesMtx sync.RWMutex
	customCapabilities    *protobufs.CustomCapabilities
}

type upstreamConnectionSettings struct {
	endpoint  string
	headers   http.Header
	userAgent string
}

func newUpstreamConnection(dialer websocket.Dialer, telemetry *metadata.TelemetryBuilder, settings upstreamConnectionSettings, id string, logger *zap.Logger) *upstreamConnection {
	return &upstreamConnection{
		dialer:    dialer,
		settings:  settings,
		id:        id,
		telemetry: telemetry,
		logger:    logger.Named("upstream-connection").With(zap.String(keyUpstreamConnectionID, id)),
		writeChan: make(chan *message),

		// the error channel is buffered to prevent blocking the reader goroutine if it
		// encounters an error. it will return immediately after reporting the error and the
		// main connection goroutine will handle the error.
		readerErrorChan: make(chan error, 1),
		writerDone:      make(chan struct{}),
	}
}

func (c *upstreamConnection) downstreamCount() int {
	return int(c.count.Load())
}

func (c *upstreamConnection) incrementDownstreamCount() {
	c.count.Add(1)
}

func (c *upstreamConnection) decrementDownstreamCount() {
	c.count.Add(-1)
}

func (c *upstreamConnection) isConnected() bool {
	return c.connected.Load()
}
func (c *upstreamConnection) setConnected(connected bool) {
	c.connected.Store(connected)
}

func (c *upstreamConnection) setCustomCapabilities(caps *protobufs.CustomCapabilities) {
	c.customCapabilitiesMtx.Lock()
	defer c.customCapabilitiesMtx.Unlock()
	c.customCapabilities = caps
}

func (c *upstreamConnection) getCustomCapabilities() *protobufs.CustomCapabilities {
	c.customCapabilitiesMtx.RLock()
	defer c.customCapabilitiesMtx.RUnlock()
	return c.customCapabilities
}

// start will start the reader and writer goroutines and wait for the context to be done
// or an error to be sent on the error channel. if an error is sent on the error channel,
// the connection will be stopped and the context will be cancelled.
func (c *upstreamConnection) start(ctx context.Context, callbacks ConnectionCallbacks[*upstreamConnection]) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// block while writing messages to the connection. a connection close will unblock the writer.
	err := c.startWriter(ctx, callbacks)
	if err != nil {
		callbacks.OnError(ctx, c, err)
	}

	// if the writer returns, cancel the context to stop the reader
	cancel()

	// wait for the reader and writer to finish
	<-c.writerDone

	// check for errors from the reader
	select {
	case err := <-c.readerErrorChan:
		callbacks.OnError(ctx, c, err)
	default:
	}
}

// send queues the message for writing and blocks until it has been written to
// the upstream WebSocket or the connection is permanently closed. It returns
// the write error (if any) so callers get confirmation of delivery.
func (c *upstreamConnection) send(message *message) error {
	// queue the message. Block until the writer dequeues it or the
	// connection shuts down for good (writerDone is closed).
	select {
	case c.writeChan <- message:
	case <-c.writerDone:
		return errUpstreamConnectionClosed
	}

	// wait for the writer to finish the WebSocket write.
	select {
	case err := <-message.done:
		return err
	case <-c.writerDone:
		return errUpstreamConnectionClosed
	}
}

// --------------------------------------------------------------------------------------
// reader goroutine

// startReader reads messages from the connection until it is closed or the context is done. It
// returns the error that stopped the reader, if any.
func (c *upstreamConnection) startReader(ctx context.Context, conn *websocket.Conn, callbacks ConnectionCallbacks[*upstreamConnection]) error {
	reader := newMessageReader(conn, c.id, readerCallbacks{
		OnMessage: func(ctx context.Context, messageType int, message *message) error {
			return callbacks.OnMessage(ctx, c, messageType, message)
		},
		OnError: func(ctx context.Context, err error) {
			callbacks.OnError(ctx, c, err)
		},
	}, c.logger)

	return reader.loop(ctx, 0)
}

// --------------------------------------------------------------------------------------
// writer goroutine

func (c *upstreamConnection) startWriter(ctx context.Context, callbacks ConnectionCallbacks[*upstreamConnection]) error {
	defer close(c.writerDone)

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// nextMessage is the message that was not written to the connection because the
	// connection was closed. it will be written to the connection when it is reconnected.
	// it is used to avoid losing messages when the connection is closed.
	var nextMessage *message

	for {
		// ensure connected will do infinite retries until the context is done or an error is
		// returned. this could mean that it takes a while to open the connections to the
		// upstream server.
		conn, err := c.ensureConnected(ctx, c.id)
		if err != nil {
			if ctx.Err() != nil {
				// context is done, so we return
				return nil
			}
			// shutdown, not really an error
			c.logger.Debug("stopped connecting to upstream OpAMP server", zap.Error(err))
			return nil
		}

		readerDone := make(chan struct{})

		writerCtx, writerCancel := context.WithCancel(ctx)
		readerCtx, readerCancel := context.WithCancel(ctx)

		// start the reader in a separate goroutine and cancel the context if it returns, likely
		// due to an error or the connection being closed. readerErr is safe to read once
		// readerDone is closed.
		var readerErr error
		go func() {
			defer close(readerDone)
			defer writerCancel()
			readerErr = c.startReader(readerCtx, conn, callbacks)
			c.logger.Debug("reader finished")
		}()

		var writerErr error
		nextMessage, writerErr = c.writerLoop(writerCtx, conn, nextMessage)
		c.logger.Debug("writer loop done", zap.Error(writerErr))

		// cancel the reader context to stop the reader loop and wait for it to finish
		readerCancel()
		c.logger.Debug("waiting for reader to finish")
		<-readerDone

		// Report the loss of the connection once, with whatever stopped the reader or the
		// writer. The retries that follow are logged by ensureConnected. During shutdown the
		// close is expected and only logged at Debug.
		fields := []zap.Field{
			zap.Int(keyDownstreamConnectionCt, c.downstreamCount()),
			zap.NamedError(keyReaderError, readerErr),
			zap.NamedError(keyWriterError, writerErr),
		}
		if ctx.Err() != nil {
			c.logger.Debug("upstream connection closed", fields...)
		} else {
			c.logger.Warn("upstream connection lost", fields...)
		}

		// Call the on close handler
		err = callbacks.OnClose(ctx, c)
		if err != nil {
			c.logger.Error("upstream connection OnClose handler", zap.Error(err))
		}
	}
}

// writerLoop will loop until the context is done or the write channel is closed. it takes
// the next message to write and returns the next message to write and an error if one
// occurs.
func (c *upstreamConnection) writerLoop(ctx context.Context, conn *websocket.Conn, nextMessage *message) (*message, error) {
	c.setConnected(true)
	defer c.setConnected(false)

	// attempt to write the next message if one is pending
	if nextMessage != nil {
		err := writeWSMessage(conn, nextMessage.data)
		nextMessage.complete(err)
		if err != nil {
			return nextMessage, fmt.Errorf("write message: %w", err)
		}
		// clear the next message after successfully writing it
		nextMessage = nil
	}

	for {
		select {
		case <-ctx.Done():
			c.logger.Debug("writer context done")
			// Send a WebSocket close frame to notify the peer before closing the TCP connection.
			closeMsg := websocket.FormatCloseMessage(websocket.CloseNormalClosure, "")
			_ = conn.WriteControl(websocket.CloseMessage, closeMsg, time.Now().Add(5*time.Second))
			err := conn.Close()
			if err != nil {
				// the connection is usually already closed by the peer at this point, so this
				// is not reported as an error
				c.logger.Debug("error closing connection", zap.Error(err))
			}
			return nextMessage, nil

		case message, ok := <-c.writeChan:
			if !ok {
				// the write channel is closed, so we return
				c.logger.Debug("write channel closed")
				return message, nil
			}
			err := writeWSMessage(conn, message.data)
			message.complete(err)
			if err != nil {
				return message, fmt.Errorf("write message: %w", err)
			}
			c.telemetry.OpampgatewayMessagesLatency.Record(ctx, message.elapsedTime().Milliseconds(), directionUpstream)
		}
	}
}

// --------------------------------------------------------------------------------------

// Continuously try until connected. Will return nil when successfully
// connected. Will return error if it is cancelled via context.
//
// Connecting is logged once at Info. The first failed attempt is logged at Warn so an
// unreachable upstream is visible at the default log level, and every retry after that is
// logged at Debug with the backoff interval so an extended outage does not flood the log.
func (c *upstreamConnection) ensureConnected(ctx context.Context, id string) (*websocket.Conn, error) {
	infiniteBackoff := backoff.NewExponentialBackOff()

	// Make ticker run forever.
	infiniteBackoff.MaxElapsedTime = 0

	c.logger.Info("connecting to upstream OpAMP server", zap.String(keyURLFull, c.settings.endpoint))

	interval := time.Duration(0)
	attempt := 0

	for {
		timer := time.NewTimer(interval)
		interval = infiniteBackoff.NextBackOff()

		select {
		case <-timer.C:
			{
				attempt++
				conn, err := c.tryConnectOnce(ctx, id)
				if err != nil {
					if errors.Is(err, context.Canceled) {
						c.logger.Debug("Client is stopped, will not try anymore.")
						return nil, err
					}
					fields := []zap.Field{
						zap.Error(err),
						zap.Int(keyConnectAttempt, attempt),
						zap.Duration(keyConnectRetryIn, interval),
					}
					if attempt == 1 {
						c.logger.Warn("upstream connection failed, retrying", fields...)
					} else {
						c.logger.Debug("upstream connection failed, retrying", fields...)
					}
					// Retry again a bit later.
					continue
				}
				// Connected successfully.
				return conn, nil
			}

		case <-ctx.Done():
			c.logger.Debug("Client is stopped, will not try anymore.")
			timer.Stop()
			return nil, ctx.Err()
		}
	}
}

func (c *upstreamConnection) tryConnectOnce(ctx context.Context, id string) (*websocket.Conn, error) {
	var resp *http.Response

	conn, resp, err := c.dialer.DialContext(ctx, c.settings.endpoint, c.header(id))
	if err != nil {
		if resp != nil {
			return nil, fmt.Errorf("server responded with status: %s", resp.Status)
		}
		return nil, err
	}
	c.logger.Info("connected to upstream OpAMP server", addressFields(prefixServer, conn.RemoteAddr().String())...)

	// Successfully connected.
	return conn, nil
}

func (c *upstreamConnection) header(id string) http.Header {
	h := c.settings.headers.Clone()
	if h == nil {
		h = make(http.Header)
	}
	h.Set("X-Opamp-Gateway-Connection-Id", id)
	// identify the gateway to the upstream server, leaving a configured
	// User-Agent in place so it can be overridden.
	if h.Get("User-Agent") == "" {
		h.Set("User-Agent", c.settings.userAgent)
	}
	return h
}
