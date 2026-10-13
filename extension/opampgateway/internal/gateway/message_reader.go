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

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

type messageReader struct {
	conn      *websocket.Conn
	callbacks readerCallbacks
	id        string
	logger    *zap.Logger
}

type readerCallbacks struct {
	OnMessage func(ctx context.Context, messageType int, message *message) error
	OnError   func(ctx context.Context, err error)
}

func newMessageReader(conn *websocket.Conn, id string, callbacks readerCallbacks, logger *zap.Logger) *messageReader {
	return &messageReader{conn: conn, id: id, callbacks: callbacks, logger: logger.Named("message-reader")}
}

// loop will read messages from the connection and call the OnMessage callback for each
// message. It returns the error that stopped it, or nil when the context is done. Ordinary
// disconnects are expected whenever the peer goes away, so they are logged at Debug and OnError
// is not called. Any other error is reported through OnError before returning.
func (r *messageReader) loop(ctx context.Context, messageNumber int) error {
	// loop until the connection is closed
	for {
		// try to read the message. ReadMessage will block until a message is received or the
		// connection is closed.
		messageType, messageBytes, err := r.conn.ReadMessage()
		if err != nil {
			if ctx.Err() != nil {
				// context is done, so we return cleanly
				r.logger.Debug("reader stopped", zap.Error(ctx.Err()))
				return nil
			}
			if isOrdinaryDisconnect(err) {
				r.logger.Debug("connection closed", zap.Error(err))
				return err
			}
			r.callbacks.OnError(ctx, fmt.Errorf("read message: %w", err))
			return err
		}

		// handle the message using the callback
		message := newMessage(messageNumber, messageBytes)
		if err := r.callbacks.OnMessage(ctx, messageType, message); err != nil {
			r.callbacks.OnError(ctx, fmt.Errorf("handle message: %w", err))
			return err
		}
		messageNumber++
	}
}
