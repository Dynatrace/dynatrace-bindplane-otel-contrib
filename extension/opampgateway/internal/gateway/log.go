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
	"io"
	"net"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"go.uber.org/zap"
)

var (
	// errUpstreamConnectionClosed is returned by send when the upstream connection has shut
	// down before the message could be written.
	errUpstreamConnectionClosed = errors.New("upstream connection closed")

	// errDownstreamConnectionClosed is returned by send when the downstream connection has
	// shut down before the message could be written.
	errDownstreamConnectionClosed = errors.New("downstream connection closed")
)

// isOrdinaryDisconnect reports whether err is the kind of error every connection produces when
// it or its peer goes away: a cancelled context, a WebSocket close frame, a closed socket, an
// EOF or a reset. These happen each time an agent disconnects or an upstream connection is
// cycled, so they are logged at Debug rather than reported as errors.
func isOrdinaryDisconnect(err error) bool {
	if err == nil {
		return false
	}
	var closeErr *websocket.CloseError
	switch {
	case errors.Is(err, context.Canceled),
		errors.Is(err, net.ErrClosed),
		errors.Is(err, io.EOF),
		errors.Is(err, io.ErrUnexpectedEOF),
		errors.Is(err, syscall.ECONNRESET),
		errors.Is(err, syscall.EPIPE),
		errors.Is(err, websocket.ErrCloseSent),
		errors.Is(err, errUpstreamConnectionClosed),
		errors.Is(err, errDownstreamConnectionClosed),
		errors.As(err, &closeErr):
		return true
	}
	return false
}

// logConnectionError logs an error reported by a connection. Ordinary disconnects are logged at
// Debug because they are expected whenever a peer goes away; anything else is logged at Error.
func logConnectionError(logger *zap.Logger, msg string, err error, fields ...zap.Field) {
	fields = append(fields, zap.Error(err))
	if isOrdinaryDisconnect(err) {
		logger.Debug(msg, fields...)
		return
	}
	logger.Error(msg, fields...)
}

// logLimiter allows a repeated condition to be logged at most once per interval. It counts the
// occurrences suppressed in between so the next log line can report them.
type logLimiter struct {
	interval time.Duration

	mtx        sync.Mutex
	last       time.Time
	suppressed int
}

func newLogLimiter(interval time.Duration) *logLimiter {
	return &logLimiter{interval: interval}
}

// allow reports whether the condition should be logged now. When it returns true, suppressed is
// the number of occurrences that were not logged since the previous allowed one.
func (l *logLimiter) allow(now time.Time) (ok bool, suppressed int) {
	l.mtx.Lock()
	defer l.mtx.Unlock()
	if !l.last.IsZero() && now.Sub(l.last) < l.interval {
		l.suppressed++
		return false, 0
	}
	l.last = now
	suppressed, l.suppressed = l.suppressed, 0
	return true, suppressed
}
