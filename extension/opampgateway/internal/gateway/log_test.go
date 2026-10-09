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
	"io"
	"net"
	"net/http"
	"syscall"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/open-telemetry/opamp-go/protobufs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"go.uber.org/zap/zaptest/observer"
)

func TestIsOrdinaryDisconnect(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		err  error
		want bool
	}{
		{name: "nil", err: nil, want: false},
		{name: "context canceled", err: context.Canceled, want: true},
		{name: "wrapped context canceled", err: fmt.Errorf("read message: %w", context.Canceled), want: true},
		{name: "net closed", err: net.ErrClosed, want: true},
		{name: "op error wrapping net closed", err: &net.OpError{Op: "read", Err: net.ErrClosed}, want: true},
		{name: "eof", err: io.EOF, want: true},
		{name: "unexpected eof", err: io.ErrUnexpectedEOF, want: true},
		{name: "connection reset", err: &net.OpError{Op: "read", Err: syscall.ECONNRESET}, want: true},
		{name: "broken pipe", err: &net.OpError{Op: "write", Err: syscall.EPIPE}, want: true},
		{name: "close sent", err: websocket.ErrCloseSent, want: true},
		{name: "normal close frame", err: &websocket.CloseError{Code: websocket.CloseNormalClosure}, want: true},
		{name: "going away close frame", err: &websocket.CloseError{Code: websocket.CloseGoingAway}, want: true},
		{name: "abnormal close frame", err: &websocket.CloseError{Code: websocket.CloseAbnormalClosure}, want: true},
		{name: "upstream closed", err: fmt.Errorf("send upstream: %w", errUpstreamConnectionClosed), want: true},
		{name: "downstream closed", err: fmt.Errorf("send to downstream agent: %w", errDownstreamConnectionClosed), want: true},
		{name: "joined with ordinary", err: errors.Join(errors.New("write data"), net.ErrClosed), want: true},
		{name: "protocol error", err: errors.New("websocket: RSV1 set, no extension negotiated"), want: false},
		{name: "decode error", err: fmt.Errorf("cannot decode message from WebSocket: %w", errors.New("proto: bad wire type")), want: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, isOrdinaryDisconnect(tt.err))
		})
	}
}

func TestLogConnectionErrorLevels(t *testing.T) {
	t.Parallel()

	core, logs := observer.New(zapcore.DebugLevel)
	logger := zap.New(core)

	logConnectionError(logger, "connection error", &websocket.CloseError{Code: websocket.CloseGoingAway}, zap.String("id", "a"))
	logConnectionError(logger, "connection error", errors.New("something broke"), zap.String("id", "b"))

	entries := logs.All()
	require.Len(t, entries, 2)
	assert.Equal(t, zapcore.DebugLevel, entries[0].Level)
	assert.Equal(t, "a", entries[0].ContextMap()["id"])
	assert.Equal(t, zapcore.ErrorLevel, entries[1].Level)
	assert.Equal(t, "b", entries[1].ContextMap()["id"])
}

func TestLogLimiter(t *testing.T) {
	t.Parallel()

	limiter := newLogLimiter(time.Minute)
	start := time.Now()

	ok, suppressed := limiter.allow(start)
	assert.True(t, ok, "first occurrence is always logged")
	assert.Equal(t, 0, suppressed)

	for i := 0; i < 3; i++ {
		ok, _ = limiter.allow(start.Add(time.Duration(i+1) * time.Second))
		assert.False(t, ok, "occurrences within the interval are suppressed")
	}

	ok, suppressed = limiter.allow(start.Add(time.Minute))
	assert.True(t, ok, "logged again once the interval has elapsed")
	assert.Equal(t, 3, suppressed, "reports how many occurrences were suppressed")

	ok, suppressed = limiter.allow(start.Add(2 * time.Minute))
	assert.True(t, ok)
	assert.Equal(t, 0, suppressed, "the suppressed count resets after being reported")
}

// observedLogger returns a logger that records every entry at Debug and above, while still
// writing them to the test log for troubleshooting.
func observedLogger(t *testing.T) (*zap.Logger, *observer.ObservedLogs) {
	t.Helper()
	core, logs := observer.New(zapcore.DebugLevel)
	logger := zap.New(zapcore.NewTee(core, zaptest.NewLogger(t).Core()))
	return logger, logs
}

// entriesAtOrAbove returns the observed entries at the given level or above, formatted for
// an assertion message.
func entriesAtOrAbove(logs *observer.ObservedLogs, level zapcore.Level) []string {
	var out []string
	for _, e := range logs.All() {
		if e.Level >= level {
			out = append(out, fmt.Sprintf("%s %s %s %v", e.Level, e.LoggerName, e.Message, e.ContextMap()))
		}
	}
	return out
}

// assertNoMessageContents fails if any observed entry carries a field that could hold the
// raw bytes of an OpAMP message.
func assertNoMessageContents(t *testing.T, logs *observer.ObservedLogs) {
	t.Helper()
	for _, e := range logs.All() {
		for _, f := range e.Context {
			assert.NotContains(t, []string{"message_bytes", "message"}, f.Key, "entry %q logs message contents", e.Message)
		}
	}
}

// TestGatewayLogVerbositySteadyState checks the acceptance criteria of BP-1111: at Info, a
// gateway with connected agents emits no log lines driven by message traffic, and an agent
// connecting and disconnecting emits no Info lines either, with the detail available at Debug.
func TestGatewayLogVerbositySteadyState(t *testing.T) {
	t.Parallel()

	logger, logs := observedLogger(t)
	h := newGatewayTestHarnessWithLogger(t, 1, logger)

	// startup is allowed to log at Info
	startup := entriesAtOrAbove(logs, zapcore.WarnLevel)
	assert.Empty(t, startup, "startup should not log at Warn or above")
	logs.TakeAll()

	agent := h.NewAgent(t)

	const messageCount = 5
	for i := 1; i <= messageCount; i++ {
		agent.Send(&protobufs.AgentToServer{SequenceNum: uint64(i)})
		h.upstream.WaitForAgentMessage(t, agent.ID(), 5*time.Second)
		require.NoError(t, h.upstream.Send(&protobufs.ServerToAgent{
			InstanceUid:  agent.RawID(),
			Capabilities: uint64(i),
		}))
		agent.WaitForMessage(t, 5*time.Second)
	}

	require.NoError(t, agent.Close())
	require.Eventually(t, func() bool {
		_, ok := h.gateway.server.getDownstreamConnection(agent.ID())
		return !ok
	}, 5*time.Second, 50*time.Millisecond, "downstream connection still registered")

	// give the close handlers a moment to finish logging
	time.Sleep(100 * time.Millisecond)

	assert.Empty(t, entriesAtOrAbove(logs, zapcore.InfoLevel),
		"connecting an agent, exchanging messages and disconnecting should not log at Info or above")

	// the detail is still available at Debug
	upstreamForwarded := logs.FilterMessageSnippet(" => ").Len()
	downstreamForwarded := logs.FilterMessageSnippet(" <= ").Len()
	assert.GreaterOrEqual(t, upstreamForwarded, messageCount, "forwarded upstream messages are logged at Debug")
	assert.GreaterOrEqual(t, downstreamForwarded, messageCount, "forwarded downstream messages are logged at Debug")
	assert.Equal(t, 1, logs.FilterMessage("connection request").Len())
	assert.Equal(t, 1, logs.FilterMessage("connection accepted").Len())
	assert.Equal(t, 1, logs.FilterMessage("downstream connection closed").Len())

	assertNoMessageContents(t, logs)
}

// TestGatewayLogVerbosityUpstreamLoss checks that losing and re-establishing an upstream
// connection is reported once per transition at Warn and Info, without any Error lines, since
// the loss of a peer is an ordinary disconnect.
func TestGatewayLogVerbosityUpstreamLoss(t *testing.T) {
	t.Parallel()

	logger, logs := observedLogger(t)
	h := newGatewayTestHarnessWithLogger(t, 1, logger)

	agent := h.NewAgent(t)
	agent.Send(&protobufs.AgentToServer{SequenceNum: 1})
	h.upstream.WaitForAgentMessage(t, agent.ID(), 5*time.Second)
	logs.TakeAll()

	// close the upstream connection from the server side to simulate a network failure
	h.upstream.mu.Lock()
	for _, c := range h.upstream.connections {
		_ = c.conn.Close()
	}
	h.upstream.mu.Unlock()

	// wait for the gateway to reconnect
	h.upstream.WaitForConnection(t, 10*time.Second)
	require.Eventually(t, func() bool {
		conn, ok := h.gateway.client.upstreamConnections.get("upstream-0")
		return ok && conn.isConnected()
	}, 10*time.Second, 50*time.Millisecond)

	// the agent was closed by the gateway because its upstream connection went away
	require.Eventually(t, func() bool {
		_, ok := h.gateway.server.getDownstreamConnection(agent.ID())
		return !ok
	}, 5*time.Second, 50*time.Millisecond, "downstream connection still registered")
	time.Sleep(100 * time.Millisecond)

	assert.Empty(t, entriesAtOrAbove(logs, zapcore.ErrorLevel), "an upstream disconnect is not an error")

	lost := logs.FilterMessage("upstream connection lost")
	require.Equal(t, 1, lost.Len(), "the loss is reported exactly once")
	assert.Equal(t, zapcore.WarnLevel, lost.All()[0].Level)
	assert.EqualValues(t, 1, lost.All()[0].ContextMap()["downstream_count"])

	assert.Equal(t, 1, logs.FilterMessage("connecting to upstream OpAMP server").Len())
	assert.Equal(t, 1, logs.FilterMessage("connected to upstream OpAMP server").Len())

	// everything else about the transition is Debug
	infoAndAbove := entriesAtOrAbove(logs, zapcore.InfoLevel)
	assert.Len(t, infoAndAbove, 3, "only the loss and the reconnect are logged at Info or above: %v", infoAndAbove)

	assertNoMessageContents(t, logs)
}

// TestGatewayLogVerbosityAuthRejected checks that an agent rejected by the upstream server is
// visible at the default log level.
func TestGatewayLogVerbosityAuthRejected(t *testing.T) {
	t.Parallel()

	logger, logs := observedLogger(t)
	h := newGatewayTestHarnessWithLogger(t, 1, logger)
	logs.TakeAll()

	h.upstream.rejectConnects.Store(true)
	_, resp, err := dialTestAgent(h.agentURL)
	require.Error(t, err)
	require.NotNil(t, resp)
	require.Equal(t, http.StatusForbidden, resp.StatusCode)

	rejected := logs.FilterMessage("connection rejected by upstream OpAMP server")
	require.Equal(t, 1, rejected.Len())
	assert.Equal(t, zapcore.WarnLevel, rejected.All()[0].Level)
	assert.EqualValues(t, http.StatusForbidden, rejected.All()[0].ContextMap()["status_code"])

	assert.Empty(t, entriesAtOrAbove(logs, zapcore.ErrorLevel))
	assertNoMessageContents(t, logs)
}

// TestGatewayLogVerbosityNoUpstream checks that agents connecting while no upstream connection
// is available produce a rate-limited Warn rather than a line per attempt.
func TestGatewayLogVerbosityNoUpstream(t *testing.T) {
	t.Parallel()

	logger, logs := observedLogger(t)
	h := newGatewayTestHarnessWithLogger(t, 1, logger)

	// stop the upstream server so the gateway has no connected upstream connection
	h.upstream.Close()
	require.Eventually(t, func() bool {
		conn, ok := h.gateway.client.upstreamConnections.get("upstream-0")
		return ok && !conn.isConnected()
	}, 10*time.Second, 50*time.Millisecond)
	logs.TakeAll()

	const attempts = 5
	for i := 0; i < attempts; i++ {
		_, resp, err := dialTestAgent(h.agentURL)
		require.Error(t, err)
		require.NotNil(t, resp)
		require.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	}

	noUpstream := logs.FilterMessage("no upstream connection available")
	require.Equal(t, attempts, noUpstream.Len())

	var warns int
	for _, e := range noUpstream.All() {
		if e.Level == zapcore.WarnLevel {
			warns++
		} else {
			assert.Equal(t, zapcore.DebugLevel, e.Level)
		}
	}
	assert.Equal(t, 1, warns, "the condition is logged at Warn once per interval")

	// the reconnect attempts in the background are logged at Debug after the first one
	failed := logs.FilterMessageSnippet("upstream connection failed")
	for _, e := range failed.All() {
		if e.Level == zapcore.WarnLevel {
			assert.EqualValues(t, 1, e.ContextMap()["attempt"], "only the first attempt is a Warn")
		}
		assert.Contains(t, e.ContextMap(), "retry_in")
	}

	assert.Empty(t, entriesAtOrAbove(logs, zapcore.ErrorLevel))
}
