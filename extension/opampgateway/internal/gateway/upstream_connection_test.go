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
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/dynatrace/dynatrace-bindplane-otel-contrib/extension/opampgateway/internal/metadata"
	"github.com/gorilla/websocket"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.uber.org/zap"
)

func TestUpstreamConnectionHeader(t *testing.T) {
	const userAgent = "opamp-gateway/v1.9.0 bindplane-otel-collector/v2.0.1 (linux/amd64)"

	testCases := []struct {
		name            string
		headers         http.Header
		expectUserAgent string
	}{
		{
			name:            "no configured headers",
			headers:         nil,
			expectUserAgent: userAgent,
		},
		{
			name:            "unrelated configured header",
			headers:         http.Header{"Authorization": []string{"Secret-Key secret"}},
			expectUserAgent: userAgent,
		},
		{
			name:            "configured User-Agent takes precedence",
			headers:         http.Header{"User-Agent": []string{"custom/1.0.0"}},
			expectUserAgent: "custom/1.0.0",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			c := newUpstreamConnection(*websocket.DefaultDialer, nil, upstreamConnectionSettings{
				endpoint:  "ws://localhost:4320/v1/opamp",
				headers:   tc.headers,
				userAgent: userAgent,
			}, "upstream-0", zap.NewNop())

			h := c.header("upstream-0")
			require.Equal(t, "upstream-0", h.Get("X-Opamp-Gateway-Connection-Id"))
			require.Equal(t, tc.expectUserAgent, h.Get("User-Agent"))

			// the configured headers must not be modified
			require.Equal(t, tc.headers, c.settings.headers)
		})
	}
}

func TestUpstreamConnectionHeaderAgentID(t *testing.T) {
	t.Run("without agent id", func(t *testing.T) {
		c := newUpstreamConnection(*websocket.DefaultDialer, nil, upstreamConnectionSettings{}, "upstream-0", zap.NewNop())
		h := c.header("upstream-0")
		require.Equal(t, "upstream-0", h.Get("X-Opamp-Gateway-Connection-Id"))
	})

	t.Run("with agent id", func(t *testing.T) {
		c := newUpstreamConnection(*websocket.DefaultDialer, nil, upstreamConnectionSettings{
			agentID: "01HZ7X0C7S7ZQ6M4R2E7F9K3BA",
		}, "upstream-2", zap.NewNop())
		h := c.header("upstream-2")
		require.Equal(t, "01HZ7X0C7S7ZQ6M4R2E7F9K3BA/upstream-2", h.Get("X-Opamp-Gateway-Connection-Id"))
	})

	t.Run("preserves configured headers", func(t *testing.T) {
		c := newUpstreamConnection(*websocket.DefaultDialer, nil, upstreamConnectionSettings{
			headers: http.Header{"Authorization": []string{"Secret-Key abc"}},
			agentID: "agent-1",
		}, "upstream-0", zap.NewNop())
		h := c.header("upstream-0")
		require.Equal(t, "Secret-Key abc", h.Get("Authorization"))
		require.Equal(t, "agent-1/upstream-0", h.Get("X-Opamp-Gateway-Connection-Id"))
		// the configured headers must not be mutated
		require.Empty(t, c.settings.headers.Get("X-Opamp-Gateway-Connection-Id"))
	})
}

// sendResult runs send on its own goroutine, so that a test fails instead of blocking.
func sendResult(c *upstreamConnection, msg *message) <-chan error {
	result := make(chan error, 1)
	go func() { result <- c.send(msg) }()
	return result
}

func TestUpstreamConnectionSendForAnOldSocketDoesNotWait(t *testing.T) {
	c := newUpstreamConnection(*websocket.DefaultDialer, nil, upstreamConnectionSettings{}, "upstream-0", zap.NewNop())

	msg := newMessage(0, nil)
	msg.generation = 1
	select {
	case err := <-sendResult(c, msg):
		require.ErrorIs(t, err, errUpstreamSocketReplaced)
	case <-time.After(5 * time.Second):
		t.Fatal("send waits for a socket that is not connected")
	}
}

func TestUpstreamConnectionSendStopsWhenItsSocketCloses(t *testing.T) {
	c := newUpstreamConnection(*websocket.DefaultDialer, nil, upstreamConnectionSettings{}, "upstream-0", zap.NewNop())
	socketDone := make(chan struct{})
	c.socketMtx.Lock()
	generation := c.generation.Add(1)
	c.socketDone = socketDone
	c.socketMtx.Unlock()

	// No writer reads the message, as when the writer connects again.
	msg := newMessage(0, nil)
	msg.generation = generation
	result := sendResult(c, msg)
	close(socketDone)
	select {
	case err := <-result:
		require.ErrorIs(t, err, errUpstreamSocketReplaced)
	case <-time.After(5 * time.Second):
		t.Fatal("send waits after its socket closed")
	}
}

func TestUpstreamConnectionRecordsTheSocketThatWroteTheMessage(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	t.Cleanup(srv.Close)

	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(srv.URL, "http"), nil)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	telemetry, err := metadata.NewTelemetryBuilder(componenttest.NewNopTelemetrySettings())
	require.NoError(t, err)
	c := newUpstreamConnection(*websocket.DefaultDialer, telemetry, upstreamConnectionSettings{}, "upstream-0", zap.NewNop())
	c.generation.Store(4)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = c.writerLoop(ctx, conn, nil)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	msg := newMessage(0, []byte{0})
	require.NoError(t, c.send(msg))
	require.Equal(t, uint64(5), msg.writtenGeneration)
}
