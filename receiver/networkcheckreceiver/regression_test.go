// Copyright Dynatrace LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package networkcheckreceiver

// These fail without fixes.patch and pass with it.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// The trigger keys failing hosts on net.peer.name; with that resource
// attribute disabled on_failure would silently never trace.
func TestOnFailureRequiresNetPeerName(t *testing.T) {
	err := confmap.Validate(mustLoad(t, "icmp: {targets: [{host: 127.0.0.1}], resource_attributes: {net.peer.name: {enabled: false}}}\ntraceroute: {}"))
	require.ErrorContains(t, err, "net.peer.name")

	cfg := mustLoad(t, "icmp: {targets: [{host: 127.0.0.1}], resource_attributes: {net.peer.name: {enabled: false}}}\ntraceroute: {on_failure: {enabled: false}, targets: [{host: 192.0.2.1}]}")
	require.NoError(t, confmap.Validate(cfg), "fine without on_failure")
}

// component.Component: Shutdown must be safe to call on a component that is
// already shut down. scraperhelper's controller closes a channel and panics.
func TestShutdownTwice(t *testing.T) {
	cfg := mustLoad(t, "initial_delay: 1h\nicmp: {targets: [{host: 192.0.2.1}]}\ntcp: {targets: [{endpoint: '127.0.0.1:9'}]}\ntraceroute: {targets: [{host: 192.0.2.1}]}")
	for _, signal := range []string{"metrics", "logs"} {
		t.Run(signal, func(t *testing.T) {
			set := settingsFor("twice-" + signal)
			var c interface {
				Shutdown(context.Context) error
			}
			if signal == "metrics" {
				mr, err := NewFactory().CreateMetrics(context.Background(), set, cfg, consumertest.NewNop())
				require.NoError(t, err)
				require.NoError(t, mr.Start(context.Background(), componenttest.NewNopHost()))
				c = mr
			} else {
				lr, err := NewFactory().CreateLogs(context.Background(), set, cfg, consumertest.NewNop())
				require.NoError(t, err)
				require.NoError(t, lr.Start(context.Background(), componenttest.NewNopHost()))
				c = lr
			}
			require.NoError(t, c.Shutdown(context.Background()))
			require.NotPanics(t, func() { require.NoError(t, c.Shutdown(context.Background())) })
		})
	}
}

// Trigger-only traceroute has no schedule, so the bounds that tie timeout and
// jitter to the inherited collection_interval must not apply.
func TestTriggerOnlyIgnoresInterval(t *testing.T) {
	cfg := mustLoad(t, "collection_interval: 2s\nicmp: {targets: [{host: 127.0.0.1}]}\ntraceroute: {}")
	require.NoError(t, confmap.Validate(cfg))

	err := confmap.Validate(mustLoad(t, "collection_interval: 2s\nicmp: {targets: [{host: 127.0.0.1}]}\ntraceroute: {targets: [{host: 192.0.2.1}]}"))
	require.ErrorContains(t, err, "timeout must not exceed collection_interval", "still enforced with a schedule")
}

// A trace into a path that stops answering costs probes_per_hop × timeout per
// silent hop; when that exceeds on_failure.timeout no triggered trace to a
// dead host can ever finish, so startup says so.
func TestWarnsWhenTriggeredTraceCannotFinish(t *testing.T) {
	for _, tc := range []struct {
		yaml string
		warn bool
	}{
		{"traceroute: {}", false},                           // 5 × 3 × 3s = 45s
		{"traceroute: {max_consecutive_timeouts: 0}", true}, // 30 × 3 × 3s
		{"traceroute: {timeout: 5s}", true},                 // 5 × 3 × 5s = 75s
		{"traceroute: {on_failure: {timeout: 44s}}", true},  // 45s > 44s
		{"traceroute: {on_failure: {enabled: false}, targets: [{host: 192.0.2.1}], max_consecutive_timeouts: 0, collection_interval: 1h}", false},
	} {
		t.Run(tc.yaml, func(t *testing.T) {
			cfg := mustLoad(t, "icmp: {targets: [{host: 127.0.0.1}]}\n"+tc.yaml)
			require.NoError(t, confmap.Validate(cfg))
			core, logs := observer.New(zap.WarnLevel)
			newSharedProber(cfg.Traceroute, zap.New(core)).start()
			require.Equal(t, tc.warn, logs.FilterMessageSnippet("on_failure.timeout").Len() == 1)
		})
	}
}
