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

//go:build linux

package networkcheckreceiver

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
)

// The README's privilege claims for the icmp section on Linux: when the
// collector's group is outside net.ipv4.ping_group_range (root included),
// icmp_check starts fine, logs "failed to ping host" every check and emits
// nothing, so nothing is triggered. Runs only where the range excludes this
// process, e.g. docker run --sysctl net.ipv4.ping_group_range="1 0".
func TestLinux_ICMPOutsidePingGroupRange(t *testing.T) {
	b, err := os.ReadFile("/proc/sys/net/ipv4/ping_group_range")
	require.NoError(t, err)
	var lo, hi int
	_, err = fmt.Sscan(string(b), &lo, &hi)
	require.NoError(t, err)
	if gid := os.Getegid(); gid >= lo && gid <= hi {
		t.Skipf("gid %d is inside ping_group_range %d-%d", gid, lo, hi)
	}

	cfg := mustLoad(t, "collection_interval: 200ms\ninitial_delay: 0s\nicmp: {targets: [{host: 127.0.0.1, ping_count: 1, ping_timeout: 200ms}]}\ntraceroute: {}")
	core, logs := observer.New(zap.WarnLevel)
	set := settingsFor("pinggroup")
	set.Logger = zap.New(core)
	sink := new(consumertest.MetricsSink)
	r, err := NewFactory().CreateMetrics(context.Background(), set, cfg, sink)
	require.NoError(t, err)
	require.NoError(t, r.Start(context.Background(), componenttest.NewNopHost()), "not a Start failure")
	require.Eventually(t, func() bool { return logs.FilterMessage("failed to ping host").Len() >= 2 }, 5*time.Second, 10*time.Millisecond, "an error per check")
	require.NoError(t, r.Shutdown(context.Background()))

	e := logs.FilterMessage("failed to ping host").All()[0]
	require.Equal(t, "icmp", e.LoggerName)
	t.Logf("uid=%d gid=%d range=%d-%d error=%v", os.Geteuid(), os.Getegid(), lo, hi, e.ContextMap()["error"])
	require.Zero(t, sink.DataPointCount(), "nothing emitted, so nothing triggered")
}
