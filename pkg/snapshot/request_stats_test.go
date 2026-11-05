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

package snapshot

import (
	"os"
	"runtime"
	"strconv"
	"testing"
	"time"

	"github.com/jonboulle/clockwork"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

const statsPayloadLimit = 10 << 20

func TestRequestStatsContinuous(t *testing.T) {
	buf := NewLogBuffer(10)
	buf.Add(logsN(3, "a"))

	payload, stats, err := buf.ConstructPayloadWithStats(&plog.ProtoMarshaler{}, nil, nil, statsPayloadLimit)
	require.NoError(t, err)
	require.Equal(t, ModeContinuous, stats.Mode)
	require.True(t, stats.Filled)
	require.Zero(t, stats.FillWait)
	require.Equal(t, 3, stats.Records)
	require.Positive(t, stats.Duration)

	// The plain method returns the same payload and only drops the stats.
	plain, err := buf.ConstructPayload(&plog.ProtoMarshaler{}, nil, nil, statsPayloadLimit)
	require.NoError(t, err)
	require.Equal(t, payload, plain)
}

func TestRequestStatsCountsWhatTheFilterKept(t *testing.T) {
	buf := NewLogBuffer(10)
	buf.Add(logsN(4, "keep"))
	buf.Add(logsN(2, "drop"))
	query := "keep"

	_, stats, err := buf.ConstructPayloadWithStats(&plog.ProtoMarshaler{}, &query, nil, statsPayloadLimit)
	require.NoError(t, err)
	require.Equal(t, 4, stats.Records)
}

func TestRequestStatsOnDemandFilled(t *testing.T) {
	buf := NewLogBuffer(10)
	forceOnDemand(t, buf)

	// A pipeline delivering a thousand records per second fills the ten-record
	// store well inside the fill wait.
	stop := make(chan struct{})
	done := benchProducer(buf, 5, 1_000, stop)
	defer func() {
		close(stop)
		<-done
	}()

	_, stats, err := buf.ConstructPayloadWithStats(&plog.ProtoMarshaler{}, nil, nil, statsPayloadLimit)
	require.NoError(t, err)
	require.Equal(t, ModeOnDemand, stats.Mode)
	require.True(t, stats.Filled)
	require.Positive(t, stats.FillWait)
	require.Less(t, stats.FillWait, fillWait)
	require.GreaterOrEqual(t, stats.Duration, stats.FillWait)
	require.Equal(t, 10, stats.Records)
}

func TestRequestStatsOnDemandTimedOut(t *testing.T) {
	buf := NewLogBuffer(10)
	forceOnDemand(t, buf)
	lastKnown := buf.Len()
	buf.admit.fillWait = 50 * time.Millisecond

	_, stats, err := buf.ConstructPayloadWithStats(&plog.ProtoMarshaler{}, nil, nil, statsPayloadLimit)
	require.NoError(t, err)
	require.Equal(t, ModeOnDemand, stats.Mode)
	require.False(t, stats.Filled, "nothing arrived, so the wait must expire")
	require.GreaterOrEqual(t, stats.FillWait, 50*time.Millisecond)
	require.Less(t, stats.FillWait, time.Second)
	require.Equal(t, lastKnown, stats.Records, "the last known snapshot is served")
	require.True(t, buf.admit.collecting.Load(), "a timed-out wait returns the buffer to continuous mode")

	// The next request is served at once, in continuous mode.
	_, stats, err = buf.ConstructPayloadWithStats(&plog.ProtoMarshaler{}, nil, nil, statsPayloadLimit)
	require.NoError(t, err)
	require.Equal(t, ModeContinuous, stats.Mode)
	require.Zero(t, stats.FillWait)
}

func TestRequestStatsSampledPayload(t *testing.T) {
	buf := NewLogBuffer(100, WithRefreshInterval(0))
	buf.Add(benchLogs(100, 10, 256))
	full, _, err := buf.ConstructPayloadWithStats(&plog.ProtoMarshaler{}, nil, nil, statsPayloadLimit)
	require.NoError(t, err)
	fullSize, err := compressedSize(full)
	require.NoError(t, err)

	// A limit of about half the full payload forces the sampler to drop records.
	_, stats, err := buf.ConstructPayloadWithStats(&plog.ProtoMarshaler{}, nil, nil, fullSize/2)
	require.NoError(t, err)
	require.Less(t, stats.Records, 100)
	require.Positive(t, stats.Records)

	// Nothing fits: the error path still reports the mode and the time spent.
	_, stats, err = buf.ConstructPayloadWithStats(&plog.ProtoMarshaler{}, nil, nil, 1)
	require.Error(t, err)
	require.Equal(t, ModeContinuous, stats.Mode)
	require.Positive(t, stats.Duration)
}

func TestRequestStatsMetricsAndTraces(t *testing.T) {
	mb := NewMetricBuffer(50)
	mb.Add(benchMetrics(7, 2))
	_, mstats, err := mb.ConstructPayloadWithStats(&pmetric.ProtoMarshaler{}, nil, nil, statsPayloadLimit)
	require.NoError(t, err)
	require.Equal(t, ModeContinuous, mstats.Mode)
	require.Equal(t, 7, mstats.Records)

	tb := NewTraceBuffer(50)
	tb.Add(benchTraces(5, 2))
	_, tstats, err := tb.ConstructPayloadWithStats(&ptrace.ProtoMarshaler{}, nil, nil, statsPayloadLimit)
	require.NoError(t, err)
	require.Equal(t, ModeContinuous, tstats.Mode)
	require.Equal(t, 5, tstats.Records)
}

func TestRequestStatsEmptyBuffer(t *testing.T) {
	buf := NewLogBuffer(10)
	payload, stats, err := buf.ConstructPayloadWithStats(&plog.ProtoMarshaler{}, nil, nil, statsPayloadLimit)
	require.NoError(t, err)
	require.NotNil(t, payload)
	require.Equal(t, ModeContinuous, stats.Mode)
	require.True(t, stats.Filled)
	require.Zero(t, stats.Records)
}

func TestRequestStatsConcurrentRequestsShareOneFill(t *testing.T) {
	buf := NewLogBuffer(10)
	forceOnDemand(t, buf)
	stop := make(chan struct{})
	done := benchProducer(buf, 5, 1_000, stop)
	defer func() {
		close(stop)
		<-done
	}()

	const n = 4
	results := make(chan RequestStats, n)
	for i := 0; i < n; i++ {
		go func() {
			_, stats, err := buf.ConstructPayloadWithStats(&plog.ProtoMarshaler{}, nil, nil, statsPayloadLimit)
			require.NoError(t, err)
			results <- stats
		}()
	}
	for i := 0; i < n; i++ {
		stats := <-results
		// Every concurrent request is served by the same fill: on-demand,
		// filled, and each reports how long that fill took, even one that
		// arrived after the store had already filled.
		require.Equal(t, ModeOnDemand, stats.Mode)
		require.True(t, stats.Filled)
		require.Positive(t, stats.FillWait)
		require.Less(t, stats.FillWait, fillWait)
		require.Equal(t, 10, stats.Records)
	}
	require.False(t, buf.admit.collecting.Load(), "buffer is idle again once the last request has left")
}

// TestRequestFillWaitIsTheFillTime drives the clock by hand. A request that
// joins a cycle after the store has already filled reports the fill's
// duration, the same as the request that started the cycle, not the time
// since the buffer was armed.
func TestRequestFillWaitIsTheFillTime(t *testing.T) {
	clock := clockwork.NewFakeClockAt(time.Now())
	buf := NewLogBuffer(10, withClock(clock))
	forceOnDemand(t, buf)
	a := &buf.admit

	type result struct {
		stats RequestStats
		done  func()
	}
	first := make(chan result, 1)
	go func() {
		stats, done := a.request()
		first <- result{stats, done}
	}()
	require.NoError(t, clock.BlockUntilContext(t.Context(), 1)) // the first request is waiting on its fill timer
	clock.Advance(50 * time.Millisecond)
	a.full()
	r1 := <-first

	// The second request joins the same cycle well after the fill completed.
	clock.Advance(300 * time.Millisecond)
	stats2, done2 := a.request()
	done2()
	r1.done()

	for _, stats := range []RequestStats{r1.stats, stats2} {
		require.Equal(t, ModeOnDemand, stats.Mode)
		require.True(t, stats.Filled)
		require.Equal(t, 50*time.Millisecond, stats.FillWait)
	}
	require.False(t, a.armed.Load(), "the last request disarms the buffer")
}

// TestRequestFillWaitOnTimeout checks that when the wait expires FillWait is
// the wait, for the request that started the cycle and for one that joined
// it later.
func TestRequestFillWaitOnTimeout(t *testing.T) {
	clock := clockwork.NewFakeClockAt(time.Now())
	buf := NewLogBuffer(10, withClock(clock))
	forceOnDemand(t, buf)
	a := &buf.admit

	results := make(chan RequestStats, 2)
	ask := func() {
		stats, done := a.request()
		done()
		results <- stats
	}
	go ask()
	require.NoError(t, clock.BlockUntilContext(t.Context(), 1))
	clock.Advance(300 * time.Millisecond)
	go ask()
	require.NoError(t, clock.BlockUntilContext(t.Context(), 2))
	clock.Advance(fillWait - 300*time.Millisecond) // the first request's wait expires
	clock.Advance(300 * time.Millisecond)          // then the second's
	for i := 0; i < 2; i++ {
		stats := <-results
		require.Equal(t, ModeOnDemand, stats.Mode)
		require.False(t, stats.Filled)
		require.Equal(t, fillWait, stats.FillWait)
	}
	require.True(t, buf.admit.collecting.Load(), "an expired wait returns the buffer to continuous mode")
}

// TestRequestSoakHeapStaysFlat hammers an on-demand buffer with requests while
// a pipeline feeds it and checks that the live heap does not grow. It runs
// only when SNAPSHOT_SOAK_SECONDS is set, because it takes that many seconds.
func TestRequestSoakHeapStaysFlat(t *testing.T) {
	seconds, _ := strconv.Atoi(os.Getenv("SNAPSHOT_SOAK_SECONDS"))
	if seconds <= 0 {
		t.Skip("set SNAPSHOT_SOAK_SECONDS to run")
	}
	buf := NewLogBuffer(100)
	forceOnDemand(t, buf)
	stop := make(chan struct{})
	done := benchProducer(buf, 100, 5_000, stop)
	defer func() {
		close(stop)
		<-done
	}()

	heap := func() uint64 {
		// Two cycles: sync.Pool keeps a victim cache for one GC, and the
		// pooled gzip writers are not live memory.
		runtime.GC()
		runtime.GC()
		var m runtime.MemStats
		runtime.ReadMemStats(&m)
		return m.HeapAlloc
	}
	requests, onDemand, timedOut := 0, 0, 0
	deadline := time.Now().Add(time.Duration(seconds) * time.Second)
	before := heap()
	for time.Now().Before(deadline) {
		_, stats, err := buf.ConstructPayloadWithStats(&plog.ProtoMarshaler{}, nil, nil, statsPayloadLimit)
		require.NoError(t, err)
		requests++
		if stats.Mode == ModeOnDemand {
			onDemand++
		}
		if !stats.Filled {
			timedOut++
		}
	}
	after := heap()
	goroutines := runtime.NumGoroutine()
	t.Logf("requests=%d on_demand=%d timed_out=%d heap_before=%d heap_after=%d goroutines=%d", requests, onDemand, timedOut, before, after, goroutines)
	require.Equal(t, requests, onDemand, "every request was served on-demand")
	require.Less(t, float64(after), float64(before)*1.5+8<<20, "live heap grew across the soak")
}
