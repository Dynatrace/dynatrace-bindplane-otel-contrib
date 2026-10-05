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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/plog"
)

// logsN returns a single-resource payload with n records whose bodies carry
// the given prefix.
func logsN(n int, prefix string) plog.Logs {
	ld := plog.NewLogs()
	lrs := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords()
	for i := 0; i < n; i++ {
		lrs.AppendEmpty().Body().SetStr(fmt.Sprintf("%s-%d", prefix, i))
	}
	return ld
}

// simulateWindow closes a one-second window in which the pipeline offered
// batchesPerSecond batches of recordsPerBatch records, then offers one more
// single-record payload, which rolls the window over and samples it.
func simulateWindow(buf *LogBuffer, batchesPerSecond, recordsPerBatch int) {
	buf.admit.windowNs.Store(monoNow() - int64(time.Second))
	buf.admit.used.Store(buf.admit.budget)
	buf.admit.batches.Store(int64(batchesPerSecond))
	buf.admit.offered.Store(int64(batchesPerSecond * recordsPerBatch))
	buf.Add(logsN(1, "tick"))
}

// simulateFastWindow closes a window at 100 buffers' worth of records per
// second, the way sustained high throughput does.
func simulateFastWindow(buf *LogBuffer) {
	simulateWindow(buf, 1000, buf.idealSize)
}

// forceOnDemand drives a buffer into on-demand mode with consecutive
// high-throughput windows.
func forceOnDemand(tb testing.TB, buf *LogBuffer) {
	tb.Helper()
	for i := 0; i < buf.admit.fastWindowsToOnDemand; i++ {
		simulateFastWindow(buf)
	}
	buf.admit.mu.Lock()
	onDemand := buf.admit.onDemand
	buf.admit.mu.Unlock()
	require.True(tb, onDemand, "buffer should be on-demand after fast windows")
	require.False(tb, buf.admit.collecting.Load())
}

func TestOnDemandDetection(t *testing.T) {
	t.Run("consecutive fast windows switch mode and keep the store", func(t *testing.T) {
		buf := NewLogBuffer(10)
		simulateFastWindow(buf)
		simulateFastWindow(buf)
		require.True(t, buf.admit.collecting.Load(), "still continuous after two fast windows")
		require.Equal(t, 2, buf.Len())
		simulateFastWindow(buf)
		require.False(t, buf.admit.collecting.Load())
		require.Equal(t, 2, buf.Len(), "the store is kept as the last known snapshot")
	})

	t.Run("slow windows never switch", func(t *testing.T) {
		buf := NewLogBuffer(10)
		for i := 0; i < 10; i++ {
			simulateWindow(buf, 2, 100) // 200 records/s in 100-record batches
		}
		require.True(t, buf.admit.collecting.Load())
		require.Equal(t, 10, buf.Len())
	})

	t.Run("large batches at a low batch rate are not high throughput", func(t *testing.T) {
		buf := NewLogBuffer(10)
		for i := 0; i < 10; i++ {
			simulateWindow(buf, 1, 1000) // 1000 records/s but one batch per second
		}
		require.True(t, buf.admit.collecting.Load(), "a request would wait a whole batch interval")
	})

	t.Run("a slow window resets the streak", func(t *testing.T) {
		buf := NewLogBuffer(10)
		simulateFastWindow(buf)
		simulateFastWindow(buf)
		simulateWindow(buf, 2, 100)
		simulateFastWindow(buf)
		simulateFastWindow(buf)
		require.True(t, buf.admit.collecting.Load(), "streak restarted after the slow window")
		simulateFastWindow(buf)
		require.False(t, buf.admit.collecting.Load())
	})

	t.Run("mixed batch sizes are counted, not extrapolated", func(t *testing.T) {
		buf := NewLogBuffer(100)
		// 14 one-record batches and one 100-record batch per second: 114 rec/s.
		for i := 0; i < 10; i++ {
			buf.admit.windowNs.Store(monoNow() - int64(time.Second))
			buf.admit.used.Store(buf.admit.budget)
			buf.admit.batches.Store(15)
			buf.admit.offered.Store(114)
			buf.Add(logsN(1, "tick"))
		}
		require.True(t, buf.admit.collecting.Load())
	})

	t.Run("zero ideal size never switches", func(t *testing.T) {
		buf := NewLogBuffer(0)
		for i := 0; i < 5; i++ {
			simulateWindow(buf, 1000, 100)
		}
		require.True(t, buf.admit.collecting.Load())
	})

	t.Run("admit-everything disables detection", func(t *testing.T) {
		buf := NewLogBuffer(10, WithRefreshInterval(0))
		for i := 0; i < 10; i++ {
			simulateFastWindow(buf)
		}
		require.True(t, buf.admit.collecting.Load())
		require.Equal(t, 10, buf.Len())
	})
}

func TestOnDemandIdleIgnoresTraffic(t *testing.T) {
	buf := NewLogBuffer(10)
	forceOnDemand(t, buf)
	before := buf.Len()

	big := logsN(1000, "ignored")
	allocs := testing.AllocsPerRun(100, func() { buf.Add(big) })
	require.Zero(t, allocs)
	require.Equal(t, before, buf.Len())
}

func TestOnDemandHeartbeatKeepsStoreCurrent(t *testing.T) {
	buf := NewLogBuffer(10)
	forceOnDemand(t, buf)
	before := buf.Len()

	// Not due: nothing is admitted, however many payloads pass.
	for i := 0; i < 100; i++ {
		buf.Add(logsN(1, "early"))
	}
	require.Equal(t, before, buf.Len())

	// Due: the very next payload is admitted, so a pipeline that offers one
	// batch per second refreshes the store on its first batch after the
	// interval, not after some number of batches. The payload after it is
	// not admitted.
	buf.admit.lastHeartbeatNs.Store(monoNow() - int64(buf.admit.heartbeatInterval))
	buf.Add(logsN(1, "hb"))
	require.Equal(t, before+1, buf.Len())
	require.Contains(t, logBodiesNoRequest(buf), "hb-0")
	buf.Add(logsN(1, "late"))
	require.Equal(t, before+1, buf.Len())
	require.False(t, buf.admit.collecting.Load())
}

func TestOnDemandReturnsToContinuousAfterSlowWindows(t *testing.T) {
	t.Run("consecutive slow windows switch back and keep the store", func(t *testing.T) {
		buf := NewLogBuffer(10)
		forceOnDemand(t, buf)
		kept := buf.Len()

		// The pipeline settles to one 10-record batch per second: a tenth of
		// the high-throughput record rate and of the batch rate.
		for i := 1; i < buf.admit.slowWindowsToContinuous; i++ {
			simulateWindow(buf, 1, 10)
			require.False(t, buf.admit.collecting.Load(), "still on-demand after %d slow windows", i)
		}
		simulateWindow(buf, 1, 10)
		require.True(t, buf.admit.collecting.Load(), "continuous again after %d slow windows", buf.admit.slowWindowsToContinuous)
		buf.admit.mu.Lock()
		onDemand := buf.admit.onDemand
		buf.admit.mu.Unlock()
		require.False(t, onDemand)

		// The payload that closed the last window was admitted on top of the
		// kept store, and a request answers at once: no arming, no wait.
		require.Equal(t, kept+1, buf.Len())
		buf.admit.fillWait = 500 * time.Millisecond
		start := time.Now()
		bodies := logBodies(t, buf)
		require.Less(t, time.Since(start), 250*time.Millisecond, "request must not wait for a fill in continuous mode")
		require.Contains(t, bodies, "tick-0")
	})

	t.Run("a fast window restarts the slow streak", func(t *testing.T) {
		buf := NewLogBuffer(10)
		forceOnDemand(t, buf)
		simulateWindow(buf, 1, 10)
		simulateWindow(buf, 1, 10)
		simulateFastWindow(buf)
		require.False(t, buf.admit.collecting.Load())
		for i := 1; i < buf.admit.slowWindowsToContinuous; i++ {
			simulateWindow(buf, 1, 10)
			require.False(t, buf.admit.collecting.Load(), "streak restarted by the fast window")
		}
		simulateWindow(buf, 1, 10)
		require.True(t, buf.admit.collecting.Load())
	})
}

// TestOnDemandBacklogThenTrickle is the shape of a file receiver at start-up:
// a backlog drained at thousands of records per second for a few seconds,
// then a trickle. The backlog switches the buffer to on-demand mode; the
// trickle must switch it back on its own, so the first request after the
// pipeline settled answers at once with the trickle's records instead of
// waiting a whole fillWait for a fill that cannot complete.
func TestOnDemandBacklogThenTrickle(t *testing.T) {
	if testing.Short() {
		t.Skip("runs several seconds of real time")
	}
	buf := NewLogBuffer(100)

	// Backlog: 100-record batches at 50 batches/s, 5,000 records/s, for four
	// windows; three fast ones switch the mode, the fourth is margin for a
	// slow machine.
	stop := make(chan struct{})
	done := benchProducer(buf, 100, 5_000, stop)
	time.Sleep(4 * time.Second)
	close(stop)
	<-done
	require.False(t, buf.admit.collecting.Load(), "backlog should switch the buffer to on-demand mode")

	// Trickle: 2-record batches at 5 batches/s, 10 records/s.
	stop = make(chan struct{})
	done = benchProducer(buf, 2, 10, stop)
	defer func() {
		close(stop)
		<-done
	}()
	require.Eventually(t, func() bool { return buf.admit.collecting.Load() },
		8*time.Second, 50*time.Millisecond, "trickle should return the buffer to continuous mode")
	// The mode flips inside the Add of the payload that closed the slow
	// window, before that Add has appended its records; a request in that
	// gap gets the last known snapshot, which is correct but not what this
	// test is about. Let a couple of trickle batches land first.
	time.Sleep(500 * time.Millisecond)

	start := time.Now()
	payload, err := buf.ConstructPayload(&plog.ProtoMarshaler{}, nil, nil, 10<<20)
	require.NoError(t, err)
	require.Less(t, time.Since(start), 250*time.Millisecond, "request must not wait for a fill")
	newest, n := newestObserved(t, payload)
	require.Positive(t, n)
	require.Less(t, time.Since(newest), time.Second, "payload should carry the trickle's latest records")
}

// logBodiesNoRequest reads the store directly, without going through a
// snapshot request that would arm an on-demand buffer.
func logBodiesNoRequest(buf *LogBuffer) []string {
	buf.mutex.Lock()
	defer buf.mutex.Unlock()
	var bodies []string
	rls := buf.store.ResourceLogs()
	for ri := 0; ri < rls.Len(); ri++ {
		sls := rls.At(ri).ScopeLogs()
		for si := 0; si < sls.Len(); si++ {
			lrs := sls.At(si).LogRecords()
			for li := 0; li < lrs.Len(); li++ {
				bodies = append(bodies, lrs.At(li).Body().Str())
			}
		}
	}
	return bodies
}

func TestOnDemandRequestCollectsJustInTime(t *testing.T) {
	buf := NewLogBuffer(10)
	forceOnDemand(t, buf)

	// A pipeline delivering one record per millisecond.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				buf.Add(logsN(1, fmt.Sprintf("live-%d", i)))
				time.Sleep(time.Millisecond)
			}
		}
	}()

	start := time.Now()
	bodies := logBodies(t, buf)
	elapsed := time.Since(start)
	close(stop)
	wg.Wait()

	require.Len(t, bodies, 10, "request collected a full buffer")
	for _, b := range bodies {
		require.Contains(t, b, "live-", "only fresh records: %s", b)
	}
	require.Less(t, elapsed, buf.admit.fillWait, "filled before the wait expired")

	// Back to idle: the fresh records stay as the last known snapshot,
	// nothing more is collected.
	require.Equal(t, 10, buf.Len())
	require.False(t, buf.admit.collecting.Load())
	buf.admit.mu.Lock()
	require.True(t, buf.admit.onDemand)
	require.Equal(t, 0, buf.admit.waiters)
	buf.admit.mu.Unlock()
}

// TestOnDemandRequestReplacesStaleStore guards the freshness guarantee: while
// the pipeline is flowing, a request returns only records collected for it,
// not the last known snapshot the idle buffer kept.
func TestOnDemandRequestReplacesStaleStore(t *testing.T) {
	buf := NewLogBuffer(10)
	forceOnDemand(t, buf)

	// The last known snapshot an idle buffer holds.
	buf.mutex.Lock()
	buf.store = logsN(10, "stale")
	buf.count.Store(10)
	buf.mutex.Unlock()

	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				buf.Add(logsN(1, fmt.Sprintf("live-%d", i)))
				time.Sleep(time.Millisecond)
			}
		}
	}()
	bodies := logBodies(t, buf)
	close(stop)
	wg.Wait()

	require.Len(t, bodies, 10)
	for _, b := range bodies {
		require.Contains(t, b, "live-", "stale record served: %s", b)
	}
}

func TestOnDemandRequestTimeoutFallsBackToContinuous(t *testing.T) {
	t.Run("nothing arrives: the last known snapshot is returned", func(t *testing.T) {
		buf := NewLogBuffer(10)
		forceOnDemand(t, buf)
		buf.mutex.Lock()
		buf.store = logsN(4, "known")
		buf.count.Store(4)
		buf.mutex.Unlock()
		buf.admit.fillWait = 50 * time.Millisecond

		start := time.Now()
		bodies := logBodies(t, buf)
		require.GreaterOrEqual(t, time.Since(start), 50*time.Millisecond)
		require.Equal(t, []string{"known-0", "known-1", "known-2", "known-3"}, bodies)

		// The pipeline is not fast any more: collect continuously again.
		require.True(t, buf.admit.collecting.Load())
		buf.admit.mu.Lock()
		require.False(t, buf.admit.onDemand)
		buf.admit.mu.Unlock()
		buf.Add(logsN(4, "after"))
		require.Equal(t, 8, buf.Len())
	})

	t.Run("partial fill is returned and kept", func(t *testing.T) {
		buf := NewLogBuffer(10)
		forceOnDemand(t, buf)
		buf.admit.fillWait = 100 * time.Millisecond

		buf.Reset()
		go func() {
			time.Sleep(10 * time.Millisecond)
			buf.Add(logsN(3, "partial"))
		}()
		bodies := logBodies(t, buf)
		require.Len(t, bodies, 3)
		require.True(t, buf.admit.collecting.Load())
		require.Equal(t, 3, buf.Len(), "partial store is kept once continuous")
	})
}

func TestOnDemandConcurrentRequestsShareOneFill(t *testing.T) {
	buf := NewLogBuffer(10)
	forceOnDemand(t, buf)

	stop := make(chan struct{})
	var producer sync.WaitGroup
	producer.Add(1)
	go func() {
		defer producer.Done()
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
				buf.Add(logsN(2, fmt.Sprintf("live-%d", i)))
				time.Sleep(time.Millisecond)
			}
		}
	}()

	const requests = 4
	results := make([]int, requests)
	var wg sync.WaitGroup
	for r := 0; r < requests; r++ {
		wg.Add(1)
		go func(r int) {
			defer wg.Done()
			payload, err := buf.ConstructPayload(&plog.ProtoMarshaler{}, nil, nil, 1024*1024)
			require.NoError(t, err)
			ld, err := (&plog.ProtoUnmarshaler{}).UnmarshalLogs(payload)
			require.NoError(t, err)
			results[r] = ld.LogRecordCount()
		}(r)
	}
	wg.Wait()
	close(stop)
	producer.Wait()

	for r, n := range results {
		require.Equal(t, 10, n, "request %d saw a full buffer", r)
	}
	require.Equal(t, 10, buf.Len(), "store kept as the last known snapshot")
	require.False(t, buf.admit.collecting.Load())
	buf.admit.mu.Lock()
	require.True(t, buf.admit.onDemand)
	require.Equal(t, 0, buf.admit.waiters)
	buf.admit.mu.Unlock()
}

// BenchmarkLogBufferAddOnDemandIdle measures the hot path of a buffer in
// on-demand mode between requests: one atomic load per batch.
func BenchmarkLogBufferAddOnDemandIdle(b *testing.B) {
	buf := NewLogBuffer(benchIdealSize)
	forceOnDemand(b, buf)
	ld := benchLogs(1000, 10, 256)
	benchGCMetrics(b, func() {
		for i := 0; i < b.N; i++ {
			buf.Add(ld)
		}
	})
}
