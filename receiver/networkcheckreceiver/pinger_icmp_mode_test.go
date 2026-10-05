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

import (
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// resetICMPModeForTest forgets the memoized capability check so the next
// checkICMPMode call probes again.
func resetICMPModeForTest() {
	icmpMode = sync.OnceValues(detectICMPMode)
}

func TestCheckICMPModeIsBoundedAndMemoized(t *testing.T) {
	resetICMPModeForTest()

	start := time.Now()
	available, privileged := checkICMPMode()
	t.Logf("available=%v privileged=%v after %v", available, privileged, time.Since(start))
	require.LessOrEqual(t, time.Since(start), 1200*time.Millisecond, "receiver start waits on this")
	if privileged {
		require.True(t, available)
	}
	if runtime.GOOS == "darwin" && os.Geteuid() != 0 {
		require.False(t, privileged, "macOS without root has only datagram ICMP")
	}

	start = time.Now()
	again, againPrivileged := checkICMPMode()
	require.Less(t, time.Since(start), 50*time.Millisecond, "second call must be served from the memo")
	require.Equal(t, available, again)
	require.Equal(t, privileged, againPrivileged)
}
