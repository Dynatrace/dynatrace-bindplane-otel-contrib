// Copyright  Dynatrace LLC
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

package lookupprocessor

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestLoadWithoutInjectedClockDoesNotPanic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lookup.csv")
	writeCSVAt(t, path, "0.0.0.0", "prod")

	// A fileSource built by hand rather than through newFileSource has no clock,
	// so the stamp-settle check must fall back instead of dereferencing a nil func.
	f := &fileSource{filepath: path, name: "csv", parse: csvParser("ip"), logger: zap.NewNop()}
	require.NotPanics(t, func() { require.NoError(t, f.Load()) })

	got, err := f.Lookup(context.Background(), "0.0.0.0")
	require.NoError(t, err)
	require.Equal(t, "prod", got["env"])
}

func TestLookupBeforeLoadReportsNotLoaded(t *testing.T) {
	f := &fileSource{filepath: "unused", name: "csv", parse: csvParser("ip"), logger: zap.NewNop()}
	_, err := f.Lookup(context.Background(), "anything")
	require.ErrorIs(t, err, errSourceNotLoaded)
}

func TestLoadSurfacesReadFileErrors(t *testing.T) {
	// A directory stats fine but cannot be read as a file, so ReadFile fails.
	require.Error(t, NewCSVFile(t.TempDir(), "ip", zap.NewNop()).Load())
}

func TestCSVParserSurfacesEmptyAndMissingColumn(t *testing.T) {
	empty := filepath.Join(t.TempDir(), "empty.csv")
	require.NoError(t, os.WriteFile(empty, []byte(""), 0o600))
	require.ErrorIs(t, NewCSVFile(empty, "ip", zap.NewNop()).Load(), errNoRecords)

	noCol := filepath.Join(t.TempDir(), "nocol.csv")
	require.NoError(t, os.WriteFile(noCol, []byte("host,env\nh1,prod\n"), 0o600))
	require.ErrorIs(t, NewCSVFile(noCol, "ip", zap.NewNop()).Load(), errLookupColumnNotFound)
}
