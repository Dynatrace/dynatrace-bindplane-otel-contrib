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
	"errors"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"go.uber.org/zap"
)

var (
	// errSourceNotLoaded is returned when a lookup happens before the first load.
	errSourceNotLoaded = errors.New("lookup source not loaded")
	// errKeyNotFound is the error for when the key is not found.
	errKeyNotFound = errors.New("key not found")
)

// index maps a lookup key to the attributes attached to it. Each entry carries
// its own set of attribute keys, so different keys may contribute a different
// number of fields.
type index map[string]map[string]string

// parseFunc builds an index from the raw bytes of a lookup file. It is the only
// per-format piece of a file-backed source; csv, json, and future formats differ
// only here.
type parseFunc func([]byte) (index, error)

// fileSource is a lookup source backed by a local file whose whole contents are
// parsed into an in-memory index.
//
// The index is published through an atomic pointer rather than guarded by a
// lock, so a reload never blocks concurrent lookups. Load builds the whole
// index off to the side and swaps it in with a single Store, which leaves
// readers observing either the complete previous index or the complete new
// one, never a partial rebuild.
type fileSource struct {
	filepath string
	// name identifies the format in log messages (e.g. "csv", "json").
	name   string
	parse  parseFunc
	logger *zap.Logger
	data   atomic.Pointer[index]

	// stamp identifies the file version behind the published index, so an
	// unchanged file can be skipped without re-reading it. stampSettled records
	// whether that stamp was taken late enough after the file's own modification
	// time to identify the content unambiguously. Only Load writes them, and Load
	// runs on a single goroutine.
	stamp        fileStamp
	stampSettled bool

	// now reports the current time, and is a seam so a test can drive the
	// stamp-settle check off a fixed clock; it defaults to time.Now. reads and
	// readHook exist only for tests: reads counts completed file reads, and
	// readHook runs between a read and the re-stat that follows it. readHook is
	// nil in production.
	reads    atomic.Int64
	readHook func()
	now      func() time.Time
}

// newFileSource builds a file-backed source for the given format.
func newFileSource(filepath, name string, parse parseFunc, logger *zap.Logger) *fileSource {
	if logger == nil {
		logger = zap.NewNop()
	}
	return &fileSource{
		filepath: filepath,
		name:     name,
		parse:    parse,
		logger:   logger,
		now:      time.Now,
	}
}

// stampSettleWindow is how long a file's modification time must already be in the
// past before an equal stamp is trusted to mean the content is unchanged.
//
// A write takes its modification time from a coarse clock, so two writes landing
// close together carry byte-identical timestamps. When they also produce the same
// size, the first write's stamp is indistinguishable from the second's, and
// skipping on it would serve the earlier content for as long as the file then sat
// still. Re-reading once while the modification time is still this fresh closes
// that window, and costs one extra read per write rather than one per interval.
// The window covers the coarsest timestamp granularity still in common use, FAT's
// two seconds, so it comfortably covers the millisecond-scale granularity that
// Linux filesystems report.
const stampSettleWindow = 2 * time.Second

// fileStamp identifies a file version cheaply. It relies on a write advancing the
// modification time, which holds on any filesystem storing mtime at sub-second
// resolution. Two writes can still share a timestamp when they land in the same
// clock tick, so a stamp only identifies content once its modification time has
// settled; see stampSettleWindow. A rewrite that deliberately restores the
// previous timestamp and keeps the same size would read as unchanged.
type fileStamp struct {
	modTime time.Time
	size    int64
}

func stampOf(fi os.FileInfo) fileStamp {
	return fileStamp{modTime: fi.ModTime(), size: fi.Size()}
}

// equal compares two stamps. It uses time.Equal rather than ==, since == on a
// time.Time also compares the monotonic reading and the location pointer, which
// would report a difference for the same instant and defeat the skip.
func (s fileStamp) equal(o fileStamp) bool {
	return s.size == o.size && s.modTime.Equal(o.modTime)
}

// Load publishes a freshly built index, skipping the read entirely when the file
// has not changed since the last successful load.
func (f *fileSource) Load() error {
	before, err := os.Stat(f.filepath)
	if err != nil {
		return fmt.Errorf("stat file: %w", err)
	}

	stamp := stampOf(before)
	if f.data.Load() != nil && f.stampSettled && stamp.equal(f.stamp) {
		f.logger.Debug(f.name+" unchanged, skipping reload", zap.String("path", f.filepath))
		return nil
	}

	data, err := f.read()
	if err != nil {
		return err
	}

	// A rewrite landing mid-read can produce a truncated file that still parses
	// cleanly, which would publish a partial index. Re-stat and read once more
	// when the file moved under us. Writers that rename a completed file into
	// place avoid the window entirely, since the swap is then atomic.
	after, err := os.Stat(f.filepath)
	if err != nil {
		return fmt.Errorf("re-stat file: %w", err)
	}
	if retryStamp := stampOf(after); !retryStamp.equal(stamp) {
		f.logger.Debug(f.name+" changed while being read, retrying", zap.String("path", f.filepath))
		stamp = retryStamp
		if data, err = f.read(); err != nil {
			return err
		}
	}

	f.data.Store(&data)
	f.stamp = stamp
	// now is unset only on a fileSource built without newFileSource; fall back so
	// that path publishes rather than panics.
	now := f.now
	if now == nil {
		now = time.Now
	}
	f.stampSettled = now().Sub(stamp.modTime) >= stampSettleWindow
	return nil
}

// read reads the whole file and parses it into an index, without publishing.
func (f *fileSource) read() (index, error) {
	contents, err := os.ReadFile(f.filepath)
	if err != nil {
		return nil, fmt.Errorf("read file: %w", err)
	}

	data, err := f.parse(contents)
	if err != nil {
		return nil, err
	}

	f.reads.Add(1)
	if f.readHook != nil {
		f.readHook()
	}

	return data, nil
}

// Lookup returns the attributes indexed under key. The context is accepted to
// satisfy the LookupSource interface; file lookups are in-memory and do not
// block on it.
func (f *fileSource) Lookup(_ context.Context, key string) (map[string]string, error) {
	data := f.data.Load()
	if data == nil {
		return nil, errSourceNotLoaded
	}

	results, ok := (*data)[key]
	if !ok {
		return nil, errKeyNotFound
	}

	return results, nil
}

// Close releases resources held by the source. File data is read on demand and
// the file is closed after each read, so this is a no-op kept for LookupSource
// interface compliance.
func (f *fileSource) Close() error {
	return nil
}
