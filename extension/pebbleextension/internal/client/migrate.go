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

package client

import (
	"fmt"

	pebblev1 "github.com/cockroachdb/pebble"
	"github.com/cockroachdb/pebble/vfs"
	"go.uber.org/zap"
)

// migrateLegacyStore upgrades a store written by pebble v1 at its default format
// (FormatMostCompatible), which pebble v2 refuses to open, by reopening it once with
// v1 at its newest format. New directories and already-upgraded stores are left alone.
func migrateLegacyStore(path string, logger *zap.Logger) error {
	desc, err := pebblev1.Peek(path, vfs.Default)
	if err != nil || !desc.Exists || desc.FormatMajorVersion >= pebblev1.FormatFlushableIngest {
		return nil
	}

	logger.Info("upgrading pebble store format for pebble v2",
		zap.String("path", path), zap.Uint64("from_format", uint64(desc.FormatMajorVersion)))
	db, err := pebblev1.Open(path, &pebblev1.Options{FormatMajorVersion: pebblev1.FormatNewest})
	if err != nil {
		return fmt.Errorf("upgrade legacy pebble store at %s: %w", path, err)
	}
	if err := db.Close(); err != nil {
		return fmt.Errorf("close upgraded pebble store at %s: %w", path, err)
	}
	return nil
}
