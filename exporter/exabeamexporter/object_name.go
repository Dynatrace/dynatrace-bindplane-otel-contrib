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

package exabeamexporter // import "github.com/dynatrace/dynatrace-bindplane-otel-contrib/exporter/exabeamexporter"

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"time"
)

// collectorType is written to every record, and objectPrefix starts every object name.
// They are placeholders until Exabeam defines a collector type for Bindplane.
const (
	collectorType = "cribl-logs"
	objectPrefix  = "exa-" + collectorType
)

// maxRandomSuffix bounds the random number at the end of the object name to 16 digits.
var maxRandomSuffix = big.NewInt(10_000_000_000_000_000)

// newObjectName returns an object name for a file written at t.
func newObjectName(collectorID string, t time.Time) (string, error) {
	n, err := rand.Int(rand.Reader, maxRandomSuffix)
	if err != nil {
		return "", fmt.Errorf("generate object name: %w", err)
	}
	return objectName(collectorID, t, n.Uint64()), nil
}

// objectName formats an object name in the form Exabeam expects:
// exa-cribl-logs_<collector_id>_1_YYYY_MM_DD_<HHMMSS><nanoseconds>_<random>.log.gz
// Objects are always written to the bucket root.
func objectName(collectorID string, t time.Time, random uint64) string {
	t = t.UTC()
	return fmt.Sprintf("%s_%s_1_%04d_%02d_%02d_%02d%02d%02d%09d_%016d.log.gz",
		objectPrefix, collectorID,
		t.Year(), t.Month(), t.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond(),
		random,
	)
}
