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

import "time"

// Mode is the collection mode a buffer was in when a snapshot request arrived.
type Mode string

const (
	// ModeContinuous means the buffer was collecting between requests, so the
	// request copied the store at once.
	ModeContinuous Mode = "continuous"
	// ModeOnDemand means the buffer was idle between requests and the request
	// collected its records just in time.
	ModeOnDemand Mode = "on_demand"
)

// RequestStats describes how a snapshot request was served. Reported with the
// payload, it lets the server see which mode a buffer is in and how long a
// just-in-time collection took, which the payload alone cannot show.
type RequestStats struct {
	// Mode is the buffer's mode when the request arrived.
	Mode Mode
	// Filled reports whether the payload holds a full store's worth of records
	// collected for this request. It is always true in continuous mode. In
	// on-demand mode it is false when the fill wait expired and the last known
	// snapshot was served instead; the buffer then returns to continuous mode.
	Filled bool
	// FillWait is the time the request spent waiting for fresh records: zero
	// in continuous mode, at most the fill wait in on-demand mode.
	FillWait time.Duration
	// Duration is the time from the request's arrival until its payload was
	// ready: the fill wait plus the copy, filter, sampling and marshal.
	Duration time.Duration
	// Records is the number of items in the returned payload: log records,
	// data points or spans.
	Records int
}
