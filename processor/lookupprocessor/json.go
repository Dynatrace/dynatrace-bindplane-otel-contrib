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
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"

	"go.uber.org/zap"
)

// JSONFile is a lookup source backed by a JSON file. Unlike CSV's fixed-width
// rows, each key maps to its own object, so different keys may carry a different
// set of fields.
type JSONFile struct {
	*fileSource
}

// NewJSONFile creates a new JSONFile.
func NewJSONFile(filepath string, logger *zap.Logger) *JSONFile {
	return &JSONFile{newFileSource(filepath, "json", parseJSONIndex, logger)}
}

// parseJSONIndex parses a JSON object-of-objects (lookup key to attribute
// object) into an index, coercing every value to a string via coerceJSONValue.
// A non-object top level, a non-object entry, and trailing data are all rejected.
func parseJSONIndex(contents []byte) (index, error) {
	dec := json.NewDecoder(bytes.NewReader(contents))
	dec.UseNumber()

	var raw map[string]map[string]any
	if err := dec.Decode(&raw); err != nil {
		return nil, fmt.Errorf("parse json: %w", err)
	}
	if raw == nil { // top-level null decodes without error
		return nil, fmt.Errorf("parse json: top level must be an object, got null")
	}
	// Decoder.More reports false before a closing '}' or ']', so check the rest
	// of the input directly: only JSON whitespace may follow the object.
	if len(bytes.Trim(contents[dec.InputOffset():], " \t\r\n")) > 0 {
		return nil, fmt.Errorf("parse json: unexpected trailing data after top-level object")
	}

	result := make(index, len(raw))
	for key, entry := range raw {
		fields := make(map[string]string, len(entry))
		for name, value := range entry {
			if s, keep := coerceJSONValue(value); keep {
				fields[name] = s
			}
		}
		result[key] = fields
	}

	return result, nil
}

// coerceJSONValue turns a decoded JSON value into a string, returning keep=false
// when the value contributes no field. string passes through, a number keeps its
// exact source text (UseNumber, so no precision loss or added decimal point), a
// bool becomes "true"/"false", an object or array is re-encoded as compact JSON,
// and null is dropped.
func coerceJSONValue(value any) (s string, keep bool) {
	switch v := value.(type) {
	case nil:
		return "", false
	case string:
		return v, true
	case json.Number:
		return v.String(), true
	case bool:
		return strconv.FormatBool(v), true
	default:
		// Object or array. encoding/json sorts map keys, so the result is stable.
		encoded, err := json.Marshal(v)
		if err != nil {
			return "", false
		}
		return string(encoded), true
	}
}
