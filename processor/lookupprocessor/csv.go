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
	"encoding/csv"
	"errors"
	"fmt"

	"go.uber.org/zap"
)

var (
	// errNoRecords is the error for when there are no records to parse
	errNoRecords = errors.New("no records to parse")
	// errLookupColumnNotFound is the error for when the lookup column is not found
	errLookupColumnNotFound = errors.New("lookup column not found")
)

// CSVFile is a lookup source backed by a CSV file. Its rows are fixed-width: the
// header names every column, so every key contributes the same set of fields.
type CSVFile struct {
	*fileSource
}

// NewCSVFile creates a new CSVFile keyed by lookupColumn.
func NewCSVFile(filepath string, lookupColumn string, logger *zap.Logger) *CSVFile {
	return &CSVFile{newFileSource(filepath, "csv", csvParser(lookupColumn), logger)}
}

// csvParser parses CSV bytes into an index keyed by lookupColumn.
func csvParser(lookupColumn string) parseFunc {
	return func(contents []byte) (index, error) {
		records, err := csv.NewReader(bytes.NewReader(contents)).ReadAll()
		if err != nil {
			return nil, fmt.Errorf("read all: %w", err)
		}
		data, err := indexRecords(records, lookupColumn)
		if err != nil {
			return nil, fmt.Errorf("index records: %w", err)
		}
		return data, nil
	}
}

// indexRecords indexes the records by the lookup column
func indexRecords(records [][]string, lookupColumn string) (index, error) {
	if len(records) == 0 {
		return nil, errNoRecords
	}

	headers := records[0]
	lookupIndex, err := findLookupIndex(headers, lookupColumn)
	if err != nil {
		return nil, fmt.Errorf("find lookup index: %w", err)
	}

	result := make(index)
	for _, record := range records[1:] {
		lookupKey := record[lookupIndex]
		result[lookupKey] = make(map[string]string)
		for i, value := range record {
			// Skip the lookup column
			if i == lookupIndex {
				continue
			}

			result[lookupKey][headers[i]] = value
		}
	}

	return result, nil
}

// findLookupIndex finds the index of the lookup column
func findLookupIndex(headers []string, lookupColumn string) (int, error) {
	for i, header := range headers {
		if header == lookupColumn {
			return i, nil
		}
	}

	return -1, errLookupColumnNotFound
}
