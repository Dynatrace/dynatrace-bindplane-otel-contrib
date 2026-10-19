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

func writeJSONAt(t *testing.T, contents string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "lookup.json")
	require.NoError(t, os.WriteFile(path, []byte(contents), 0o600))
	return path
}

func TestJSONLookup(t *testing.T) {
	path := writeJSONAt(t, `{
		"0.0.0.0": {"env": "prod", "region": "us-west"},
		"1.1.1.1": {"env": "dev"}
	}`)
	f := NewJSONFile(path, zap.NewNop())
	require.NoError(t, f.Load())

	got, err := f.Lookup(context.Background(), "0.0.0.0")
	require.NoError(t, err)
	require.Equal(t, "prod", got["env"])
	require.Equal(t, "us-west", got["region"])

	_, err = f.Lookup(context.Background(), "9.9.9.9")
	require.ErrorIs(t, err, errKeyNotFound)
}

// TestJSONRaggedFields is the property CSV cannot express: different keys may
// carry a different number of fields.
func TestJSONRaggedFields(t *testing.T) {
	idx, err := parseJSONIndex([]byte(`{"a": {"x": "1", "y": "2"}, "b": {"z": "3"}}`))
	require.NoError(t, err)
	require.Len(t, idx["a"], 2)
	require.Len(t, idx["b"], 1)
	require.Equal(t, "3", idx["b"]["z"])
}

func TestJSONCoercion(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
		want map[string]string
	}{
		{"string", `{"k": {"s": "hi"}}`, map[string]string{"s": "hi"}},
		{"int", `{"k": {"n": 42}}`, map[string]string{"n": "42"}},
		{"float", `{"k": {"n": 3.14}}`, map[string]string{"n": "3.14"}},
		{"big int keeps precision", `{"k": {"n": 10000000000000000001}}`, map[string]string{"n": "10000000000000000001"}},
		{"bool true", `{"k": {"b": true}}`, map[string]string{"b": "true"}},
		{"bool false", `{"k": {"b": false}}`, map[string]string{"b": "false"}},
		{"null omitted", `{"k": {"n": null, "keep": "x"}}`, map[string]string{"keep": "x"}},
		{"nested object json-encoded", `{"k": {"o": {"a": "b"}}}`, map[string]string{"o": `{"a":"b"}`}},
		{"array json-encoded", `{"k": {"arr": [1, 2, 3]}}`, map[string]string{"arr": "[1,2,3]"}},
		{"empty entry", `{"k": {}}`, map[string]string{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			idx, err := parseJSONIndex([]byte(tc.in))
			require.NoError(t, err)
			require.Equal(t, tc.want, idx["k"])
		})
	}
}

func TestJSONParseErrors(t *testing.T) {
	for _, tc := range []struct {
		name string
		in   string
	}{
		{"malformed", `{"k": {`},
		{"entry not an object", `{"k": "scalar"}`},
		{"entry is an array", `{"k": [1, 2]}`},
		{"top level not an object", `[1, 2, 3]`},
		{"top level scalar", `"hello"`},
		{"top level null", `null`},
		{"trailing data", `{"a": {}} {"b": {}}`},
		{"trailing close brace", `{"a": {}}}`},
		{"trailing close bracket", `{"a": {}}]`},
		{"trailing non-JSON whitespace", "{\"a\": {}}\f"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := parseJSONIndex([]byte(tc.in))
			require.Error(t, err)
		})
	}
}

func TestCoerceJSONValue(t *testing.T) {
	s, keep := coerceJSONValue(nil)
	require.False(t, keep)
	require.Empty(t, s)

	// A channel cannot be JSON-encoded; the coercer must drop it rather than panic.
	_, keep = coerceJSONValue(make(chan int))
	require.False(t, keep)
}

func TestJSONTrailingWhitespaceIsAllowed(t *testing.T) {
	idx, err := parseJSONIndex([]byte("{\"a\": {\"x\": \"1\"}}\n\t \r\n"))
	require.NoError(t, err)
	require.Equal(t, "1", idx["a"]["x"])
}

func TestJSONEmptyObjectLoadsWithNoKeys(t *testing.T) {
	idx, err := parseJSONIndex([]byte(`{}`))
	require.NoError(t, err)
	require.Empty(t, idx)
}

func TestNewJSONFileToleratesNilLogger(t *testing.T) {
	path := writeJSONAt(t, `{"a": {"x": "1"}}`)
	f := NewJSONFile(path, nil)
	require.NotPanics(t, func() { require.NoError(t, f.Load()) })
}

func TestJSONSourceIsNotWrappedInCache(t *testing.T) {
	path := writeJSONAt(t, `{"0.0.0.0": {"env": "prod"}}`)
	p, _ := startedProcessor(t, &Config{
		Context: attributesContext, Field: "ip", JSON: path,
		CacheEnabled: true, CacheTTL: defaultCacheTTL, CacheMaxEntries: defaultCacheMaxEntries,
	})
	_, isCache := p.source.(*LookupCache)
	require.False(t, isCache, "a JSON source must not be wrapped in LookupCache")
	require.IsType(t, &JSONFile{}, p.source, "a JSON source must be used directly")
}

// TestCSVAndJSONParityOnSameData guards the shared file machinery: the same
// logical data as CSV and as JSON must look up identically.
func TestCSVAndJSONParityOnSameData(t *testing.T) {
	csvFile := NewCSVFile(
		createTestCSVFile(t, map[string]any{"ip": "0.0.0.0", "env": "prod", "region": "us-west"}),
		"ip", zap.NewNop())
	require.NoError(t, csvFile.Load())

	jsonFile := NewJSONFile(
		writeJSONAt(t, `{"0.0.0.0": {"env": "prod", "region": "us-west"}}`), zap.NewNop())
	require.NoError(t, jsonFile.Load())

	csvGot, err := csvFile.Lookup(context.Background(), "0.0.0.0")
	require.NoError(t, err)
	jsonGot, err := jsonFile.Lookup(context.Background(), "0.0.0.0")
	require.NoError(t, err)
	require.Equal(t, csvGot, jsonGot)
}

func FuzzParseJSONIndex(f *testing.F) {
	for _, seed := range []string{
		`{}`, `{"a": {"x": "1"}}`, `{"a": {"n": 1, "b": true, "o": {"k": "v"}, "arr": [1, 2]}}`,
		`{"a": null}`, `[1,2,3]`, `"x"`, `{bad`, ``,
	} {
		f.Add([]byte(seed))
	}
	f.Fuzz(func(_ *testing.T, data []byte) {
		// A parser fed arbitrary bytes must return an error or an index, never panic.
		_, _ = parseJSONIndex(data)
	})
}
