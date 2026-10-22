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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/collector/config/configtls"
	"go.uber.org/zap"
)

var (
	errMissingPuppetDBEndpoint = errors.New("puppetdb endpoint is required")
	errInvalidPuppetDBEndpoint = errors.New("puppetdb endpoint must be an http or https URL")
	errMissingPuppetDBFacts    = errors.New("puppetdb facts must list at least one fact path")
	errInvalidPuppetDBFact     = errors.New("invalid puppetdb fact path")
	errInvalidPuppetDBAttr     = errors.New("invalid puppetdb attribute name")
	errInvalidPuppetDBQuery    = errors.New("puppetdb query must be a JSON array")
	errNegativePuppetDBSetting = errors.New("puppetdb timeout, page_size, and max_nodes must not be negative")
	errPuppetDBNotLoaded       = errors.New("puppetdb inventory not loaded")
	errPuppetDBTooManyNodes    = errors.New("puppetdb inventory exceeds max_nodes")
	errPuppetDBPageTooLarge    = errors.New("puppetdb response page exceeds the size limit")
)

const (
	puppetDBDefaultTimeout  = 30 * time.Second
	puppetDBDefaultPageSize = 1000
	puppetDBDefaultMaxNodes = 100_000

	// puppetDBMaxPageBytes caps one page of the response. Fact selection keeps
	// rows to hundreds of bytes (about 120 bytes per node for four facts when
	// measured against live servers), so a full default page is well under a
	// megabyte and this only stops a runaway response.
	puppetDBMaxPageBytes = 64 << 20

	puppetDBInventoryPath = "/pdb/query/v4/inventory"
)

// PuppetDBConfig is the configuration for PuppetDB-based lookups. It also
// serves OpenVoxDB, which implements the same query API.
//
// Facts maps an inventory field path to the attribute name it is written as.
// Paths start with "facts." or "trusted.", or are "environment", and use
// PuppetDB's dot notation; segments containing dots are double-quoted.
type PuppetDBConfig struct {
	Endpoint           string                 `mapstructure:"endpoint"`
	TLS                configtls.ClientConfig `mapstructure:"tls"`
	Timeout            time.Duration          `mapstructure:"timeout"`
	Query              string                 `mapstructure:"query"`
	Facts              map[string]string      `mapstructure:"facts"`
	MatchShortHostname bool                   `mapstructure:"match_short_hostname"`
	PageSize           int                    `mapstructure:"page_size"`
	MaxNodes           int                    `mapstructure:"max_nodes"`
}

func (cfg *PuppetDBConfig) validate() error {
	if cfg.Endpoint == "" {
		return errMissingPuppetDBEndpoint
	}
	if u, err := url.Parse(cfg.Endpoint); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return fmt.Errorf("%w: %q", errInvalidPuppetDBEndpoint, cfg.Endpoint)
	}
	if cfg.Timeout < 0 || cfg.PageSize < 0 || cfg.MaxNodes < 0 {
		return errNegativePuppetDBSetting
	}
	if len(cfg.Facts) == 0 {
		return errMissingPuppetDBFacts
	}

	attrs := make(map[string]string, len(cfg.Facts))
	for path, attr := range cfg.Facts {
		if !validPuppetDBPath(path) {
			return fmt.Errorf("%w: %q must be \"environment\" or start with \"facts.\" or \"trusted.\", and cannot index arrays", errInvalidPuppetDBFact, path)
		}
		if attr == "" {
			return fmt.Errorf("%w: %q maps to an empty attribute name", errInvalidPuppetDBAttr, path)
		}
		if other, ok := attrs[attr]; ok {
			return fmt.Errorf("%w: %q is the target of both %q and %q", errInvalidPuppetDBAttr, attr, other, path)
		}
		attrs[attr] = path
	}

	if cfg.Query != "" {
		var q []any
		if err := json.Unmarshal([]byte(cfg.Query), &q); err != nil {
			return fmt.Errorf("%w: %w", errInvalidPuppetDBQuery, err)
		}
	}
	return nil
}

// validPuppetDBPath accepts the inventory fields extract can return. Array
// indexing ("[0]") is rejected because PuppetDB 8 and OpenVoxDB 9 both answer
// it inside extract with a 400.
func validPuppetDBPath(path string) bool {
	if path == "environment" {
		return true
	}
	if strings.Contains(path, "[") {
		return false
	}
	for _, prefix := range []string{"facts.", "trusted."} {
		if strings.HasPrefix(path, prefix) && len(path) > len(prefix) {
			return true
		}
	}
	return false
}

// puppetDBIndex is one published inventory snapshot.
type puppetDBIndex struct {
	byCertname map[string]map[string]string
	byShort    map[string]map[string]string
}

// PuppetDBSource implements LookupSource over a PuppetDB or OpenVoxDB
// inventory.
//
// Each Load pages through /pdb/query/v4/inventory with the selected facts and
// publishes a complete index through an atomic pointer, so lookups never touch
// the network and never see a partial reload. A failed load leaves the
// previous index in place.
type PuppetDBSource struct {
	cfg      *PuppetDBConfig
	logger   *zap.Logger
	client   *http.Client
	endpoint string
	paths    []string // sorted fact paths, the extract column order
	query    string
	data     atomic.Pointer[puppetDBIndex]

	// maxPageBytes is a field rather than the constant so a test can lower it.
	maxPageBytes int64
}

// NewPuppetDBSource creates a new PuppetDBSource. It does not contact the
// server; the first Load does.
func NewPuppetDBSource(cfg *PuppetDBConfig, logger *zap.Logger) (*PuppetDBSource, error) {
	tlsCfg, err := cfg.TLS.LoadTLSConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("load puppetdb tls config: %w", err)
	}

	timeout := cfg.Timeout
	if timeout == 0 {
		timeout = puppetDBDefaultTimeout
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = tlsCfg

	paths := make([]string, 0, len(cfg.Facts))
	for path := range cfg.Facts {
		paths = append(paths, path)
	}
	sort.Strings(paths)

	query, err := puppetDBQuery(paths, cfg.Query)
	if err != nil {
		return nil, err
	}

	return &PuppetDBSource{
		cfg:          cfg,
		logger:       logger.With(zap.String("endpoint", cfg.Endpoint)),
		client:       &http.Client{Timeout: timeout, Transport: transport},
		endpoint:     strings.TrimRight(cfg.Endpoint, "/") + puppetDBInventoryPath,
		paths:        paths,
		query:        query,
		maxPageBytes: puppetDBMaxPageBytes,
	}, nil
}

// Lookup returns the selected facts for a certname, or for a short hostname
// when match_short_hostname is set and the short name is unambiguous.
func (p *PuppetDBSource) Lookup(_ context.Context, key string) (map[string]string, error) {
	idx := p.data.Load()
	if idx == nil {
		return nil, errPuppetDBNotLoaded
	}
	if attrs, ok := idx.byCertname[key]; ok {
		return attrs, nil
	}
	if attrs, ok := idx.byShort[key]; ok {
		return attrs, nil
	}
	return nil, errKeyNotFound
}

// Load fetches the inventory without a caller context. The processor calls
// LoadContext instead so shutdown can cancel a reload in flight.
func (p *PuppetDBSource) Load() error {
	return p.LoadContext(context.Background())
}

// LoadContext fetches the whole inventory and publishes a new index.
func (p *PuppetDBSource) LoadContext(ctx context.Context) error {
	pageSize := p.cfg.PageSize
	if pageSize == 0 {
		pageSize = puppetDBDefaultPageSize
	}
	maxNodes := p.cfg.MaxNodes
	if maxNodes == 0 {
		maxNodes = puppetDBDefaultMaxNodes
	}

	// ponytail: offset paging is not a snapshot, so a node added or removed
	// mid-load can shift a row across a page boundary and be missed until the
	// next reload. Acceptable for enrichment; PuppetDB offers no cursor.
	byCertname := make(map[string]map[string]string)
	for offset := 0; ; offset += pageSize {
		rows, err := p.fetchPage(ctx, pageSize, offset)
		if err != nil {
			return err
		}
		for _, row := range rows {
			certname, attrs, err := p.convertRow(row)
			if err != nil {
				return err
			}
			byCertname[certname] = attrs
		}
		if len(byCertname) > maxNodes {
			return fmt.Errorf("%w (%d)", errPuppetDBTooManyNodes, maxNodes)
		}
		if len(rows) < pageSize {
			break
		}
	}

	idx := &puppetDBIndex{byCertname: byCertname}
	if p.cfg.MatchShortHostname {
		idx.byShort = p.shortIndex(byCertname)
	}
	p.data.Store(idx)
	p.logger.Debug("puppetdb inventory loaded", zap.Int("nodes", len(byCertname)))
	return nil
}

// puppetDBQuery builds the extract request: certname plus each selected path,
// ANDed with the user's filter when one is set.
func puppetDBQuery(paths []string, filter string) (string, error) {
	columns := append([]string{"certname"}, paths...)
	q := []any{"extract", columns}
	if filter != "" {
		q = append(q, json.RawMessage(filter))
	}
	out, err := json.Marshal(q)
	if err != nil {
		return "", fmt.Errorf("build puppetdb query: %w", err)
	}
	return string(out), nil
}

func (p *PuppetDBSource) fetchPage(ctx context.Context, limit, offset int) ([]map[string]any, error) {
	params := url.Values{}
	params.Set("query", p.query)
	params.Set("order_by", `[{"field":"certname"}]`)
	params.Set("limit", strconv.Itoa(limit))
	params.Set("offset", strconv.Itoa(offset))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.endpoint+"?"+params.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("build puppetdb request: %w", err)
	}
	resp, err := p.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("query puppetdb inventory: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(io.LimitReader(resp.Body, p.maxPageBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read puppetdb response: %w", err)
	}
	if int64(len(body)) > p.maxPageBytes {
		return nil, fmt.Errorf("%w (%d bytes)", errPuppetDBPageTooLarge, p.maxPageBytes)
	}

	switch {
	case resp.StatusCode == http.StatusForbidden:
		return nil, fmt.Errorf("puppetdb returned 403; the collector's client certificate name must be listed in the server's certificate-allowlist: %s", truncateForError(body))
	case resp.StatusCode != http.StatusOK:
		return nil, fmt.Errorf("puppetdb returned status %d: %s", resp.StatusCode, truncateForError(body))
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	var rows []map[string]any
	if err := dec.Decode(&rows); err != nil {
		return nil, fmt.Errorf("decode puppetdb response: %w", err)
	}
	return rows, nil
}

// convertRow maps one extract row to its certname and attributes. A null
// value means the node lacks that fact, so the attribute is left out.
func (p *PuppetDBSource) convertRow(row map[string]any) (string, map[string]string, error) {
	certname, ok := row["certname"].(string)
	if !ok || certname == "" {
		return "", nil, errors.New("puppetdb inventory row has no certname")
	}
	attrs := make(map[string]string, len(p.paths))
	for _, path := range p.paths {
		value, ok := row[path]
		if !ok || value == nil {
			continue
		}
		attrs[p.cfg.Facts[path]] = puppetDBValueString(value)
	}
	return certname, attrs, nil
}

// puppetDBValueString renders a fact value as an attribute string. Numbers
// keep their exact JSON text and structured facts become compact JSON.
func puppetDBValueString(value any) string {
	switch v := value.(type) {
	case string:
		return v
	case json.Number:
		return v.String()
	case bool:
		return strconv.FormatBool(v)
	default:
		out, _ := json.Marshal(v) // maps and slices from the decoder always marshal
		return string(out)
	}
}

// shortIndex maps each first DNS label to its node. A short name shared by
// more than one certname is left out, since picking one would enrich records
// with another host's facts.
func (p *PuppetDBSource) shortIndex(byCertname map[string]map[string]string) map[string]map[string]string {
	short := make(map[string]map[string]string, len(byCertname))
	ambiguous := make(map[string]bool)
	for certname, attrs := range byCertname {
		name, _, _ := strings.Cut(certname, ".")
		if _, dup := short[name]; dup || ambiguous[name] {
			delete(short, name)
			ambiguous[name] = true
			continue
		}
		short[name] = attrs
	}
	if len(ambiguous) > 0 {
		p.logger.Warn("short hostnames shared by more than one certname are not matched; use the full certname for these hosts",
			zap.Int("ambiguous", len(ambiguous)))
	}
	return short
}

// Close releases idle connections.
func (p *PuppetDBSource) Close() error {
	p.client.CloseIdleConnections()
	return nil
}
