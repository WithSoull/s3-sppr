// Package traces converts local trace files; it never downloads data or accepts terms.
package traces

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/dataset"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/validator"
	"github.com/klauspost/compress/zstd"
)

const Version = "meta-cdn-v2"
const SourceRoot = "https://cache-datasets.s3.amazonaws.com/cache_dataset_txt/2023_metaCDN/"

type Config struct {
	Input       string
	SourceURL   string
	Bucket      string
	SourceShare float64
	KeyShare    float64
	SampleSeed  string
	// The trace lacks HTTP status. Require explicit opt-in to the documented proxy.
	AllowInferredSuccess bool
}

type Manifest struct {
	ConverterVersion string            `json:"converter_version"`
	ConverterSHA256  string            `json:"converter_sha256"`
	SourceURLs       []string          `json:"source_urls"`
	License          map[string]string `json:"license"`
	InputSHA256      map[string]string `json:"input_sha256"`
	SHA256           map[string]string `json:"sha256"`
	FieldMapping     map[string]string `json:"field_mapping"`
	Sampling         map[string]any    `json:"sampling"`
	TimeRange        model.Interval    `json:"time_range"`
	TimeRangeMeaning string            `json:"time_range_meaning"`
	Limitations      []string          `json:"limitations"`
	Counts           map[string]int64  `json:"counts"`
	RowPolicy        map[string]string `json:"row_policy"`
}

// SampleKey uses the same decision for every observation of a key, independent
// of order, request frequency, size/version changes and process hash randomization.
func SampleKey(key, seed string, share float64) bool {
	if share == 1 {
		return true
	}
	h := sha256.Sum256([]byte(seed + "\x00" + key))
	return binary.BigEndian.Uint64(h[:8]) < uint64(share*math.Exp2(64))
}

// Reserve the hash namespace as well as shortening long keys: a literal short
// "sha256-..." key must not alias the mapped form of another original key.
func mapKey(key string) string {
	if len(key) <= 1024 && !strings.HasPrefix(key, "sha256-") {
		return key
	}
	sum := sha256.Sum256([]byte(key))
	return "sha256-" + hex.EncodeToString(sum[:])
}

type currentVersion struct {
	size       int64
	id         string
	generation int64
}

// Convert streams CSV and output files. Memory retains selected key state and
// distinct mapped originals for collision detection/counting, never all requests.
func Convert(ctx context.Context, c Config, dir string) (Manifest, error) {
	m := Manifest{Counts: map[string]int64{}}
	counts := m.Counts
	for _, name := range []string{"input_rows", "events", "versions", "ignored_non_get", "sampled_out", "empty_key", "invalid_key", "long_key_rows", "mapped_key_rows", "mapped_keys", "selected_mapped_keys", "invalid_timestamp", "duplicate_timestamps", "out_of_order_timestamps", "invalid_op_type", "invalid_object_size", "unknown_size", "invalid_negative_size", "invalid_response_size", "no_body", "invalid_range"} {
		counts[name] = 0
	}
	if !c.AllowInferredSuccess {
		return m, fmt.Errorf("Meta has no HTTP status: explicitly supply -allow-inferred-success to use the documented GET/body proxy")
	}
	if len(c.Bucket) > 255 || !utf8.ValidString(c.Bucket) || strings.ContainsRune(c.Bucket, 0) || c.Bucket == "" || c.SourceURL == "" || c.Input == "" || math.IsNaN(c.KeyShare) || math.IsNaN(c.SourceShare) || c.KeyShare <= 0 || c.KeyShare > 1 || c.SourceShare <= 0 || c.SourceShare > 1 {
		return m, fmt.Errorf("input, source URL, bucket and sampling shares in (0,1] required")
	}
	f, err := os.Open(c.Input)
	if err != nil {
		return m, err
	}
	defer f.Close()
	h := sha256.New()
	var input io.Reader = io.TeeReader(f, h)
	if strings.HasSuffix(c.Input, ".zst") {
		z, err := zstd.NewReader(input, zstd.WithDecoderConcurrency(1), zstd.WithDecoderMaxMemory(64<<20))
		if err != nil {
			return m, err
		}
		defer z.Close()
		input = z
	}
	r := csv.NewReader(bufio.NewReaderSize(input, 256*1024))
	header, err := r.Read()
	if err != nil {
		return m, err
	}
	columns := map[string]int{}
	for i, name := range header {
		if _, ok := columns[name]; ok {
			return m, fmt.Errorf("duplicate column %s", name)
		}
		columns[name] = i
	}
	for _, name := range []string{"timestamp", "cacheKey", "OpType", "objectSize", "responseSize", "responseHeaderSize", "rangeStart", "rangeEnd"} {
		if _, ok := columns[name]; !ok {
			return m, fmt.Errorf("missing column %s", name)
		}
	}
	catalogFile, err := os.OpenFile(filepath.Join(dir, "catalog.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return m, err
	}
	defer catalogFile.Close()
	eventsFile, err := os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return m, err
	}
	defer eventsFile.Close()
	catalog := bufio.NewWriterSize(catalogFile, 256*1024)
	events := bufio.NewWriterSize(eventsFile, 256*1024)
	encoder := json.NewEncoder(events)
	if _, err := catalog.WriteString("[\n"); err != nil {
		return m, err
	}
	mappedOriginals := map[string]string{}
	states := map[string]currentVersion{}
	var first, last time.Time
	for {
		if err := ctx.Err(); err != nil {
			return m, err
		}
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return m, fmt.Errorf("CSV row %d: %w", counts["input_rows"]+2, err)
		}
		counts["input_rows"]++
		number := func(name string) (int64, error) { return strconv.ParseInt(row[columns[name]], 10, 64) }
		ms, err := number("timestamp")
		at := time.UnixMilli(ms).UTC()
		if err != nil || ms < 0 || at.Year() > 9999 {
			counts["invalid_timestamp"]++
			continue
		}
		if !last.IsZero() {
			if at.Before(last) {
				counts["out_of_order_timestamps"]++
				return m, fmt.Errorf("CSV row %d: out-of-order timestamp %s precedes %s; stable-sort the complete input by numeric timestamp before conversion (dropping/reordering would corrupt version chronology); counts=%v", counts["input_rows"]+1, at.Format(time.RFC3339Nano), last.Format(time.RFC3339Nano), counts)
			}
			if at.Equal(last) {
				counts["duplicate_timestamps"]++
			}
		}
		if first.IsZero() {
			first = at
		}
		last = at
		op, err := number("OpType")
		if err != nil {
			counts["invalid_op_type"]++
			continue
		}
		if op != 0 && op != 1 {
			counts["ignored_non_get"]++
			continue
		}
		originalKey := row[columns["cacheKey"]]
		if originalKey == "" {
			counts["empty_key"]++
			continue
		}
		if !utf8.ValidString(originalKey) || strings.ContainsRune(originalKey, 0) {
			counts["invalid_key"]++
			continue
		}
		key := mapKey(originalKey)
		if len(originalKey) > 1024 {
			counts["long_key_rows"]++
		}
		if key != originalKey {
			counts["mapped_key_rows"]++
			if prior, ok := mappedOriginals[key]; ok {
				if prior != originalKey {
					return m, fmt.Errorf("CSV row %d: SHA-256 key collision; refusing to merge distinct originals", counts["input_rows"]+1)
				}
			} else {
				mappedOriginals[key] = originalKey
				counts["mapped_keys"]++
			}
		}
		size, err := number("objectSize")
		if err != nil {
			counts["invalid_object_size"]++
			continue
		}
		if size == -1 {
			counts["unknown_size"]++
			continue
		}
		if size < 0 {
			counts["invalid_negative_size"]++
			continue
		}
		// Sampling and version IDs remain based on the original identity.
		if !SampleKey(originalKey, c.SampleSeed, c.KeyShare) {
			counts["sampled_out"]++
			continue
		}
		response, headers, from, to := int64(0), int64(0), int64(-1), int64(-1)
		if op == 1 {
			response, err = number("responseSize")
			if err != nil || response < 0 {
				counts["invalid_response_size"]++
				continue
			}
			headers, err = number("responseHeaderSize")
			if err != nil || headers < 0 {
				counts["invalid_response_size"]++
				continue
			}
			from, err = number("rangeStart")
			if err != nil {
				counts["invalid_range"]++
				continue
			}
			to, err = number("rangeEnd")
			if err != nil {
				counts["invalid_range"]++
				continue
			}
		}
		old, exists := states[key]
		if !exists || old.size != size || op == 0 {
			generation := old.generation + 1
			sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d\x00%d", originalKey, size, generation)))
			old = currentVersion{size: size, id: hex.EncodeToString(sum[:]), generation: generation}
			states[key] = old
			if !exists && key != originalKey {
				counts["selected_mapped_keys"]++
			}
			v := model.Version{VersionRef: model.VersionRef{Bucket: c.Bucket, ObjectKey: key, VersionID: old.id}, SizeBytes: size, CreatedAt: at}
			if counts["versions"] > 0 {
				if _, err := catalog.WriteString(",\n"); err != nil {
					return m, err
				}
			}
			b, err := json.Marshal(v)
			if err != nil {
				return m, err
			}
			if _, err := catalog.Write(b); err != nil {
				return m, err
			}
			counts["versions"]++
		}
		if op != 1 {
			counts["ignored_non_get"]++
			continue
		}
		if response <= headers {
			counts["no_body"]++
			continue
		}
		// responseSize includes protocol overhead. Never attribute more than the
		// object/range payload to a read; offsets in CacheBench are inclusive.
		bytes := min(size, response-headers)
		if from != -1 || to != -1 {
			if from < 0 || to < from || from >= size {
				counts["invalid_range"]++
				continue
			}
			bytes = min(bytes, min(to, size-1)-from+1)
		}
		e := model.AccessEvent{GatewayID: "trace-meta", StreamID: Version, Sequence: counts["events"] + 1, OccurredAt: at, Bucket: c.Bucket, ObjectKey: key, VersionID: old.id, Operation: model.OperationRead, Origin: model.OriginClient, Result: model.ResultSuccess, Bytes: bytes}
		if err := validator.AccessEvent(e); err != nil {
			return m, err
		}
		if err := encoder.Encode(e); err != nil {
			return m, err
		}
		counts["events"]++
	}
	if counts["events"] == 0 {
		return m, fmt.Errorf("no selected GETs with known size and response body")
	}
	if _, err := catalog.WriteString("\n]\n"); err != nil {
		return m, err
	}
	for _, err := range []error{catalog.Flush(), events.Flush(), catalogFile.Close(), eventsFile.Close()} {
		if err != nil {
			return m, err
		}
	}
	m = Manifest{ConverterVersion: Version, SourceURLs: []string{c.SourceURL, "https://cachelib.org/docs/Cache_Library_User_Guides/Cachebench_FB_HW_eval/", "https://github.com/cacheMon/cache_dataset"}, License: map[string]string{"original": "Apache-2.0 (CacheLib)", "collection": "CC-BY-4.0 (CacheMon)", "original_url": "https://github.com/facebook/CacheLib/blob/main/LICENSE", "collection_url": "https://github.com/cacheMon/cache_dataset#license", "attribution": "Meta / CacheLib; CacheMon collection curated by Juncheng Yang; converted to stage-6 JSON with key sampling and inferred versions"}, InputSHA256: map[string]string{filepath.Base(c.Input): hex.EncodeToString(h.Sum(nil))}, SHA256: map[string]string{}, FieldMapping: map[string]string{"OccurredAt": "timestamp: Unix milliseconds → UTC", "ObjectKey": "cacheKey unchanged up to 1024 UTF-8 bytes, except reserved sha256- prefix; otherwise sha256- + full lowercase SHA256(original UTF-8 bytes); collision checked, sampling/version IDs use original", "Bucket": "CLI bucket (one region per dataset)", "VersionID": "SHA256(key NUL size NUL generation); first observation and size transition or kSet create version", "CreatedAt": "first observation with known size of inferred version", "Operation/Origin/Result": "OpType=1 (kGet), responseSize>responseHeaderSize → read/client/success; inferred, HTTP status absent", "Bytes": "min(objectSize,responseSize-responseHeaderSize,inclusive range length if present)", "Sequence": "contiguous emitted read counter; Payload=null", "non_get": "kSet=0 updates catalog and creates version but emits no read; other operations ignored"}, Sampling: map[string]any{"source_file_share": c.SourceShare, "key_share": c.KeyShare, "seed": c.SampleSeed, "algorithm": "first 64 bits of SHA256(seed NUL key), big endian, < floor(share * 2^64)", "SamplingRate": "not multiplied: collection mixes hosts; source_file_share is the filename share, not global population coverage", "counts_rescaled": false}, TimeRange: model.Interval{Start: first, End: last}, TimeRangeMeaning: "min/max valid timestamps in full input, inclusive; not a completeness watermark or declared capture boundary", Counts: counts, Limitations: []string{"HTTP status absent: GET with positive response body is only a success proxy, explicitly opted in; cache_hit=0 is not treated as failure", "creation time is first observation with known size, not actual object creation", "unknown objectSize=-1 and invalid rows are skipped with counters, never imputed from a range response; counts describe exclusions, not complete production demand", "same-size overwrites without kSet are undetectable; size returning to a prior value creates another generation", "no deletion information: prior inferred versions are retained in catalog; their existence is an assumption", "bucket aggregates describe selected trace keys, not the entire production bucket; counts are not upscaled", "response payload is capped to object/range size; protocol overhead cannot be recovered exactly"}}
	m.RowPolicy = map[string]string{
		"counts_scope": "anomaly counters before sampling cover all supported operations; invalid response/range counters cover selected known-size GETs; skipped-row reasons are first-match, duplicate timestamps are not deduplication",
		"keys":         "empty/invalid UTF-8/NUL keys skipped; long and reserved-prefix keys hash-mapped; mapped_keys counts distinct originals before sampling, selected_mapped_keys counts those written to catalog",
		"sizes":        "-1 unknown skipped; other negatives and non-integer/overflow sizes skipped; no inference from response bytes",
		"timestamps":   "invalid skipped; equal valid timestamps accepted in input order and counted; any regression is fatal, stable numeric external sort required",
		"structure":    "missing/duplicate headers, malformed CSV, truncated zstd, hash collision, invalid configuration, I/O failure and no usable reads are fatal; no finished dataset is published",
		"duplicates":   "timestamp equality does not identify duplicate requests; all such reads retained with distinct Sequence",
	}

	for _, name := range []string{"catalog.json", "events.jsonl"} {
		sum, err := dataset.HashFile(filepath.Join(dir, name))
		if err != nil {
			return m, err
		}
		m.SHA256[name] = sum
	}
	// Hash the running converter binary so the manifest identifies its actual implementation.
	executable, err := os.Executable()
	if err != nil {
		return m, err
	}
	m.ConverterSHA256, err = dataset.HashFile(executable)
	if err != nil {
		return m, err
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return m, err
	}
	return m, os.WriteFile(filepath.Join(dir, "manifest.json"), append(b, '\n'), 0644)
}
