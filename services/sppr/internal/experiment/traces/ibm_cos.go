package traces

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

const COSSource = "https://iotta.snia.org/traces/key-value/36305"

type COSConfig struct {
	Config
	Origin time.Time
}

type cosState struct {
	index      int
	generation int64
	lastRead   time.Time
}

func ConvertCOS(ctx context.Context, c COSConfig, dir string) (Manifest, error) {
	m := Manifest{ConverterVersion: "ibm-cos-v1", Counts: map[string]int64{}}
	n := m.Counts
	for _, k := range []string{"input_rows", "get_rows", "put_rows", "delete_rows", "events", "versions", "head_excluded", "copy_excluded", "copy_seen_before", "copy_read_afterwards", "copy_distinct_ids", "copy_distinct_read_afterwards", "sampled_out", "invalid_timestamp", "invalid_key", "invalid_size", "invalid_range", "range_end_clamped", "unknown_operation", "size_conflicts", "reads_after_delete", "inferred_initial_versions", "overwrites", "deletes", "delete_without_current", "duplicate_timestamps", "out_of_order_timestamps"} {
		n[k] = 0
	}
	if err := validateTrace(c.Config); err != nil {
		return m, err
	}
	if !c.AllowInferredSuccess {
		return m, fmt.Errorf("IBM COS has no HTTP status: supply -allow-inferred-success")
	}
	if c.Origin.IsZero() || c.Origin.Year() < 1 || c.Origin.Year() > 9999 {
		return m, fmt.Errorf("IBM COS requires explicit absolute -origin RFC3339")
	}
	c.Origin = c.Origin.UTC()
	f, err := os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return m, err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 256*1024)
	enc := json.NewEncoder(w)
	var versions []model.Version
	states := map[string]cosState{}
	seen := map[string]bool{}
	copies := map[string]int64{}
	copyRead := map[string]bool{}
	var first, last time.Time
	members := 0
	inputHash, err := scanArchive(ctx, c.Input, true, func(name string, r io.Reader) error {
		members++
		if members != 1 {
			return fmt.Errorf("one dataset per COS file: archive contains multiple regular members")
		}
		s := bufio.NewScanner(r)
		s.Buffer(make([]byte, 4096), 64*1024)
		for s.Scan() {
			if err := ctx.Err(); err != nil {
				return err
			}
			n["input_rows"]++
			fields := strings.Fields(s.Text())
			if len(fields) != 4 && len(fields) != 6 && !(len(fields) == 3 && fields[1] == "REST.DELETE.OBJECT") {
				return fmt.Errorf("line %d: expected 4 or 6 fields (DELETE may omit size)", n["input_rows"])
			}
			ms, e := strconv.ParseInt(fields[0], 10, 64)
			// Bound before duration multiplication and calendar conversion.
			if e != nil || ms < 0 || ms > math.MaxInt64/int64(time.Millisecond) {
				n["invalid_timestamp"]++
				continue
			}
			at := c.Origin.Add(time.Duration(ms) * time.Millisecond)
			if at.Year() > 9999 {
				n["invalid_timestamp"]++
				continue
			}
			if !last.IsZero() && at.Before(last) {
				n["out_of_order_timestamps"]++
				return fmt.Errorf("line %d: timestamp regression", n["input_rows"])
			}
			if at.Equal(last) {
				n["duplicate_timestamps"]++
			}
			if first.IsZero() {
				first = at
			}
			last = at
			op, id := fields[1], fields[2]
			if !validKey(id) {
				n["invalid_key"]++
				continue
			}
			if op == "REST.COPY.OBJECT" {
				n["copy_excluded"]++
				if seen[id] {
					n["copy_seen_before"]++
				}
				copies[id]++
				seen[id] = true
				continue
			}
			seen[id] = true
			if op == "REST.GET.OBJECT" && copies[id] > 0 {
				n["copy_read_afterwards"] += copies[id]
				copies[id] = 0
				copyRead[id] = true
			}
			if op == "REST.HEAD.OBJECT" {
				n["head_excluded"]++
			}
			if op != "REST.GET.OBJECT" && op != "REST.PUT.OBJECT" && op != "REST.DELETE.OBJECT" && op != "REST.HEAD.OBJECT" {
				n["unknown_operation"]++
				continue
			}
			if op == "REST.GET.OBJECT" {
				n["get_rows"]++
			}
			if op == "REST.PUT.OBJECT" {
				n["put_rows"]++
			}
			if op == "REST.DELETE.OBJECT" {
				n["delete_rows"]++
			}
			size := int64(0)
			if len(fields) >= 4 {
				size, e = strconv.ParseInt(fields[3], 10, 64)
			}
			if op != "REST.DELETE.OBJECT" && (e != nil || size < 0) {
				n["invalid_size"]++
				continue
			}
			payload := size
			if op == "REST.GET.OBJECT" && len(fields) == 6 {
				start, e1 := strconv.ParseInt(fields[4], 10, 64)
				end, e2 := strconv.ParseInt(fields[5], 10, 64)
				if size == 0 && start == 0 && end == -1 && e1 == nil && e2 == nil {
					payload = 0
				} else if e1 != nil || e2 != nil || start < 0 || end < start || start >= size {
					n["invalid_range"]++
					continue
				}
				if size > 0 && end >= size {
					n["range_end_clamped"]++
				}
				payload = max(int64(0), min(end, size-1)-start+1)
			} else if len(fields) != 4 && op != "REST.DELETE.OBJECT" {
				return fmt.Errorf("line %d: range allowed only for GET", n["input_rows"])
			}
			if !SampleKey(id, c.SampleSeed, c.KeyShare) {
				n["sampled_out"]++
				continue
			}
			key := mapKey(id)
			st, known := states[key]
			active := known && st.index >= 0
			if op == "REST.DELETE.OBJECT" || op == "REST.PUT.OBJECT" {
				if active {
					if st.lastRead.Equal(at) {
						return fmt.Errorf("line %d: mutation and prior read of same version share a millisecond; cannot express ordered lifetime", n["input_rows"])
					}
					versions[st.index].DeletedAt = &at
					if op == "REST.PUT.OBJECT" {
						n["overwrites"]++
					}
				}
				if op == "REST.DELETE.OBJECT" {
					n["deletes"]++
					if !active {
						n["delete_without_current"]++
					}
					st.index = -1
					states[key] = st
					continue
				}
			}
			if !active || op == "REST.PUT.OBJECT" {
				created := at
				if !known && op != "REST.PUT.OBJECT" {
					created = c.Origin
					n["inferred_initial_versions"]++
				}
				if known && !active && op != "REST.PUT.OBJECT" {
					n["reads_after_delete"]++
				}
				st.generation++
				st.index = len(versions)
				st.lastRead = time.Time{}
				versions = append(versions, model.Version{VersionRef: model.VersionRef{Bucket: c.Bucket, ObjectKey: key, VersionID: versionID(id, st.generation)}, SizeBytes: size, CreatedAt: created})
				n["versions"]++
			}
			states[key] = st
			if op != "REST.GET.OBJECT" {
				continue
			}
			v := versions[st.index]
			if v.SizeBytes != size {
				n["size_conflicts"]++
				continue
			}
			if err := emitRead(enc, c.Config, "ibm-cos", v, model.AccessEvent{OccurredAt: at, Sequence: n["events"] + 1, Bytes: payload}); err != nil {
				return err
			}
			n["events"]++
			st.lastRead = at
			states[key] = st
		}
		return s.Err()
	})
	if err != nil {
		return m, err
	}
	n["copy_distinct_ids"] = int64(len(copies))
	n["copy_distinct_read_afterwards"] = int64(len(copyRead))
	if n["events"] == 0 {
		return m, fmt.Errorf("no selected usable GET requests")
	}
	if err = w.Flush(); err != nil {
		return m, err
	}
	if err = f.Close(); err != nil {
		return m, err
	}
	if err = writeCatalog(dir, versions); err != nil {
		return m, err
	}
	m.SourceURLs = []string{COSSource, "https://www.usenix.org/conference/hotstorage20/presentation/eytan"}
	if c.SourceURL != "" {
		m.SourceURLs = append(m.SourceURLs, c.SourceURL)
	}
	m.License = map[string]string{"data": "SNIA Trace Data Files Download License; accepted by input provider", "url": "https://iotta.snia.org/repository/download_license", "attribution": "IBM Object Store; Ohad Eytan et al., It's Time to Revisit LRU vs. FIFO, HotStorage 2020"}
	m.Sampling = sampling(c.Config)
	m.Sampling["absolute_origin"] = c.Origin.Format(time.RFC3339Nano)
	m.TimeRange = model.Interval{Start: first, End: last}
	m.TimeRangeMeaning = "min/max valid input timestamps, inclusive; relative zero is explicit origin; not a completeness watermark"
	m.FieldMapping = map[string]string{"OccurredAt": "origin + relative_ms milliseconds", "ObjectKey": "mapKey(object_id); SampleKey uses original id", "Bucket": "CLI bucket: one trace file per dataset", "VersionID": "SHA256(original id NUL generation); each PUT creates next generation", "CreatedAt": "explicit origin for initial HEAD/GET; PUT time for new versions; observation time for reappearance after DELETE", "DeletedAt": "next PUT or DELETE time (exclusive)", "Bytes": "object size for full GET; inclusive range length for ranged GET; end capped to size-1; zero-size 0..-1 means empty payload; one event per GET", "Operation/Origin/Result": "GET → read/client/success; inferred success explicitly opted in", "Sequence": "contiguous emitted read counter"}
	m.RowPolicy = map[string]string{"HEAD": "excluded from reads, counted; known size may initialize catalogue", "COPY": "excluded entirely: identifier role ambiguous; global pre-sampling evidence counters track prior observation and any later GET per COPY row", "PUT": "new version, including same-size overwrite; closes previous current version", "DELETE": "closes current version; unknown delete counted; no read event", "size_conflicts": "retain current version size; skip conflicting GET; HEAD never changes active size", "timestamps": "regression fatal; equal timestamps accepted; mutation sharing time with prior read of its version fatal (unrepresentable half-open lifetime)", "invalid": "bad timestamp/key/size/range skipped and counted; malformed structure, multiple members, decoding or I/O failure fatal", "counts_scope": "operation/COPY evidence before sampling; catalogue transitions and size conflicts after sampling"}
	m.Limitations = []string{"HTTP status absent: GET success is inferred, explicitly opted in", "relative time has an arbitrary explicit origin: hour-of-day / weekday features are not real calendar time", "objects initially seen by HEAD/GET inferred to exist at relative zero; no actual creation time", "COPY excluded because the ID may be source or destination; missing effects can leave stale catalogue state", "HEAD is not demand; no event emitted for PUT/DELETE/COPY", "GET after deletion without PUT starts an inferred version at observation time; conflicting GET sizes skipped rather than inventing an overwrite", "selected key counts are not upscaled; observed bounds do not prove complete production demand"}
	return finishManifest(m, c.Config, dir, inputHash)
}
