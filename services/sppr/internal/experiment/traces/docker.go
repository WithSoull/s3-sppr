package traces

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

const DockerSource = "https://dssl.cs.vt.edu/drtp/"

type DockerConfig struct {
	Config
	Zone     string
	From, To string
}
type dockerRecord struct {
	Method    string `json:"http.request.method"`
	URI       string `json:"http.request.uri"`
	Status    int    `json:"http.response.status"`
	Written   *int64 `json:"http.response.written"`
	Timestamp string `json:"timestamp"`
}
type dockerObject struct {
	created       time.Time
	size          int64
	full, maxRows int64
}
type digestAudit struct {
	repoSizes map[string]int64
	repo      string
	multi     bool
	min, max  int64
	hasSize   bool
}
type dockerRead struct {
	Key    string
	Bytes  int64
	Status int
}

func dockerURI(uri string) (category, repo, digest string) {
	uri, _, _ = strings.Cut(uri, "?")
	parts := strings.Split(strings.TrimPrefix(uri, "/"), "/")
	if len(parts) > 0 && parts[len(parts)-1] == "" {
		parts = parts[:len(parts)-1]
	}
	if len(parts) == 1 && parts[0] == "v2" {
		return "ping", "", ""
	}
	if len(parts) < 4 || parts[0] != "v2" {
		return "other", "", ""
	}
	for i := 2; i < len(parts); i++ {
		if parts[i] == "manifests" {
			return "manifests", "", ""
		}
		if parts[i] == "blobs" {
			if i+1 < len(parts) && parts[i+1] == "uploads" {
				return "uploads", "", ""
			}
			if i+2 != len(parts) {
				return "other", "", ""
			}
			for _, s := range parts[1:] {
				if !validKey(s) || s == "." || s == ".." || strings.ContainsAny(s, "\t\r\n ") {
					return "other", "", ""
				}
			}
			return "blobs", strings.Join(parts[1:i], "/"), parts[i+1]
		}
	}
	return "other", "", ""
}
func validDay(day string) bool { _, err := time.Parse("2006-01-02", day); return err == nil }
func dockerDay(name, zone string) (string, bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 3 || parts[0] != "data_centers" || parts[1] != zone {
		return "", false
	}
	rest, ok := strings.CutPrefix(parts[2], zone+"-logstash-")
	if !ok {
		rest, ok = strings.CutPrefix(parts[2], "prod-"+zone+"-logstash-")
	}
	if !ok || len(rest) < 17 || !strings.HasSuffix(rest, ".json") {
		return "", false
	}
	day := strings.ReplaceAll(rest[:10], ".", "-")
	suffix := strings.TrimSuffix(rest[11:], ".json")
	_, err := strconv.ParseUint(suffix, 10, 32)
	return day, validDay(day) && rest[10] == '-' && err == nil
}

func ConvertDocker(ctx context.Context, c DockerConfig, dir string) (Manifest, error) {
	m := Manifest{ConverterVersion: "ibm-docker-v1", Counts: map[string]int64{}}
	n := m.Counts
	for _, k := range []string{"input_rows", "files_selected", "files_outside_filter", "events", "versions", "sampled_out", "head_excluded", "manifests_excluded", "uploads_excluded", "ping_excluded", "other_excluded", "method_excluded", "non_2xx", "status_excluded", "invalid_timestamp", "invalid_written", "unknown_full_size_reads", "unknown_full_size_objects", "size_conflict_objects", "size_conflict_rows", "digests", "digests_multiple_repos", "digests_multiple_full_get_repos", "digests_cross_repo_max_size_conflicts", "full_get_blob_uris", "digests_full_size_conflicts", "distinct_blob_uris", "out_of_order_timestamps"} {
		n[k] = 0
	}
	if err := validateTrace(c.Config); err != nil {
		return m, err
	}
	if !validKey(c.Zone) || path.Base(c.Zone) != c.Zone || strings.ContainsAny(c.Zone, "\\ \t\n\r") || c.Zone == "." || c.Zone == ".." {
		return m, fmt.Errorf("valid -zone required")
	}
	if (c.From != "" && !validDay(c.From)) || (c.To != "" && !validDay(c.To)) || (c.From != "" && c.To != "" && c.From >= c.To) {
		return m, fmt.Errorf("-from/-to must be YYYY-MM-DD, from < to (exclusive)")
	}
	spool, err := os.CreateTemp(dir, ".docker-spool-")
	if err != nil {
		return m, err
	}
	defer os.Remove(spool.Name())
	defer spool.Close()
	w := bufio.NewWriterSize(spool, 256*1024)
	objects := map[string]*dockerObject{}
	digests := map[string]*digestAudit{}
	uris := map[string]bool{}
	var first, last, previous time.Time
	inputHash, err := scanArchive(ctx, c.Input, false, func(name string, r io.Reader) error {
		day, ok := dockerDay(name, c.Zone)
		if !ok {
			return nil
		}
		if (c.From != "" && day < c.From) || (c.To != "" && day >= c.To) {
			n["files_outside_filter"]++
			return nil
		}
		n["files_selected"]++
		d := json.NewDecoder(bufio.NewReaderSize(r, 256*1024))
		token, err := d.Token()
		if err != nil || token != json.Delim('[') {
			return fmt.Errorf("expected JSON array: %v", err)
		}
		for d.More() {
			if err := ctx.Err(); err != nil {
				return err
			}
			var row dockerRecord
			if err := d.Decode(&row); err != nil {
				return err
			}
			n["input_rows"]++
			at, err := time.Parse(time.RFC3339Nano, row.Timestamp)
			if err != nil || at.IsZero() || at.Year() < 1 {
				n["invalid_timestamp"]++
				continue
			}
			at = at.UTC()
			if !previous.IsZero() && at.Before(previous) {
				n["out_of_order_timestamps"]++
			}
			previous = at
			if first.IsZero() || at.Before(first) {
				first = at
			}
			if last.IsZero() || at.After(last) {
				last = at
			}
			category, repo, digest := dockerURI(row.URI)
			n["route_"+category]++
			if category == "blobs" {
				n["blob_"+strings.ToLower(row.Method)+"_"+strconv.Itoa(row.Status)]++
			}
			n["method_"+row.Method]++
			n["status_"+strconv.Itoa(row.Status)]++
			if row.Status < 200 || row.Status >= 300 {
				n["non_2xx"]++
			}
			if category != "blobs" {
				n[category+"_excluded"]++
				continue
			}
			uris[repo+"/"+digest] = true
			a := digests[digest]
			if a == nil {
				a = &digestAudit{repo: repo, repoSizes: map[string]int64{}}
				digests[digest] = a
			} else if a.repo != repo {
				a.multi = true
			}
			if row.Method == "GET" && row.Status == 200 && row.Written != nil && *row.Written >= 0 {
				if old, ok := a.repoSizes[repo]; !ok || *row.Written > old {
					a.repoSizes[repo] = *row.Written
				}
				if !a.hasSize {
					a.min = *row.Written
					a.max = *row.Written
					a.hasSize = true
				} else {
					a.min = min(a.min, *row.Written)
					a.max = max(a.max, *row.Written)
				}
			}
			if row.Status < 200 || row.Status >= 300 {
				n["status_excluded"]++
				continue
			}
			// Blob identity is the anonymized digest within the selected zone. Upload
			// paths do not identify digests; HEAD may establish existence, never size.
			key := digest
			if !SampleKey(key, c.SampleSeed, c.KeyShare) {
				n["sampled_out"]++
				continue
			}
			o := objects[key]
			if o == nil {
				o = &dockerObject{created: at, size: -1}
				objects[key] = o
			} else if at.Before(o.created) {
				o.created = at
			}
			if row.Method == "HEAD" {
				n["head_excluded"]++
				continue
			}
			if row.Method != "GET" {
				n["method_excluded"]++
				continue
			}
			if row.Status != 200 && row.Status != 206 {
				n["status_excluded"]++
				continue
			}
			if row.Written == nil || *row.Written < 0 {
				n["invalid_written"]++
				continue
			}
			if row.Status == 200 {
				o.full++
				if *row.Written > o.size {
					o.size = *row.Written
					o.maxRows = 1
				} else if *row.Written == o.size {
					o.maxRows++
				}
			}
			b, err := json.Marshal(dockerRead{Key: key, Bytes: *row.Written, Status: row.Status})
			if err != nil {
				return err
			}
			if _, err = fmt.Fprintf(w, "%s\t%020d\t%s\n", at.Format("2006-01-02T15:04:05.000000000Z"), n["input_rows"], b); err != nil {
				return err
			}
		}
		if _, err = d.Token(); err != nil {
			return err
		}
		if err = d.Decode(new(any)); err != io.EOF {
			return fmt.Errorf("trailing JSON content: %v", err)
		}
		return nil
	})
	if err != nil {
		return m, err
	}
	if err = w.Flush(); err != nil {
		return m, err
	}
	if err = spool.Close(); err != nil {
		return m, err
	}
	n["digests"] = int64(len(digests))
	n["distinct_blob_uris"] = int64(len(uris))
	for _, a := range digests {
		n["full_get_blob_uris"] += int64(len(a.repoSizes))
		if len(a.repoSizes) > 1 {
			n["digests_multiple_full_get_repos"]++
			var firstSize int64
			have := false
			conflict := false
			for _, size := range a.repoSizes {
				if have && firstSize != size {
					conflict = true
				}
				firstSize = size
				have = true
			}
			if conflict {
				n["digests_cross_repo_max_size_conflicts"]++
			}
		}
		if a.multi {
			n["digests_multiple_repos"]++
		}
		if a.hasSize && a.min != a.max {
			n["digests_full_size_conflicts"]++
		}
	}
	var versions []model.Version
	for key, o := range objects {
		if o.size < 0 {
			n["unknown_full_size_objects"]++
			continue
		}
		if o.full != o.maxRows {
			n["size_conflict_objects"]++
			n["size_conflict_rows"] += o.full - o.maxRows
		}
		versions = append(versions, model.Version{VersionRef: model.VersionRef{Bucket: c.Bucket, ObjectKey: mapKey(key), VersionID: versionID(key, 1)}, SizeBytes: o.size, CreatedAt: o.created})
	}
	n["versions"] = int64(len(versions))
	if err = writeCatalog(dir, versions); err != nil {
		return m, err
	}
	f, err := os.OpenFile(filepath.Join(dir, "events.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return m, err
	}
	defer f.Close()
	out := bufio.NewWriterSize(f, 256*1024)
	enc := json.NewEncoder(out)
	// Bounded external sort: tar member order and per-file JSON order are not
	// chronological. No request collection or complete member is held in memory.
	sortCmd := exec.CommandContext(ctx, "/usr/bin/sort", "-S", "32M", "-T", dir, "-k1,1", "-k2,2", spool.Name())
	sortCmd.Env = append(os.Environ(), "LC_ALL=C")
	pipe, err := sortCmd.StdoutPipe()
	if err != nil {
		return m, err
	}
	if err = sortCmd.Start(); err != nil {
		return m, err
	}
	defer func() {
		_ = pipe.Close()
		if sortCmd.ProcessState == nil {
			_ = sortCmd.Process.Kill()
			_ = sortCmd.Wait()
		}
	}()
	s := bufio.NewScanner(pipe)
	s.Buffer(make([]byte, 4096), 128*1024)
	for s.Scan() {
		if err := ctx.Err(); err != nil {
			return m, err
		}
		parts := strings.SplitN(s.Text(), "\t", 3)
		if len(parts) != 3 {
			return m, fmt.Errorf("invalid internal sort row")
		}
		at, err := time.Parse(time.RFC3339Nano, parts[0])
		if err != nil {
			return m, err
		}
		var row dockerRead
		if err = json.Unmarshal([]byte(parts[2]), &row); err != nil {
			return m, err
		}
		o := objects[row.Key]
		if o.size < 0 {
			n["unknown_full_size_reads"]++
			continue
		}
		if row.Bytes > o.size {
			n["invalid_written"]++
			continue
		}
		v := model.Version{VersionRef: model.VersionRef{ObjectKey: mapKey(row.Key), VersionID: versionID(row.Key, 1)}}
		if err = emitRead(enc, c.Config, "docker", v, model.AccessEvent{OccurredAt: at, Sequence: n["events"] + 1, Bytes: row.Bytes}); err != nil {
			return m, err
		}
		n["events"]++
	}
	if err = s.Err(); err != nil {
		return m, err
	}
	if err = sortCmd.Wait(); err != nil {
		return m, fmt.Errorf("external sort: %w", err)
	}
	if n["events"] == 0 {
		return m, fmt.Errorf("no selected blob GET 200/206 with known full size")
	}
	if err = out.Flush(); err != nil {
		return m, err
	}
	if err = f.Close(); err != nil {
		return m, err
	}
	m.SourceURLs = []string{DockerSource, "https://drive.google.com/file/d/1FjgyCJ8YayOKeMlxtbznhsiXGPYC9yP8/view", "https://www.usenix.org/conference/fast18/presentation/anwar"}
	if c.SourceURL != "" {
		m.SourceURLs = append(m.SourceURLs, c.SourceURL)
	}
	m.License = map[string]string{"data": "not stated; used with the authors' permission", "attribution": "Anwar et al., Improving Docker Registry Design based on Production Workload Analysis, FAST 2018; IBM Docker Registry / DRTP"}
	m.Sampling = sampling(c.Config)
	m.Sampling["zone"] = c.Zone
	m.Sampling["from_day_inclusive"] = c.From
	m.Sampling["to_day_exclusive"] = c.To
	m.TimeRange = model.Interval{Start: first, End: last}
	m.TimeRangeMeaning = "min/max valid timestamps in selected files, inclusive; day filters select filename dates, not a completeness assertion"
	m.FieldMapping = map[string]string{"OccurredAt": "timestamp RFC3339 UTC, sorted on disk with input row ordinal tie-break", "ObjectKey": "mapKey(anonymized blob digest), deduplicated across repositories within zone", "Bucket": "CLI bucket, default docker-<zone>; one zone per dataset", "VersionID": "SHA256(original digest NUL 1); immutable blob", "CreatedAt": "earliest successful blob observation within selected files (HEAD included); no upload inference", "SizeBytes": "maximum http.response.written among GET blobs status 200, independent of row order", "Bytes": "http.response.written for each GET blobs status 200/206; reject values above full size", "Operation/Origin/Result": "GET blobs HTTP 200/206 → read/client/success; HTTP status is present", "Sequence": "contiguous emitted counter after external sort"}
	m.RowPolicy = map[string]string{"excluded": "first-match: non-blob routes (manifests/uploads/ping/other), non-2xx blobs, sampling, HEAD, non-GET, other statuses, invalid written; missing full size excluded after scan", "counts_scope": "route/method/status and digest audit before sampling; version/size conflict/read exclusion counters after sampling; non_2xx overlaps route exclusions", "size_conflicts": "immutable version uses maximum full 200 response; conflict_rows counts full observations below final maximum; range never establishes size", "structure": "malformed/trailing JSON, wrong array shape, truncated compression/tar, I/O/sort failure fatal; bad timestamp/written counted; member paths never extracted", "ordering": "external sort by UTC timestamp then archive input ordinal; timestamp equality is not request deduplication"}
	m.Limitations = []string{"data license not stated; used with the authors' permission; replayer license does not license trace data", "blobs immutable, one version per digest; cross-repository digest identity assumes anonymization preserves equality", "creation inferred at first successful observation within processed files, not actual upload: upload URIs carry no digest", "size inferred retrospectively from full GET 200; maximum resolves conflicting observations deterministically and may overestimate true size; this is retrospective metadata, not a historical size feed", "HEAD/manifests/uploads/pings/non-2xx are excluded from demand; 206-only objects without full 200 size are excluded", "filename day filters do not clip timestamp outliers; explicit dataset coverage bounds remain caller responsibility", "sorting needs disk proportional to selected reads; catalogue/digest audit memory proportional to distinct identities, not requests", "selected key counts not upscaled; zero labels mean no observed selected successful blob GET, not proven absence of production demand"}
	return finishManifest(m, c.Config, dir, inputHash)
}
