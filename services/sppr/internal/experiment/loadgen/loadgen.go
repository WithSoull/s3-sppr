// Package loadgen replays a placement-independent arrival schedule over HTTP.
package loadgen

import (
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/synthetic"
)

const Version = "stand-loadgen-v1"

type Config struct {
	Generator             synthetic.Config `json:"generator"`
	Schedule              string           `json:"schedule"` // synthetic or constant (uniform keys, seed still applies)
	DurationSeconds       float64          `json:"duration_seconds"`
	WarmupSeconds         float64          `json:"warmup_seconds"`
	TimeCompression       float64          `json:"time_compression"`
	Slots                 int              `json:"slots"`
	RequestTimeoutSeconds float64          `json:"request_timeout_seconds"`
	// Explicit shifts override only group boundaries, retaining synthetic-v1 RNG semantics.
	ShiftAtSeconds []float64 `json:"shift_at_seconds,omitempty"`
}

func (c Config) Validate() error {
	if err := c.Generator.Validate(); err != nil {
		return err
	}
	for _, x := range []float64{c.DurationSeconds, c.WarmupSeconds, c.TimeCompression, c.RequestTimeoutSeconds} {
		if math.IsNaN(x) || math.IsInf(x, 0) {
			return fmt.Errorf("non-finite timing")
		}
	}
	if c.Generator.Lifecycle || c.DurationSeconds <= 0 || c.WarmupSeconds < 0 || c.DurationSeconds+c.WarmupSeconds > 7*86400 || c.TimeCompression <= 0 || c.Slots < 1 || c.Slots > 8 || c.RequestTimeoutSeconds <= 0 || (c.Schedule != "synthetic" && c.Schedule != "constant") {
		return fmt.Errorf("invalid load config (fixed catalogue, <=7 days, 1..8 slots required)")
	}
	last := 0.0
	for _, at := range c.ShiftAtSeconds {
		if math.IsNaN(at) || math.IsInf(at, 0) || at <= last || at >= c.DurationSeconds+c.WarmupSeconds {
			return fmt.Errorf("invalid shift boundaries")
		}
		last = at
	}
	if len(c.ShiftAtSeconds) > 0 && (c.Generator.Scenario != "shift" || c.Schedule != "synthetic") {
		return fmt.Errorf("explicit shifts require synthetic shift scenario")
	}
	return nil
}

type Arrival struct {
	At  time.Duration `json:"at_ns"`
	Key string        `json:"key"`
}

func Schedule(c Config) ([]Arrival, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	gc := c.Generator
	if c.Schedule == "constant" {
		gc.Scenario = "uniform"
	}
	g, err := synthetic.NewWithShifts(gc, c.ShiftAtSeconds)
	if err != nil {
		return nil, err
	}
	end := time.Duration((c.DurationSeconds + c.WarmupSeconds) * float64(time.Second))
	var out []Arrival
	for {
		e, ok := g.Next()
		if !ok {
			break
		}
		at := e.OccurredAt.Sub(gc.Start)
		if at >= end {
			break
		}
		out = append(out, Arrival{time.Duration(float64(at) / c.TimeCompression), e.ObjectKey})
	}
	return out, nil
}
func ScheduleHash(a []Arrival) string {
	h := sha256.New()
	_ = json.NewEncoder(h).Encode(a)
	return hex.EncodeToString(h.Sum(nil))
}
func target(base, bucket, key string) string {
	return strings.TrimRight(base, "/") + "/objects/" + url.PathEscape(bucket) + "/" + url.PathEscape(key)
}

// Payload is deterministic across policies and contains object-specific bytes.
func Payload(key string, size int64) []byte {
	p := make([]byte, size)
	h := sha256.Sum256([]byte(key))
	for i := range p {
		p[i] = h[i%len(h)]
	}
	return p
}
func Seed(ctx context.Context, client *http.Client, base, bucket string, c Config) error {
	g, err := synthetic.New(c.Generator)
	if err != nil {
		return err
	}
	for _, v := range g.Catalog() {
		req, err := http.NewRequestWithContext(ctx, "PUT", target(base, bucket, v.ObjectKey), bytes.NewReader(Payload(v.ObjectKey, v.SizeBytes)))
		if err != nil {
			return err
		}
		res, err := client.Do(req)
		if err != nil {
			return err
		}
		_, re := io.Copy(io.Discard, res.Body)
		ce := res.Body.Close()
		if res.StatusCode < 200 || res.StatusCode >= 300 {
			return fmt.Errorf("seed %s: HTTP %d", v.ObjectKey, res.StatusCode)
		}
		if err = errors.Join(re, ce); err != nil {
			return err
		}
	}
	return nil
}

type Sample struct {
	Index       int    `json:"index"`
	ScheduledNs int64  `json:"scheduled_ns"`
	StartedNs   int64  `json:"started_ns"`
	FinishedNs  int64  `json:"finished_ns"`
	LatencyNs   int64  `json:"latency_ns"`
	Bytes       int64  `json:"bytes"`
	Error       string `json:"error,omitempty"`
	Class       string `json:"class,omitempty"`
	Warmup      bool   `json:"warmup"`
}
type Latency struct {
	Reads  int64   `json:"reads"`
	Errors int64   `json:"errors"`
	MeanMs float64 `json:"mean_ms"`
	P50Ms  float64 `json:"p50_ms"`
	P95Ms  float64 `json:"p95_ms"`
	P99Ms  float64 `json:"p99_ms"`
}
type Minute struct {
	Minute    int              `json:"minute"`
	Offered   int64            `json:"offered"`
	Completed int64            `json:"completed"`
	Errors    map[string]int64 `json:"errors_by_kind"`
	Latency   Latency          `json:"latency"`
}
type Result struct {
	Version       string           `json:"version"`
	Config        Config           `json:"load_config"`
	ScheduleSHA   string           `json:"schedule_sha256"`
	Started       time.Time        `json:"started"`
	Finished      time.Time        `json:"finished"`
	Offered       int64            `json:"offered"`
	Completed     int64            `json:"completed"`
	Errors        map[string]int64 `json:"errors_by_kind"`
	Latency       Latency          `json:"latency"`
	HotShare      *float64         `json:"hot_read_share"`
	ClassKnown    int64            `json:"class_known_reads"`
	WarmupOffered int64            `json:"warmup_offered"`
	WarmupErrors  map[string]int64 `json:"warmup_errors_by_kind"`
	Minutes       []Minute         `json:"minutes"`
}

func summarize(samples []Sample) Latency {
	var ns []int64
	var total float64
	var l Latency
	for _, s := range samples {
		if s.Error != "" {
			l.Errors++
			continue
		}
		ns = append(ns, s.LatencyNs)
		total += float64(s.LatencyNs)
	}
	l.Reads = int64(len(ns))
	if len(ns) == 0 {
		return l
	}
	sort.Slice(ns, func(i, j int) bool { return ns[i] < ns[j] })
	rank := func(p float64) float64 { return float64(ns[int(math.Ceil(p*float64(len(ns))))-1]) / 1e6 }
	l.MeanMs = total / float64(len(ns)) / 1e6
	l.P50Ms = rank(.5)
	l.P95Ms = rank(.95)
	l.P99Ms = rank(.99)
	return l
}
func errorKind(err error) string {
	if errors.Is(err, context.Canceled) {
		return "cancelled"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "timeout"
	}
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return "timeout"
	}
	return "transport"
}
func waitUntil(ctx context.Context, t time.Time) error {
	d := time.Until(t)
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

// Run buffers the entire finite arrival queue before serving. The producer never
// waits for workers, and workers drain even after the arrival window has ended.
// A cancellation classifies every remaining arrival instead of losing demand.
func Run(ctx context.Context, client *http.Client, base, bucket string, c Config, rawPath string) (Result, error) {
	arrivals, err := Schedule(c)
	if err != nil {
		return Result{}, err
	}
	r := Result{Version: Version, Config: c, ScheduleSHA: ScheduleHash(arrivals), Errors: map[string]int64{}, WarmupErrors: map[string]int64{}}
	expected := map[string][32]byte{}
	for _, a := range arrivals {
		if _, ok := expected[a.Key]; !ok {
			expected[a.Key] = sha256.Sum256(Payload(a.Key, c.Generator.SizeBytes))
		}
	}
	samples := make([]Sample, len(arrivals))
	jobs := make(chan int, len(arrivals))
	started := time.Now()
	r.Started = started.UTC()
	var wg sync.WaitGroup
	for w := 0; w < c.Slots; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				a := arrivals[i]
				s := Sample{Index: i, ScheduledNs: int64(a.At), Warmup: float64(a.At) < c.WarmupSeconds*1e9/c.TimeCompression}
				s.StartedNs = time.Since(started).Nanoseconds()
				if ctx.Err() != nil {
					s.Error = errorKind(ctx.Err())
				} else {
					reqCtx, cancel := context.WithTimeout(ctx, time.Duration(c.RequestTimeoutSeconds*1e9))
					req, re := http.NewRequestWithContext(reqCtx, "GET", target(base, bucket, a.Key), nil)
					if re == nil {
						var res *http.Response
						res, re = client.Do(req)
						if re == nil {
							h := sha256.New()
							reader := &lastByteReader{Reader: res.Body}
							s.Bytes, re = io.Copy(h, reader)
							if !reader.last.IsZero() {
								s.FinishedNs = reader.last.Sub(started).Nanoseconds()
							} else {
								s.FinishedNs = time.Since(started).Nanoseconds()
							}
							ce := res.Body.Close()
							if re == nil {
								re = ce
							}
							s.Class = strings.ToUpper(res.Header.Get("X-Storage-Class"))
							switch {
							case res.StatusCode != 200:
								s.Error = fmt.Sprintf("http_%d", res.StatusCode)
							case re != nil:
								s.Error = errorKind(re)
							case s.Bytes != c.Generator.SizeBytes:
								s.Error = "size"
							case !bytes.Equal(h.Sum(nil), hashBytes(expected[a.Key])):
								s.Error = "integrity"
							}
						}
					}
					if re != nil && s.Error == "" {
						s.Error = errorKind(re)
					}
					cancel()
				}
				if s.FinishedNs == 0 {
					s.FinishedNs = time.Since(started).Nanoseconds()
				}
				s.LatencyNs = s.FinishedNs - s.ScheduledNs
				samples[i] = s
			}
		}()
	}
	for i, a := range arrivals {
		_ = waitUntil(ctx, started.Add(a.At))
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	r.Finished = time.Now().UTC()
	var measured []Sample
	minutes := map[int][]Sample{}
	var hot int64
	for _, s := range samples {
		if s.Warmup {
			r.WarmupOffered++
			if s.Error != "" {
				r.WarmupErrors[s.Error]++
			}
			continue
		}
		r.Offered++
		measured = append(measured, s)
		minute := int((float64(s.ScheduledNs)*c.TimeCompression/1e9 - c.WarmupSeconds) / 60)
		minutes[minute] = append(minutes[minute], s)
		if s.Error != "" {
			r.Errors[s.Error]++
		} else {
			r.Completed++
			if s.Class == "HOT" || s.Class == "COLD" {
				r.ClassKnown++
				if s.Class == "HOT" {
					hot++
				}
			}
		}
	}
	r.Latency = summarize(measured)
	if r.Completed > 0 && r.ClassKnown == r.Completed {
		v := float64(hot) / float64(r.Completed)
		r.HotShare = &v
	}
	for m := 0; m < int(math.Ceil(c.DurationSeconds/60)); m++ {
		ss := minutes[m]
		v := Minute{Minute: m, Offered: int64(len(ss)), Errors: map[string]int64{}, Latency: summarize(ss)}
		v.Completed = v.Latency.Reads
		for _, s := range ss {
			if s.Error != "" {
				v.Errors[s.Error]++
			}
		}
		r.Minutes = append(r.Minutes, v)
	}
	if rawPath != "" {
		f, e := os.OpenFile(rawPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0644)
		if e != nil {
			return r, e
		}
		gz := gzip.NewWriter(f)
		enc := json.NewEncoder(gz)
		for _, s := range samples {
			if err = enc.Encode(s); err != nil {
				break
			}
		}
		err = errors.Join(err, gz.Close(), f.Close())
		if err != nil {
			return r, err
		}
	}
	return r, ctx.Err()
}

func hashBytes(h [32]byte) []byte { return h[:] }

// Capture the arrival of the final payload byte before hashing that buffer or
// waiting for EOF/trailers; those steps verify success but do not extend latency.
type lastByteReader struct {
	io.Reader
	last time.Time
}

func (r *lastByteReader) Read(p []byte) (int, error) {
	n, err := r.Reader.Read(p)
	if n > 0 {
		r.last = time.Now()
	}
	return n, err
}
