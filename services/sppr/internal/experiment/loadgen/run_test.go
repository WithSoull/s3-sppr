package loadgen

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/testhttp"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/sim"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/synthetic"
	"github.com/stretchr/testify/require"
)

func config() Config {
	return Config{Generator: sim.DefaultConfig().Generator, Schedule: "synthetic", DurationSeconds: .1, TimeCompression: 1, Slots: 8, RequestTimeoutSeconds: 2}
}
func server(t *testing.T, delay time.Duration) (*httptest.Server, *atomic.Int64, *atomic.Int64) {
	t.Helper()
	active, peak := new(atomic.Int64), new(atomic.Int64)
	s := testhttp.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := active.Add(1)
		defer active.Add(-1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		time.Sleep(delay)
		w.Header().Set("X-Storage-Class", "HOT")
		_, _ = w.Write(Payload(filepath.Base(r.URL.Path), 32))
	}))
	t.Cleanup(s.Close)
	return s, active, peak
}
func TestScheduleMatchesSynthetic(t *testing.T) {
	c := config()
	c.DurationSeconds = 2
	c.Generator.ReadsPerSecond = 20
	a, err := Schedule(c)
	require.NoError(t, err)
	g, err := synthetic.New(c.Generator)
	require.NoError(t, err)
	for i, r := range a {
		e, _ := g.Next()
		require.Equal(t, e.ObjectKey, r.Key)
		require.Equal(t, time.Duration(i)*50*time.Millisecond, r.At)
	}
	c.TimeCompression = 10
	b, err := Schedule(c)
	require.NoError(t, err)
	require.Len(t, b, len(a))
	for i := range a {
		require.Equal(t, a[i].Key, b[i].Key)
		require.Equal(t, a[i].At/10, b[i].At)
	}
	c.Generator.Seed++
	b, err = Schedule(c)
	require.NoError(t, err)
	require.NotEqual(t, ScheduleHash(a), ScheduleHash(b))
}
func TestOpenLoopQueueDoesNotReduceLoad(t *testing.T) {
	c := config()
	c.Generator.SizeBytes = 32
	c.Generator.ReadsPerSecond = 400
	c.DurationSeconds = .1
	var results []Result
	for _, delay := range []time.Duration{0, 30 * time.Millisecond} {
		s, _, peak := server(t, delay)
		raw := filepath.Join(t.TempDir(), "raw.gz")
		r, err := Run(context.Background(), s.Client(), s.URL, "bucket", c, raw)
		require.NoError(t, err)
		require.EqualValues(t, 40, r.Offered)
		require.Equal(t, r.Offered, r.Completed)
		require.LessOrEqual(t, peak.Load(), int64(8))
		require.Equal(t, 1.0, *r.HotShare)
		results = append(results, r)
		f, err := os.Open(raw)
		require.NoError(t, err)
		gz, err := gzip.NewReader(f)
		require.NoError(t, err)
		d := json.NewDecoder(gz)
		var samples []Sample
		for d.More() {
			var x Sample
			require.NoError(t, d.Decode(&x))
			samples = append(samples, x)
		}
		require.NoError(t, gz.Close())
		require.NoError(t, f.Close())
		require.Len(t, samples, 40)
		for i, s := range samples {
			require.Equal(t, int64(time.Duration(i)*2500*time.Microsecond), s.ScheduledNs)
			require.Equal(t, s.FinishedNs-s.ScheduledNs, s.LatencyNs)
		}
		if delay > 0 {
			require.Greater(t, samples[39].StartedNs-samples[39].ScheduledNs, int64(15*time.Millisecond))
			require.Greater(t, r.Finished.Sub(r.Started), 100*time.Millisecond)
		}
	}
	require.Equal(t, results[0].ScheduleSHA, results[1].ScheduleSHA)
	require.Greater(t, results[1].Latency.MeanMs, results[0].Latency.MeanMs)
}
func TestLatencyThroughLastByte(t *testing.T) {
	c := config()
	c.Generator.SizeBytes = 32
	c.DurationSeconds = .01
	s := testhttp.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := Payload(filepath.Base(r.URL.Path), 32)
		_, _ = w.Write(p[:16])
		w.(http.Flusher).Flush()
		time.Sleep(30 * time.Millisecond)
		_, _ = w.Write(p[16:])
	}))
	defer s.Close()
	r, err := Run(context.Background(), s.Client(), s.URL, "bucket", c, "")
	require.NoError(t, err)
	require.EqualValues(t, 1, r.Completed)
	require.GreaterOrEqual(t, r.Latency.MeanMs, 30.0)
	require.Nil(t, r.HotShare)
}
func TestErrorsAreAccounted(t *testing.T) {
	for _, tt := range []struct {
		name   string
		status int
		body   string
		delay  time.Duration
		want   string
	}{{"status", 503, "", 0, "http_503"}, {"short", 200, "short", 0, "size"}, {"corrupt", 200, "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", 0, "integrity"}, {"timeout", 200, "", 40 * time.Millisecond, "timeout"}} {
		t.Run(tt.name, func(t *testing.T) {
			c := config()
			c.Generator.SizeBytes = 32
			c.RequestTimeoutSeconds = .01
			c.DurationSeconds = .01
			s := testhttp.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				time.Sleep(tt.delay)
				w.WriteHeader(tt.status)
				fmt.Fprint(w, tt.body)
			}))
			defer s.Close()
			r, err := Run(context.Background(), s.Client(), s.URL, "b", c, "")
			require.NoError(t, err)
			require.EqualValues(t, 1, r.Offered)
			require.Zero(t, r.Completed)
			require.Equal(t, map[string]int64{tt.want: 1}, r.Errors)
			require.EqualValues(t, 1, r.Latency.Errors)
			require.Equal(t, r.Errors, r.Minutes[0].Errors)
		})
	}
}
func TestCancellationAccountsWholeSchedule(t *testing.T) {
	c := config()
	c.DurationSeconds = 1
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err := Run(ctx, http.DefaultClient, "http://127.0.0.1:1", "b", c, "")
	require.ErrorIs(t, err, context.Canceled)
	require.EqualValues(t, 20, r.Offered)
	require.EqualValues(t, 20, r.Errors["cancelled"])
	require.Zero(t, r.Completed)
}
func TestNearestRankAndWarmup(t *testing.T) {
	l := summarize([]Sample{{LatencyNs: 1e6}, {LatencyNs: 2e6}, {LatencyNs: 3e6}, {LatencyNs: 4e6}, {Error: "timeout"}})
	require.Equal(t, Latency{Reads: 4, Errors: 1, MeanMs: 2.5, P50Ms: 2, P95Ms: 4, P99Ms: 4}, l)
	c := config()
	c.Generator.SizeBytes = 32
	c.WarmupSeconds = .05
	c.DurationSeconds = .05
	s, _, _ := server(t, 0)
	r, err := Run(context.Background(), s.Client(), s.URL, "b", c, "")
	require.NoError(t, err)
	require.EqualValues(t, 1, r.WarmupOffered)
	require.EqualValues(t, 1, r.Offered)
}
func TestSeedThroughGateway(t *testing.T) {
	c := config()
	c.Generator.Objects = 10
	c.Generator.SizeBytes = 32
	var puts int
	s := testhttp.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "PUT", r.Method)
		require.Equal(t, int64(32), r.ContentLength)
		puts++
	}))
	defer s.Close()
	require.NoError(t, Seed(context.Background(), s.Client(), s.URL, "b", c))
	require.Equal(t, 10, puts)
}
