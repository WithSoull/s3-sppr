package traces

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/dataset"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/stretchr/testify/require"
)

func cosConfig() COSConfig {
	return COSConfig{Config: Config{Input: "testdata/ibm-cos.txt", Bucket: "ibm-cos-001", KeyShare: 1, SampleSeed: "0", AllowInferredSuccess: true}, Origin: time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}
}
func readTrace(t *testing.T, dir string) ([]model.Version, []model.AccessEvent) {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "catalog.json"))
	require.NoError(t, err)
	var vs []model.Version
	require.NoError(t, json.Unmarshal(b, &vs))
	f, err := os.Open(filepath.Join(dir, "events.jsonl"))
	require.NoError(t, err)
	defer f.Close()
	var es []model.AccessEvent
	d := json.NewDecoder(f)
	for {
		var e model.AccessEvent
		err := d.Decode(&e)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		es = append(es, e)
	}
	return vs, es
}
func makeArchive(t *testing.T, members map[string]string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "fixture.tar.gz")
	f, err := os.Create(p)
	require.NoError(t, err)
	z := gzip.NewWriter(f)
	w := tar.NewWriter(z)
	for name, data := range members {
		require.NoError(t, w.WriteHeader(&tar.Header{Name: name, Mode: 0644, Size: int64(len(data))}))
		_, err = w.Write([]byte(data))
		require.NoError(t, err)
	}
	require.NoError(t, w.Close())
	require.NoError(t, z.Close())
	require.NoError(t, f.Close())
	return p
}
func TestCOSPolicies(t *testing.T) {
	raw, err := os.ReadFile("testdata/ibm-cos.txt")
	require.NoError(t, err)
	for _, tt := range []struct {
		name          string
		archive, crlf bool
	}{{"plain", false, false}, {"CRLF", false, true}, {"tar gzip", true, true}} {
		t.Run(tt.name, func(t *testing.T) {
			c := cosConfig()
			data := string(raw)
			if tt.crlf {
				data = strings.ReplaceAll(data, "\n", "\r\n")
			}
			if tt.archive {
				c.Input = makeArchive(t, map[string]string{"../../never-extracted": data})
			} else {
				c.Input = filepath.Join(t.TempDir(), "input")
				require.NoError(t, os.WriteFile(c.Input, []byte(data), 0644))
			}
			dir := t.TempDir()
			m, err := ConvertCOS(context.Background(), c, dir)
			require.NoError(t, err)
			for key, want := range map[string]int64{"events": 8, "versions": 7, "head_excluded": 2, "copy_excluded": 2, "copy_seen_before": 1, "copy_read_afterwards": 2, "overwrites": 2, "deletes": 2, "delete_without_current": 1, "range_end_clamped": 1, "invalid_range": 1, "invalid_size": 1, "size_conflicts": 1, "unknown_operation": 1} {
				require.Equal(t, want, m.Counts[key], key)
			}
			vs, es := readTrace(t, dir)
			require.Len(t, vs, 7)
			for i, e := range es {
				require.Equal(t, int64(i+1), e.Sequence)
				found := false
				for _, v := range vs {
					if v.VersionID == e.VersionID {
						require.True(t, v.Active(e.OccurredAt))
						require.LessOrEqual(t, e.Bytes, v.SizeBytes)
						found = true
					}
				}
				require.True(t, found)
			}
			require.Equal(t, int64(10), es[0].Bytes)
			require.Equal(t, int64(100), es[1].Bytes)
			require.NotEqual(t, es[2].VersionID, es[3].VersionID, "same-size PUT is overwrite")
			require.Equal(t, c.Origin, vs[0].CreatedAt)
			require.NotNil(t, vs[0].DeletedAt)
			sum, err := dataset.HashFile(c.Input)
			require.NoError(t, err)
			require.Equal(t, sum, m.InputSHA256[filepath.Base(c.Input)])
		})
	}
}
func TestCOSRejects(t *testing.T) {
	for _, tt := range []struct {
		name, raw string
		origin    time.Time
		opt       bool
	}{
		{"origin required", "0 REST.GET.OBJECT a 1\n", time.Time{}, true},
		{"opt in required", "0 REST.GET.OBJECT a 1\n", cosConfig().Origin, false},
		{"regression", "2 REST.GET.OBJECT a 1\n1 REST.GET.OBJECT a 1\n", cosConfig().Origin, true},
		{"malformed", "0 REST.GET.OBJECT\n", cosConfig().Origin, true},
		{"ambiguous lifetime", "0 REST.GET.OBJECT a 1\n0 REST.PUT.OBJECT a 2\n", cosConfig().Origin, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := cosConfig()
			c.Origin = tt.origin
			c.AllowInferredSuccess = tt.opt
			c.Input = filepath.Join(t.TempDir(), "input")
			require.NoError(t, os.WriteFile(c.Input, []byte(tt.raw), 0644))
			_, err := ConvertCOS(context.Background(), c, t.TempDir())
			require.Error(t, err)
		})
	}
}
func TestCOSSampling(t *testing.T) {
	c := cosConfig()
	c.KeyShare = 0.5
	a, b := t.TempDir(), t.TempDir()
	m, err := ConvertCOS(context.Background(), c, a)
	require.NoError(t, err)
	n, err := ConvertCOS(context.Background(), c, b)
	require.NoError(t, err)
	require.Equal(t, m, n)
	vs, es := readTrace(t, a)
	for _, v := range vs {
		require.True(t, SampleKey(v.ObjectKey, c.SampleSeed, c.KeyShare))
	}
	for _, e := range es {
		require.True(t, SampleKey(e.ObjectKey, c.SampleSeed, c.KeyShare))
	}
}
