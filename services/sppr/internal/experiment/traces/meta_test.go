package traces

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/dataset"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func config() Config {
	return Config{Input: "testdata/meta.csv", SourceURL: SourceRoot + "fixture.csv", Bucket: "eag", SourceShare: 0.2, KeyShare: 1, SampleSeed: "0", AllowInferredSuccess: true}
}

func TestConvertMeta(t *testing.T) {
	for _, compressed := range []bool{false, true} {
		t.Run(map[bool]string{false: "csv", true: "zstd"}[compressed], func(t *testing.T) {
			c := config()
			if compressed {
				raw, err := os.ReadFile(c.Input)
				require.NoError(t, err)
				z, err := zstd.NewWriter(nil)
				require.NoError(t, err)
				c.Input = filepath.Join(t.TempDir(), "fixture.csv.zst")
				require.NoError(t, os.WriteFile(c.Input, z.EncodeAll(raw, nil), 0644))
				z.Close()
			}
			dir := t.TempDir()
			m, err := Convert(context.Background(), c, dir)
			require.NoError(t, err)
			require.Equal(t, int64(5), m.Counts["events"])
			require.Equal(t, int64(6), m.Counts["versions"])
			require.Equal(t, int64(2), m.Counts["ignored_non_get"])
			require.Equal(t, int64(1), m.Counts["unknown_size"])
			require.Equal(t, int64(1), m.Counts["no_body"])
			require.Equal(t, int64(1), m.Counts["invalid_range"])
			b, err := os.ReadFile(filepath.Join(dir, "catalog.json"))
			require.NoError(t, err)
			var versions []model.Version
			require.NoError(t, json.Unmarshal(b, &versions))
			require.Len(t, versions, 6)
			require.Equal(t, int64(100), versions[0].SizeBytes)
			require.Equal(t, int64(200), versions[1].SizeBytes)
			require.NotEqual(t, versions[0].VersionID, versions[2].VersionID, "return to original size creates a new generation")
			require.NotEqual(t, versions[2].VersionID, versions[3].VersionID, "same-size SET creates a new version")
			require.Equal(t, "2023-03-15T00:00:00Z", versions[0].CreatedAt.Format("2006-01-02T15:04:05Z"))
			f, err := os.Open(filepath.Join(dir, "events.jsonl"))
			require.NoError(t, err)
			defer f.Close()
			d := json.NewDecoder(f)
			for i, want := range []int64{100, 50, 200, 100, 100} {
				var e model.AccessEvent
				require.NoError(t, d.Decode(&e))
				require.True(t, e.IsDemand())
				require.Equal(t, want, e.Bytes)
				require.Equal(t, int64(i+1), e.Sequence)
				require.Equal(t, "a", e.ObjectKey)
			}
			for _, name := range []string{"catalog.json", "events.jsonl"} {
				sum, err := dataset.HashFile(filepath.Join(dir, name))
				require.NoError(t, err)
				require.Equal(t, sum, m.SHA256[name])
			}
			sum, err := dataset.HashFile(c.Input)
			require.NoError(t, err)
			require.Equal(t, sum, m.InputSHA256[filepath.Base(c.Input)])
			require.Equal(t, 0.2, m.Sampling["source_file_share"])
			require.Contains(t, strings.Join(m.Limitations, " "), "first observation")
		})
	}
}

func TestSamplingDeterministicByKey(t *testing.T) {
	require.True(t, SampleKey("a", "0", 0.5))
	require.False(t, SampleKey("b", "0", 0.5))
	for _, key := range []string{"a", "b", "c", "d", "e"} {
		for i := 0; i < 10; i++ {
			require.Equal(t, SampleKey(key, "0", 0.5), SampleKey(key, "0", 0.5))
			require.True(t, SampleKey(key, "0", 1))
		}
	}
	c := config()
	c.KeyShare = 0.5
	a, b := t.TempDir(), t.TempDir()
	m, err := Convert(context.Background(), c, a)
	require.NoError(t, err)
	n, err := Convert(context.Background(), c, b)
	require.NoError(t, err)
	require.Equal(t, m, n)
	for _, name := range []string{"catalog.json", "events.jsonl"} {
		x, _ := os.ReadFile(filepath.Join(a, name))
		y, _ := os.ReadFile(filepath.Join(b, name))
		require.Equal(t, x, y)
	}
}

func TestConvertRejectsBadInput(t *testing.T) {
	raw, err := os.ReadFile("testdata/meta.csv")
	require.NoError(t, err)
	for _, tt := range []struct {
		name  string
		raw   []byte
		optin bool
	}{
		{"missing opt in", raw, false},
		{"bad header", []byte("timestamp,key\n1,a\n"), true},
		{"out of order", bytes.Replace(raw, []byte("1678838460000"), []byte("1678838300000"), 1), true},
		{"malformed", append(raw, []byte("invalid,row\n")...), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := config()
			c.Input = filepath.Join(t.TempDir(), "trace.csv")
			c.AllowInferredSuccess = tt.optin
			require.NoError(t, os.WriteFile(c.Input, tt.raw, 0644))
			_, err := Convert(context.Background(), c, t.TempDir())
			require.Error(t, err)
		})
	}
	t.Run("truncated zstd", func(t *testing.T) {
		z, err := zstd.NewWriter(nil)
		require.NoError(t, err)
		compressed := z.EncodeAll(raw, nil)
		z.Close()
		c := config()
		c.Input = filepath.Join(t.TempDir(), "trace.zst")
		require.NoError(t, os.WriteFile(c.Input, compressed[:len(compressed)-7], 0644))
		_, err = Convert(context.Background(), c, t.TempDir())
		require.Error(t, err)
	})
}
