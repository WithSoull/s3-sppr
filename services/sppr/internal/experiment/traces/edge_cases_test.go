package traces

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/validator"
	"github.com/stretchr/testify/require"
)

func TestConvertRealDataEdgeCases(t *testing.T) {
	c := config()
	c.Input = "testdata/edge_cases.csv"
	dir := t.TempDir()
	m, err := Convert(context.Background(), c, dir)
	require.NoError(t, err)
	for name, want := range map[string]int64{
		"input_rows": 18, "events": 8, "versions": 7, "empty_key": 1, "invalid_object_size": 2,
		"unknown_size": 1, "invalid_negative_size": 1, "invalid_timestamp": 1, "invalid_op_type": 1,
		"invalid_response_size": 2, "invalid_range": 1, "duplicate_timestamps": 2,
		"long_key_rows": 3, "mapped_key_rows": 4, "mapped_keys": 3, "selected_mapped_keys": 3,
		"out_of_order_timestamps": 0,
	} {
		require.Equal(t, want, m.Counts[name], name)
	}
	require.Contains(t, m.FieldMapping["ObjectKey"], "1024")
	require.Contains(t, m.RowPolicy["timestamps"], "fatal")
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	require.NoError(t, err)
	var saved Manifest
	require.NoError(t, json.Unmarshal(b, &saved))
	require.Equal(t, m, saved)
	f, err := os.Open(filepath.Join(dir, "events.jsonl"))
	require.NoError(t, err)
	defer f.Close()
	var events []model.AccessEvent
	d := json.NewDecoder(f)
	for {
		var e model.AccessEvent
		err := d.Decode(&e)
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		require.NoError(t, validator.AccessEvent(e))
		events = append(events, e)
	}
	require.Len(t, events, 8)
	originalA, originalB := "1"+strings.Repeat("0", 1315), "2"+strings.Repeat("0", 1315)
	sum := sha256.Sum256([]byte(originalA))
	mappedA := "sha256-" + hex.EncodeToString(sum[:])
	sum = sha256.Sum256([]byte(originalB))
	mappedB := "sha256-" + hex.EncodeToString(sum[:])
	require.Equal(t, mappedA, events[1].ObjectKey)
	require.Equal(t, mappedA, events[2].ObjectKey)
	require.Equal(t, events[1].VersionID, events[2].VersionID)
	require.NotEqual(t, events[1].Sequence, events[2].Sequence, "equal timestamps do not deduplicate reads")
	require.Equal(t, mappedB, events[3].ObjectKey)
	require.NotEqual(t, mappedA, mappedB)
	require.NotEqual(t, mappedA, events[4].ObjectKey, "literal reserved-prefix key cannot alias a mapped long key")
	require.Equal(t, strings.Repeat("k", 1024), events[5].ObjectKey)
	require.Zero(t, events[6].Bytes)
	// The identical original-key sampling decision applies before mapping.
	c.KeyShare = 0.5
	sampledDir := t.TempDir()
	sampled, err := Convert(context.Background(), c, sampledDir)
	require.NoError(t, err)
	require.Equal(t, m.Counts["mapped_keys"], sampled.Counts["mapped_keys"])
	require.Equal(t, m.Counts["unknown_size"], sampled.Counts["unknown_size"])
	originals := []string{"base", originalA, originalA, originalB, mappedA, strings.Repeat("k", 1024), "empty-object", "same-time"}
	var expected []string
	for _, original := range originals {
		if SampleKey(original, c.SampleSeed, c.KeyShare) {
			expected = append(expected, mapKey(original))
		}
	}
	sf, err := os.Open(filepath.Join(sampledDir, "events.jsonl"))
	require.NoError(t, err)
	defer sf.Close()
	sd := json.NewDecoder(sf)
	for _, key := range expected {
		var e model.AccessEvent
		require.NoError(t, sd.Decode(&e))
		require.Equal(t, key, e.ObjectKey)
	}
	require.ErrorIs(t, sd.Decode(new(model.AccessEvent)), io.EOF)

}

func TestOutOfOrderIsExplicitlyFatal(t *testing.T) {
	c := config()
	c.Input = "testdata/out_of_order.csv"
	dir := t.TempDir()
	m, err := Convert(context.Background(), c, dir)
	require.ErrorContains(t, err, "CSV row 4: out-of-order timestamp")
	require.ErrorContains(t, err, "stable-sort")
	require.Equal(t, int64(1), m.Counts["out_of_order_timestamps"])
	_, err = os.Stat(filepath.Join(dir, "manifest.json"))
	require.True(t, os.IsNotExist(err), "must not claim a complete conversion")
}

func TestInvalidKeyBytesAreCounted(t *testing.T) {
	for _, tt := range []struct {
		name string
		key  string
	}{{"NUL", "a\x00b"}, {"invalid UTF8", "a\xffb"}} {
		t.Run(tt.name, func(t *testing.T) {
			c := config()
			b, err := os.ReadFile(c.Input)
			require.NoError(t, err)
			b = append(b, []byte("1678839000000,"+tt.key+",1,100,120,20,-1,-1,60,0.0001,1,1,1000,9,2000\n")...)
			c.Input = filepath.Join(t.TempDir(), "invalid.csv")
			require.NoError(t, os.WriteFile(c.Input, b, 0644))
			m, err := Convert(context.Background(), c, t.TempDir())
			require.NoError(t, err)
			require.Equal(t, int64(1), m.Counts["invalid_key"])
			require.Equal(t, int64(5), m.Counts["events"])
		})
	}
}
