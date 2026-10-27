package dataset

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/synthetic"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service/features"
	"github.com/stretchr/testify/require"
)

func TestWriteReproducibleDataset(t *testing.T) {
	data, err := os.ReadFile("../../../examples/data/small.json")
	require.NoError(t, err)
	var cfg synthetic.Config
	require.NoError(t, json.Unmarshal(data, &cfg))
	first, err := Write(context.Background(), cfg, t.TempDir(), nil)
	require.NoError(t, err)
	dir := t.TempDir()
	second, err := Write(context.Background(), cfg, dir, nil)
	require.NoError(t, err)
	require.Equal(t, first, second)
	require.Equal(t, int64(6048), first.Events)
	require.Equal(t, map[string]int64{"train": 912, "validation": 480, "test": 1776}, first.Rows)
	require.Equal(t, first.Rows, first.Eligible)
	f, err := os.Open(filepath.Join(dir, "slices.jsonl"))
	require.NoError(t, err)
	defer f.Close()
	decoder := json.NewDecoder(f)
	var firstRow Row
	require.NoError(t, decoder.Decode(&firstRow))
	require.True(t, firstRow.Snapshot.Quality.Usable)
	require.Equal(t, "no_history", firstRow.Snapshot.Quality.Reason)
	var manifest Manifest
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	require.NoError(t, err)
	require.NoError(t, json.Unmarshal(b, &manifest))
	require.Equal(t, second, manifest)
}

func TestTrimPreservesFeatureAndLabelResults(t *testing.T) {
	req := Lesson()
	for i := range req.Catalog {
		req.Catalog[i].CreatedAt = req.At.Add(-60 * time.Hour)
	}
	for _, hours := range []time.Duration{50, 40, 30} {
		e := req.Events[0]
		e.ObjectKey = "b"
		e.VersionID = "b1"
		e.OccurredAt = req.At.Add(-hours * time.Hour)
		req.Events = append(req.Events, e)
	}
	sort.Slice(req.Events, func(i, j int) bool { return req.Events[i].OccurredAt.Before(req.Events[j].OccurredAt) })
	coverage := model.Coverage{Known: true, Start: req.At.Add(-60 * time.Hour), Watermark: req.At.Add(time.Hour)}
	want, err := Rows(context.Background(), req, coverage, req.At.Add(-24*time.Hour))
	require.NoError(t, err)
	req.Events = trim(req.Events, req.At.Add(-features.MaxWindow))
	got, err := Rows(context.Background(), req, coverage, req.At.Add(-24*time.Hour))
	require.NoError(t, err)
	require.Equal(t, want, got)
}
