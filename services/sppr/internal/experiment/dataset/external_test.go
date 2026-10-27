package dataset

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/stretchr/testify/require"
)

func TestExternalProvenanceAndValidation(t *testing.T) {
	start := time.Date(2023, 3, 15, 0, 0, 0, 0, time.UTC)
	options := Options{SnapshotEvery: 5 * time.Minute, Periods: Periods{model.Interval{Start: start, End: start.Add(2 * time.Hour)}, model.Interval{Start: start.Add(2 * time.Hour), End: start.Add(4 * time.Hour)}, model.Interval{Start: start.Add(4 * time.Hour), End: start.Add(6 * time.Hour)}}}
	catalogPath := "../../../cmd/dataset/testdata/catalog.json"
	eventsPath := "../../../cmd/dataset/testdata/events.jsonl"
	catalogHash, err := HashFile(catalogPath)
	require.NoError(t, err)
	eventsHash, err := HashFile(eventsPath)
	require.NoError(t, err)
	for _, tt := range []struct {
		name         string
		mutate       func(*model.AccessEvent)
		manifestHash string
		wantError    bool
	}{
		{name: "source preserved", manifestHash: eventsHash},
		{name: "source changed", manifestHash: "wrong", wantError: true},
		{name: "unsorted", mutate: func(e *model.AccessEvent) { e.OccurredAt = start.Add(-time.Second) }, wantError: true},
		{name: "unknown version", mutate: func(e *model.AccessEvent) { e.VersionID = "unknown" }, wantError: true},
		{name: "exceeds size", mutate: func(e *model.AccessEvent) { e.Bytes = 1000 }, wantError: true},
		{name: "duplicate sequence", mutate: func(e *model.AccessEvent) { e.Sequence = 1 }, wantError: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := t.TempDir()
			events := eventsPath
			manifest := ""
			if tt.mutate != nil {
				b, err := os.ReadFile(eventsPath)
				require.NoError(t, err)
				var rows []model.AccessEvent
				for _, line := range bytes.Split(bytes.TrimSpace(b), []byte("\n")) {
					var e model.AccessEvent
					require.NoError(t, json.Unmarshal(line, &e))
					rows = append(rows, e)
				}
				tt.mutate(&rows[1])
				events = filepath.Join(dir, "input.jsonl")
				f, err := os.Create(events)
				require.NoError(t, err)
				enc := json.NewEncoder(f)
				for _, e := range rows {
					require.NoError(t, enc.Encode(e))
				}
				require.NoError(t, f.Close())
			}
			if tt.manifestHash != "" {
				b, err := json.Marshal(map[string]any{"license": "fixture only", "limitations": []string{"first observation"}, "sha256": map[string]string{"catalog.json": catalogHash, "events.jsonl": tt.manifestHash}})
				require.NoError(t, err)
				manifest = filepath.Join(dir, "source.json")
				require.NoError(t, os.WriteFile(manifest, b, 0644))
			}
			out := filepath.Join(dir, "out")
			require.NoError(t, os.Mkdir(out, 0755))
			m, err := WriteExternal(context.Background(), catalogPath, events, manifest, out, nil, options)
			if tt.wantError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.JSONEq(t, string(m.SourceManifest), `{"license":"fixture only","limitations":["first observation"],"sha256":{"catalog.json":"`+catalogHash+`","events.jsonl":"`+eventsHash+`"}}`)
			require.Equal(t, eventsHash, m.InputSHA256["events.jsonl"])
			require.Equal(t, catalogHash, m.InputSHA256["catalog.json"])
		})
	}
}
