package main

import (
	"compress/gzip"
	"encoding/csv"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/dataset"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/stretchr/testify/require"
)

func externalArgs(out string) []string {
	return []string{"-catalog", "testdata/catalog.json", "-events", "testdata/events.jsonl", "-start", "2023-03-15T00:00:00Z", "-end", "2023-03-15T06:00:00Z", "-train-end", "2023-03-15T02:00:00Z", "-validation-end", "2023-03-15T04:00:00Z", "-out", out}
}
func readManifest(t *testing.T, dir string) dataset.Manifest {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
	require.NoError(t, err)
	var m dataset.Manifest
	require.NoError(t, json.Unmarshal(b, &m))
	return m
}

func TestExternalCLICompactMatchesJSONL(t *testing.T) {
	for _, trainEvery := range []string{"0", "15m"} {
		t.Run(trainEvery, func(t *testing.T) {
			root := t.TempDir()
			jsonDir, csvDir := filepath.Join(root, "json"), filepath.Join(root, "csv")
			require.NoError(t, runArgs(append(externalArgs(jsonDir), "-train-every", trainEvery)))
			require.NoError(t, runArgs(append(externalArgs(csvDir), "-format", "csv.gz", "-train-every", trainEvery)))
			jm, cm := readManifest(t, jsonDir), readManifest(t, csvDir)
			require.Equal(t, jm.Rows, cm.Rows)
			require.Equal(t, jm.Eligible, cm.Eligible)
			require.Equal(t, int64(4), cm.Events)
			require.Equal(t, 1.0/3, cm.SplitFractions["train"])
			require.Equal(t, "2023-03-15T06:00:00Z", cm.Test.End.Format(time.RFC3339))
			require.Equal(t, 26, int(cm.Rows["validation"]))
			require.Equal(t, 26, int(cm.Rows["test"]))
			if trainEvery == "15m" {
				require.Equal(t, int64(5), cm.Rows["train"])
			} else {
				require.Equal(t, int64(13), cm.Rows["train"])
			}
			for name, want := range cm.SHA256 {
				sum, err := dataset.HashFile(filepath.Join(csvDir, name))
				require.NoError(t, err)
				require.Equal(t, want, sum)
			}
			f, err := os.Open(filepath.Join(jsonDir, "slices.jsonl"))
			require.NoError(t, err)
			defer f.Close()
			decoder := json.NewDecoder(f)
			bySplit := map[string][]dataset.Row{}
			for {
				var r dataset.Row
				err := decoder.Decode(&r)
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				bySplit[r.Split] = append(bySplit[r.Split], r)
			}
			// The fixture makes the first row hand-checkable, independent of serialization.
			first := bySplit["train"][0]
			require.Equal(t, int64(2), *first.Label.Reads)
			require.True(t, *first.Label.HasRead)
			require.Nil(t, first.Snapshot.Features.LastReadSeconds)
			require.Zero(t, first.Forecast.ExpectedReads)
			for _, split := range []string{"train", "validation", "test", "purged"} {
				f, err := os.Open(filepath.Join(csvDir, split+".csv.gz"))
				require.NoError(t, err)
				gz, err := gzip.NewReader(f)
				require.NoError(t, err)
				reader := csv.NewReader(gz)
				header, err := reader.Read()
				require.NoError(t, err)
				require.Equal(t, cm.CompactColumns, header)
				for _, row := range bySplit[split] {
					values, err := reader.Read()
					require.NoError(t, err)
					require.Len(t, values, len(header))
					got := map[string]string{}
					for i, name := range header {
						_, duplicate := got[name]
						require.False(t, duplicate)
						got[name] = values[i]
					}
					require.Equal(t, row.Snapshot.Bucket, got["bucket"])
					require.Equal(t, row.Snapshot.ObjectKey, got["object_key"])
					require.Equal(t, row.Snapshot.VersionID, got["version_id"])
					require.Equal(t, row.Snapshot.At.Format(time.RFC3339Nano), got["at"])
					require.Equal(t, strconv.FormatInt(row.Snapshot.SizeBytes, 10), got["size_bytes"])
					require.Equal(t, model.FeatureRulesVersion, got["rules_version"])
					// Decode feature names/values from JSONL, independently of the CSV writer.
					raw, err := json.Marshal(row.Snapshot.Features)
					require.NoError(t, err)
					var features map[string]any
					require.NoError(t, json.Unmarshal(raw, &features))
					for name, value := range features {
						v, exists := got[name]
						require.True(t, exists, name)
						if value == nil {
							require.Empty(t, v, name)
						} else {
							parsed, err := strconv.ParseFloat(v, 64)
							require.NoError(t, err)
							require.Equal(t, value.(float64), parsed, name)
						}
					}
					require.Equal(t, strconv.FormatBool(row.Eligible), got["eligible"])
					require.Equal(t, row.Split, got["split"])
					if row.Label.Reads == nil {
						require.Empty(t, got["label_reads"])
						require.Empty(t, got["label_has_read"])
					} else {
						require.Equal(t, strconv.FormatInt(*row.Label.Reads, 10), got["label_reads"])
						require.Equal(t, strconv.FormatBool(*row.Label.HasRead), got["label_has_read"])
					}
					forecast, err := strconv.ParseFloat(got["ewma_forecast"], 64)
					require.NoError(t, err)
					require.Equal(t, row.Forecast.ExpectedReads, forecast)
					require.Equal(t, strconv.FormatBool(row.Label.Quality.Usable), got["label_usable"])
					require.Equal(t, row.Label.Quality.Reason, got["label_reason"])
				}
				_, err = reader.Read()
				require.ErrorIs(t, err, io.EOF)
				require.NoError(t, gz.Close())
				require.NoError(t, f.Close())
			}
		})
	}
}

func TestCLIExternalValidationAndAtomicOutput(t *testing.T) {
	for _, tt := range []struct {
		name  string
		extra []string
	}{
		{"bad format", []string{"-format", "parquet"}},
		{"bad thinning", []string{"-train-every", "7m"}},
		{"bad periods", []string{"-validation-end", "2023-03-15T01:00:00Z"}},
		{"missing explicit periods", []string{"-train-end", "", "-validation-end", ""}},
		{"generator mixed", []string{"-scenario", "stable"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "out")
			require.Error(t, runArgs(append(externalArgs(dir), tt.extra...)))
			_, err := os.Stat(dir)
			require.True(t, os.IsNotExist(err))
			entries, err := os.ReadDir(filepath.Dir(dir))
			require.NoError(t, err)
			require.Empty(t, entries)
		})
	}
	t.Run("bad event", func(t *testing.T) {
		root := t.TempDir()
		events := filepath.Join(root, "events.jsonl")
		require.NoError(t, os.WriteFile(events, []byte(`{"Sequence":0}`), 0644))
		out := filepath.Join(root, "out")
		require.Error(t, runArgs(append(externalArgs(out), "-events", events)))
		_, err := os.Stat(out)
		require.True(t, os.IsNotExist(err))
	})
}

func TestSyntheticCompactCLI(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	require.NoError(t, runArgs([]string{"-config", "../../examples/data/small.json", "-out", dir, "-format", "csv.gz"}))
	m := readManifest(t, dir)
	require.Equal(t, int64(6048), m.Events)
	require.Equal(t, int64(912), m.Rows["train"])
}
