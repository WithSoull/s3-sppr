package runner

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDropCacheKeepsModelsMapsAndFirstDataset(t *testing.T) {
	dir := t.TempDir()
	for _, p := range []string{"features.gob", "forecasts.gob", "demand.gob", "train-048/x.bin", "train-072/x.bin", "train-048.log", "model-048/manifest.json", "age-baseline-map.json", "manifest.json"} {
		require.NoError(t, os.MkdirAll(filepath.Dir(filepath.Join(dir, p)), 0755))
		require.NoError(t, os.WriteFile(filepath.Join(dir, p), []byte("x"), 0600))
	}
	require.NoError(t, dropCache(dir))
	for _, gone := range []string{"features.gob", "forecasts.gob", "demand.gob", "train-072"} {
		require.NoFileExists(t, filepath.Join(dir, gone))
		require.NoDirExists(t, filepath.Join(dir, gone))
	}
	for _, kept := range []string{"train-048/x.bin", "train-048.log", "model-048/manifest.json", "age-baseline-map.json", "manifest.json"} {
		require.FileExists(t, filepath.Join(dir, kept))
	}
}
