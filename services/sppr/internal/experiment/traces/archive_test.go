package traces

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/dataset"
	"github.com/stretchr/testify/require"
)

func TestArchiveIntegrity(t *testing.T) {
	original := makeArchive(t, map[string]string{"../../not-extracted": "fixture\n"})
	sum, err := scanArchive(context.Background(), original, false, func(name string, r io.Reader) error {
		b, err := io.ReadAll(r)
		require.Equal(t, "fixture\n", string(b))
		return err
	})
	require.NoError(t, err)
	want, err := dataset.HashFile(original)
	require.NoError(t, err)
	require.Equal(t, want, sum)
	raw, err := os.ReadFile(original)
	require.NoError(t, err)
	p := filepath.Join(t.TempDir(), "truncated.tar.gz")
	require.NoError(t, os.WriteFile(p, raw[:len(raw)-5], 0644))
	_, err = scanArchive(context.Background(), p, false, func(_ string, r io.Reader) error { _, err := io.Copy(io.Discard, r); return err })
	require.Error(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = scanArchive(ctx, original, false, func(_ string, _ io.Reader) error { t.Fatal("callback after cancellation"); return nil })
	require.ErrorIs(t, err, context.Canceled)
}
