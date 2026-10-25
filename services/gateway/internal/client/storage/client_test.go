package storage_test

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	domainerrors "github.com/alesplll/opens3-rebac/services/gateway/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/testkit"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStoreErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("storage full", func(t *testing.T) {
		upstream := testkit.New()
		upstream.FailStore = status.Error(codes.ResourceExhausted, "disk full")
		_, storage := upstream.Clients(t)
		_, err := storage.Store(ctx, strings.NewReader("data"), nil, "text/plain")
		require.ErrorIs(t, err, domainerrors.ErrInsufficientStorage)
		require.Equal(t, codes.ResourceExhausted, status.Code(err))
	})
	t.Run("other storage failure is not storage full", func(t *testing.T) {
		upstream := testkit.New()
		upstream.FailStore = status.Error(codes.Unavailable, "down")
		_, storage := upstream.Clients(t)
		_, err := storage.Store(ctx, strings.NewReader("data"), nil, "text/plain")
		require.Equal(t, codes.Unavailable, status.Code(err))
		require.NotErrorIs(t, err, domainerrors.ErrInsufficientStorage)
	})
	t.Run("request body fails", func(t *testing.T) {
		upstream := testkit.New()
		_, storage := upstream.Clients(t)
		_, err := storage.Store(ctx, io.MultiReader(strings.NewReader("prefix"), errReader{}), nil, "text/plain")
		var bodyErr *domainerrors.BodyReadError
		require.ErrorAs(t, err, &bodyErr)
		blobs, _ := upstream.Stats()
		require.Zero(t, blobs, "a failed upload must not be committed")
	})
}

func TestStoreReportsSizeAndChecksum(t *testing.T) {
	upstream := testkit.New()
	_, storage := upstream.Clients(t)
	stored, err := storage.Store(context.Background(), strings.NewReader("hello"), nil, "text/plain")
	require.NoError(t, err)
	require.EqualValues(t, 5, stored.Size)
	require.Equal(t, "5d41402abc4b2a76b9719d911017c592", stored.ETag)
	require.NotEmpty(t, stored.BlobID)
}

func TestRetrieveErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("blob lost", func(t *testing.T) {
		_, storage := testkit.New().Clients(t)
		_, err := storage.Retrieve(ctx, "missing", 4)
		require.ErrorIs(t, err, domainerrors.ErrBlobMissing)
		require.Equal(t, codes.NotFound, status.Code(err))
	})
	t.Run("empty stream for nonempty object", func(t *testing.T) {
		upstream := testkit.New()
		upstream.Blobs["blob"] = nil
		_, storage := upstream.Clients(t)
		_, err := storage.Retrieve(ctx, "blob", 1)
		require.ErrorIs(t, err, domainerrors.ErrEmptyStorageStream)
	})
	t.Run("empty stream for empty object", func(t *testing.T) {
		upstream := testkit.New()
		upstream.Blobs["blob"] = nil
		_, storage := upstream.Clients(t)
		body, err := storage.Retrieve(ctx, "blob", 0)
		require.NoError(t, err)
		defer body.Close()
		data, err := io.ReadAll(body)
		require.NoError(t, err)
		require.Empty(t, data)
	})
	t.Run("stream returns every chunk", func(t *testing.T) {
		upstream := testkit.New()
		want := strings.Repeat("x", 2*testkit.RetrieveChunkSize+1)
		upstream.Blobs["blob"] = []byte(want)
		_, storage := upstream.Clients(t)
		body, err := storage.Retrieve(ctx, "blob", int64(len(want)))
		require.NoError(t, err)
		defer body.Close()
		data, err := io.ReadAll(body)
		require.NoError(t, err)
		require.True(t, want == string(data), "retrieved %d bytes, want %d", len(data), len(want))
	})
	t.Run("failure after the last chunk reaches the reader", func(t *testing.T) {
		upstream := testkit.New()
		upstream.Blobs["blob"] = []byte("data")
		upstream.FailRetrieve = status.Error(codes.Internal, "disk error")
		_, storage := upstream.Clients(t)
		body, err := storage.Retrieve(ctx, "blob", 4)
		require.NoError(t, err)
		defer body.Close()
		_, err = io.ReadAll(body)
		require.Equal(t, codes.Internal, status.Code(err))
	})
}

type errReader struct{}

func (errReader) Read([]byte) (int, error) { return 0, errors.New("body failed") }
