package httpapi_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/testkit"
	"github.com/stretchr/testify/require"
)

// The fake Storage of each class sees only blobs written to it, so a read from
// the wrong instance fails instead of passing by accident.
func TestPutAndGetUseStorageOfChosenClass(t *testing.T) {
	for _, class := range []model.StorageClass{model.StorageClassHot, model.StorageClassCold} {
		t.Run(string(class), func(t *testing.T) {
			server, upstream := newGateway(t)
			upstream.WriteClass = class
			path := "/objects/" + testkit.Bucket + "/report.csv"

			put := do(t, server, http.MethodPut, path, strings.NewReader("hello"))
			require.Equal(t, http.StatusOK, put.StatusCode)
			meta := upstream.Meta["report.csv"]
			require.Equal(t, string(class), meta.GetStorageClass(), "version registered in another class")
			require.Equal(t, class, upstream.BlobClass[meta.GetBlobId()], "blob written to another Storage")

			get := do(t, server, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, get.StatusCode)
			body, err := io.ReadAll(get.Body)
			require.NoError(t, err)
			require.Equal(t, "hello", string(body))
		})
	}
}

func TestPutPassesOnlyKnownSizeToClassChoice(t *testing.T) {
	server, upstream := newGateway(t)
	path := "/objects/" + testkit.Bucket + "/key"

	require.Equal(t, http.StatusOK, do(t, server, http.MethodPut, path, strings.NewReader("hello")).StatusCode)
	// A reader of unknown length makes the client send the body chunked, without Content-Length.
	unknownLength := io.MultiReader(strings.NewReader("hel"), strings.NewReader("lo"))
	require.Equal(t, http.StatusOK, do(t, server, http.MethodPut, path, unknownLength).StatusCode)

	require.Len(t, upstream.WriteSizes, 2)
	require.NotNil(t, upstream.WriteSizes[0])
	require.Equal(t, int64(5), *upstream.WriteSizes[0])
	require.Nil(t, upstream.WriteSizes[1])
}

func TestGetReadsFromStorageNamedByMetadata(t *testing.T) {
	server, upstream := newGateway(t)
	upstream.SeedIn(model.StorageClassHot, "key", []byte("data"))
	path := "/objects/" + testkit.Bucket + "/key"

	get := do(t, server, http.MethodGet, path, nil)
	require.Equal(t, http.StatusOK, get.StatusCode)

	// Metadata points at COLD, but the blob is only in HOT.
	upstream.Meta["key"].StorageClass = string(model.StorageClassCold)
	require.Equal(t, http.StatusBadGateway, do(t, server, http.MethodGet, path, nil).StatusCode)
}
