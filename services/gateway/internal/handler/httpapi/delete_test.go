package httpapi_test

import (
	"net/http"
	"strings"
	"testing"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/testkit"
	"github.com/stretchr/testify/require"
)

func TestDeleteRemovesObjectFromCatalogOnly(t *testing.T) {
	server, upstream := newGateway(t)
	path := "/objects/" + testkit.Bucket + "/nested/key"
	require.Equal(t, http.StatusOK, do(t, server, http.MethodPut, path, strings.NewReader("data")).StatusCode)
	blobID := upstream.Meta["nested/key"].GetBlobId()

	require.Equal(t, http.StatusNoContent, do(t, server, http.MethodDelete, path, nil).StatusCode)
	require.Equal(t, http.StatusNotFound, do(t, server, http.MethodGet, path, nil).StatusCode)
	require.Equal(t, http.StatusNotFound, do(t, server, http.MethodDelete, path, nil).StatusCode)
	require.Contains(t, upstream.Blobs, blobID, "blob cleanup is not part of DELETE yet")
}

func TestUnsupportedMethodListsDelete(t *testing.T) {
	server, _ := newGateway(t)
	response := do(t, server, http.MethodPost, "/objects/"+testkit.Bucket+"/key", nil)
	require.Equal(t, http.StatusMethodNotAllowed, response.StatusCode)
	require.Equal(t, "GET, PUT, DELETE", response.Header.Get("Allow"))
}
