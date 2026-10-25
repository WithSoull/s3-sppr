package httpapi_test

import (
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/testkit"
	"github.com/stretchr/testify/require"
)

func TestPutGetRoundTrip(t *testing.T) {
	server, upstream := newGateway(t)
	for _, tc := range []struct {
		name string
		body string
	}{
		{name: "nested key", body: "hello world"},
		{name: "empty object", body: ""},
		{name: "multiple storage chunks", body: strings.Repeat("0123456789abcdef", 3*testkit.RetrieveChunkSize/16)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := "/objects/" + testkit.Bucket + "/" + strings.ReplaceAll(tc.name, " ", "-") + "/file.txt"
			put := do(t, server, http.MethodPut, path, strings.NewReader(tc.body), "Content-Type", "text/plain")
			require.Equal(t, http.StatusOK, put.StatusCode)
			require.Equal(t, "version-1", put.Header.Get("X-Object-Version-Id"))

			got := do(t, server, http.MethodGet, path, nil)
			body, err := io.ReadAll(got.Body)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, got.StatusCode)
			require.True(t, tc.body == string(body), "GET body differs from PUT body (%d bytes)", len(body))
			require.Equal(t, "text/plain", got.Header.Get("Content-Type"))
			require.Equal(t, put.Header.Get("ETag"), got.Header.Get("ETag"))
		})
	}
	blobs, _ := upstream.Stats()
	require.Equal(t, 3, blobs)
}

func TestHealthEndpoint(t *testing.T) {
	server, _ := newGateway(t)
	require.Equal(t, http.StatusOK, do(t, server, http.MethodGet, "/healthz", nil).StatusCode)
}
