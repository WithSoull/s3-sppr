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

func TestHeadAfterPut(t *testing.T) {
	for _, tt := range []struct {
		name  string
		class model.StorageClass
		body  string
	}{
		{name: "COLD", class: model.StorageClassCold, body: "hello"},
		{name: "HOT", class: model.StorageClassHot, body: "hello"},
		{name: "empty object", class: model.StorageClassCold},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server, upstream := newGateway(t)
			upstream.WriteClass = tt.class
			path := "/objects/" + testkit.Bucket + "/nested/key"
			put := do(t, server, http.MethodPut, path, strings.NewReader(tt.body), "Content-Type", "text/plain")
			require.Equal(t, http.StatusOK, put.StatusCode)
			get := do(t, server, http.MethodGet, path, nil)
			require.Equal(t, http.StatusOK, get.StatusCode)
			data, err := io.ReadAll(get.Body)
			require.NoError(t, err)
			require.Equal(t, tt.body, string(data))

			checkHead := func() {
				t.Helper()
				head := do(t, server, http.MethodHead, path, nil, "Range", "bytes=999-", "If-Range", put.Header.Get("ETag"))
				require.Equal(t, http.StatusOK, head.StatusCode)
				require.Equal(t, int64(len(tt.body)), head.ContentLength)
				for _, header := range []string{"Content-Type", "Content-Length", "ETag", "X-Object-Version-Id", "Accept-Ranges", "X-Content-Type-Options"} {
					require.Equal(t, get.Header.Get(header), head.Header.Get(header), header)
				}
				require.Equal(t, put.Header.Get("ETag"), head.Header.Get("ETag"))
				require.Empty(t, head.Header.Get("Content-Range"))
				data, err := io.ReadAll(head.Body)
				require.NoError(t, err)
				require.Empty(t, data)
			}
			checkHead()
			upstream.Lock()
			delete(upstream.Blobs, upstream.Meta["nested/key"].GetBlobId())
			upstream.Unlock()
			checkHead()
			require.Equal(t, http.StatusBadGateway, do(t, server, http.MethodGet, path, nil).StatusCode,
				"GET must detect the missing blob that HEAD never touches")
		})
	}
}
