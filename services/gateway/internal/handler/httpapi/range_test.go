package httpapi_test

import (
	"io"
	"net/http"
	"testing"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/testkit"
	"github.com/stretchr/testify/require"
)

func TestGetRange(t *testing.T) {
	tests := []struct {
		name             string
		object           string
		rangeHeader      string
		wantStatus       int
		wantBody         string
		wantContentRange string
	}{
		{name: "closed range", object: "0123456789", rangeHeader: "bytes=2-4", wantStatus: http.StatusPartialContent, wantBody: "234", wantContentRange: "bytes 2-4/10"},
		{name: "open range", object: "0123456789", rangeHeader: "bytes=7-", wantStatus: http.StatusPartialContent, wantBody: "789", wantContentRange: "bytes 7-9/10"},
		{name: "suffix", object: "0123456789", rangeHeader: "bytes=-3", wantStatus: http.StatusPartialContent, wantBody: "789", wantContentRange: "bytes 7-9/10"},
		{name: "suffix longer than object", object: "0123456789", rangeHeader: "bytes=-20", wantStatus: http.StatusPartialContent, wantBody: "0123456789", wantContentRange: "bytes 0-9/10"},
		{name: "end past object", object: "0123456789", rangeHeader: "bytes=8-100", wantStatus: http.StatusPartialContent, wantBody: "89", wantContentRange: "bytes 8-9/10"},
		{name: "start past object", object: "0123456789", rangeHeader: "bytes=10-", wantStatus: http.StatusRequestedRangeNotSatisfiable, wantContentRange: "bytes */10"},
		{name: "any range of empty object", object: "", rangeHeader: "bytes=0-", wantStatus: http.StatusRequestedRangeNotSatisfiable, wantContentRange: "bytes */0"},
		{name: "several ranges are ignored", object: "0123456789", rangeHeader: "bytes=0-1,3-4", wantStatus: http.StatusOK, wantBody: "0123456789"},
		{name: "invalid syntax is ignored", object: "0123456789", rangeHeader: "bytes=x-1", wantStatus: http.StatusOK, wantBody: "0123456789"},
		{name: "reversed range is ignored", object: "0123456789", rangeHeader: "bytes=5-2", wantStatus: http.StatusOK, wantBody: "0123456789"},
		{name: "other unit is ignored", object: "0123456789", rangeHeader: "items=0-1", wantStatus: http.StatusOK, wantBody: "0123456789"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server, upstream := newGateway(t)
			upstream.Seed("key", []byte(tt.object))

			response := do(t, server, http.MethodGet, "/objects/"+testkit.Bucket+"/key", nil, "Range", tt.rangeHeader)
			require.Equal(t, tt.wantStatus, response.StatusCode)
			require.Equal(t, tt.wantContentRange, response.Header.Get("Content-Range"))
			if tt.wantStatus == http.StatusRequestedRangeNotSatisfiable {
				return
			}
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Equal(t, tt.wantBody, string(body))
			require.Equal(t, "bytes", response.Header.Get("Accept-Ranges"))
		})
	}
}

func TestGetRangeAcrossStorageChunks(t *testing.T) {
	server, upstream := newGateway(t)
	object := make([]byte, 2*testkit.RetrieveChunkSize+10)
	for i := range object {
		object[i] = byte(i % 251)
	}
	upstream.Seed("key", object)
	first, last := testkit.RetrieveChunkSize-5, testkit.RetrieveChunkSize+5

	response := do(t, server, http.MethodGet, "/objects/"+testkit.Bucket+"/key", nil, "Range", "bytes="+itoa(first)+"-"+itoa(last))
	require.Equal(t, http.StatusPartialContent, response.StatusCode)
	body, err := io.ReadAll(response.Body)
	require.NoError(t, err)
	require.Equal(t, object[first:last+1], body)
}
