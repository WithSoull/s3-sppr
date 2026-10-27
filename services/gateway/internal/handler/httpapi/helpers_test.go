package httpapi_test

import (
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/handler/httpapi"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/testkit"
	"github.com/stretchr/testify/require"
)

const testMaxReadDuration = time.Minute

func newGatewayHandler(t *testing.T) (http.Handler, *testkit.Upstream) {
	t.Helper()
	upstream := testkit.New()
	return httpapi.NewHandler(upstream.ObjectService(t), testMaxReadDuration), upstream
}

// newGateway serves the real handler over HTTP on top of the in-process Metadata and Storage fakes.
func newGateway(t *testing.T) (*httptest.Server, *testkit.Upstream) {
	t.Helper()
	handler, upstream := newGatewayHandler(t)
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server, upstream
}

// do sends one request and closes the response body when the test ends.
// headers are alternating names and values.
func do(t *testing.T, server *httptest.Server, method, path string, body io.Reader, headers ...string) *http.Response {
	t.Helper()
	request, err := http.NewRequest(method, server.URL+path, body)
	require.NoError(t, err)
	for i := 0; i+1 < len(headers); i += 2 {
		request.Header.Set(headers[i], headers[i+1])
	}
	response, err := server.Client().Do(request)
	require.NoError(t, err)
	t.Cleanup(func() { response.Body.Close() })
	return response
}

type failedReader struct{}

func (failedReader) Read([]byte) (int, error) { return 0, errors.New("storage stream failed") }

func itoa(n int) string { return strconv.Itoa(n) }
