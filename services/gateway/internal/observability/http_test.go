package observability

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPHandlerPreservesResponse(t *testing.T) {
	handler, err := NewHTTPHandler(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte("created"))
	}))
	if err != nil {
		t.Fatal(err)
	}

	response := httptest.NewRecorder()
	handler.ServeHTTP(response, httptest.NewRequest(http.MethodPut, "/objects/bucket/key", nil))
	if response.Code != http.StatusCreated || response.Body.String() != "created" {
		t.Fatalf("response = %d %q", response.Code, response.Body.String())
	}
}

func TestRouteNameDoesNotExposeObjectKey(t *testing.T) {
	if got := routeName("/objects/private/customer-42/report.pdf"); got != "/objects/{bucket}/{key}" {
		t.Fatalf("routeName() = %q", got)
	}
}

func TestMethodLabel(t *testing.T) {
	tests := []struct {
		method string
		want   string
	}{
		{"GET", "GET"},
		{"PUT", "PUT"},
		{"FOO", "_OTHER"},
		{"get", "_OTHER"},
	}
	for _, tt := range tests {
		t.Run(tt.method, func(t *testing.T) {
			require.Equal(t, tt.want, methodLabel(tt.method))
		})
	}
}
