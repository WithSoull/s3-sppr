package httpapi_test

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/handler/httpapi"
	"github.com/stretchr/testify/require"
)

func TestSwaggerEndpoints(t *testing.T) {
	handler := httpapi.NewHandler(nil)

	document := httptest.NewRecorder()
	handler.ServeHTTP(document, httptest.NewRequest(http.MethodGet, "/swagger/doc.json", nil))
	if document.Code != http.StatusOK || !strings.Contains(document.Body.String(), `"/objects/{bucket}/{key}"`) {
		t.Fatalf("Swagger document: status=%d body=%q", document.Code, document.Body.String())
	}

	ui := httptest.NewRecorder()
	handler.ServeHTTP(ui, httptest.NewRequest(http.MethodGet, "/swagger/index.html", nil))
	if ui.Code != http.StatusOK {
		t.Fatalf("Swagger UI status = %d", ui.Code)
	}
}

func TestSwaggerBucketRoundTrip(t *testing.T) {
	for _, key := range []string{"key", "doc.json", "index.html"} {
		t.Run(key, func(t *testing.T) {
			server, _ := newGateway(t)
			payload := "object bytes, not Swagger"
			request, err := http.NewRequest(http.MethodPut, server.URL+"/objects/swagger/"+key, strings.NewReader(payload))
			require.NoError(t, err)
			response, err := server.Client().Do(request)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, response.StatusCode)
			response.Body.Close()
			response, err = server.Client().Get(server.URL + "/objects/swagger/" + key)
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, http.StatusOK, response.StatusCode)
			body, err := io.ReadAll(response.Body)
			require.NoError(t, err)
			require.Equal(t, payload, string(body))
		})
	}
}
