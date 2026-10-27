package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/testkit"
	"github.com/stretchr/testify/require"
)

// METADATA_RPC_TIMEOUT must bound every Metadata call made for an ordinary request that has no deadline.
func TestMetadataTimeoutThroughHTTP(t *testing.T) {
	tests := []struct{ name, operation, method string }{
		{name: "HeadBucket", operation: testkit.OpHeadBucket, method: http.MethodPut},
		{name: "ChooseWriteClass", operation: testkit.OpChooseWriteClass, method: http.MethodPut},
		{name: "CreateObjectVersion", operation: testkit.OpCreateObjectVersion, method: http.MethodPut},
		{name: "GetObjectMeta", operation: testkit.OpGetObjectMeta, method: http.MethodGet},
		{name: "DeleteObjectMeta", operation: testkit.OpDeleteObjectMeta, method: http.MethodDelete},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			upstream := testkit.New()
			released := make(chan struct{})
			upstream.OnMetadata = func(ctx context.Context, operation string) error {
				if operation != tt.operation {
					return nil
				}
				<-ctx.Done()
				close(released)
				return ctx.Err()
			}
			address := upstream.ListenTCP(t)
			a := newTestApp(t, map[string]string{
				"METADATA_GRPC_ADDR":     address,
				"STORAGE_COLD_GRPC_ADDR": address,
				"STORAGE_HOT_GRPC_ADDR":  upstream.ListenStorageTCP(t, model.StorageClassHot),
				"METADATA_RPC_TIMEOUT":   "200ms",
			})
			server := httptest.NewServer(a.httpServer.Handler)
			t.Cleanup(server.Close)
			client := server.Client()
			client.Timeout = 3 * time.Second

			request, err := http.NewRequest(tt.method, server.URL+"/objects/"+testkit.Bucket+"/key", strings.NewReader("data"))
			require.NoError(t, err)
			start := time.Now()
			response, err := client.Do(request)
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, http.StatusGatewayTimeout, response.StatusCode)
			require.Less(t, time.Since(start), 2*time.Second)
			select {
			case <-released:
			case <-time.After(time.Second):
				t.Fatal("Metadata handler survived its RPC deadline")
			}
		})
	}
}
