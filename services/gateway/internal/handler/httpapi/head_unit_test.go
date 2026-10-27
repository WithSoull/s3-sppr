package httpapi_test

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/handler/httpapi"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	"github.com/alesplll/opens3-rebac/services/gateway/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestHeadObject(t *testing.T) {
	logger.SetNopLogger()
	type args struct {
		path, byteRange, ifRange string
	}
	type mockBuilder func(*testing.T, *minimock.Controller) *mocks.ObjectServiceMock
	meta := model.ObjectMeta{Size: 10, ETag: "etag", VersionID: "version", ContentType: "text/plain"}
	defaultType := meta
	defaultType.ContentType = ""
	emptyMeta := meta
	emptyMeta.Size = 0
	invalidMeta := meta
	invalidMeta.Size = -1
	metadataResult := func(result model.ObjectMeta, err error) mockBuilder {
		return func(t *testing.T, mc *minimock.Controller) *mocks.ObjectServiceMock {
			objects := mocks.NewObjectServiceMock(mc)
			objects.HeadMock.Set(func(ctx context.Context, bucket, key string) (model.ObjectMeta, error) {
				assert.Equal(t, "bucket", bucket)
				assert.Equal(t, "nested/key", key)
				_, bounded := ctx.Deadline()
				assert.False(t, bounded, "HEAD must not inherit the GET duration limit")
				return result, err
			})
			return objects
		}
	}
	tests := []struct {
		name            string
		args            args
		wantStatus      int
		wantCalls       uint64
		wantContentType string
		wantLength      string
		buildMock       mockBuilder
	}{
		{name: "full metadata", args: args{path: "/objects/bucket/nested/key"}, wantStatus: http.StatusOK,
			wantCalls: 1, wantContentType: "text/plain", wantLength: "10", buildMock: metadataResult(meta, nil)},
		{name: "default content type", args: args{path: "/objects/bucket/nested/key"}, wantStatus: http.StatusOK,
			wantCalls: 1, wantContentType: "application/octet-stream", wantLength: "10", buildMock: metadataResult(defaultType, nil)},
		{name: "empty object", args: args{path: "/objects/bucket/nested/key"}, wantStatus: http.StatusOK,
			wantCalls: 1, wantContentType: "text/plain", wantLength: "0", buildMock: metadataResult(emptyMeta, nil)},
		{name: "Range and matching If-Range ignored", args: args{path: "/objects/bucket/nested/key", byteRange: "bytes=1-3", ifRange: `"etag"`}, wantStatus: http.StatusOK,
			wantCalls: 1, wantContentType: "text/plain", wantLength: "10", buildMock: metadataResult(meta, nil)},
		{name: "unsatisfiable Range ignored", args: args{path: "/objects/bucket/nested/key", byteRange: "bytes=999-"}, wantStatus: http.StatusOK,
			wantCalls: 1, wantContentType: "text/plain", wantLength: "10", buildMock: metadataResult(meta, nil)},
		{name: "invalid path", args: args{path: "/objects/bucket/"}, wantStatus: http.StatusNotFound,
			buildMock: func(_ *testing.T, mc *minimock.Controller) *mocks.ObjectServiceMock {
				return mocks.NewObjectServiceMock(mc)
			}},
		{name: "metadata not found", args: args{path: "/objects/bucket/nested/key"}, wantStatus: http.StatusNotFound,
			wantCalls: 1, buildMock: metadataResult(model.ObjectMeta{}, status.Error(codes.NotFound, "private detail"))},
		{name: "invalid object size", args: args{path: "/objects/bucket/nested/key"}, wantStatus: http.StatusBadGateway,
			wantCalls: 1, buildMock: metadataResult(invalidMeta, nil)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			objects := tt.buildMock(t, mc)
			server := httptest.NewServer(httpapi.NewHandler(objects, time.Nanosecond))
			t.Cleanup(server.Close)
			request, err := http.NewRequest(http.MethodHead, server.URL+tt.args.path, nil)
			require.NoError(t, err)
			request.Close = true
			request.Header.Set("Range", tt.args.byteRange)
			request.Header.Set("If-Range", tt.args.ifRange)

			// Inspect the raw wire: ResponseRecorder writes http.Error's body, and
			// http.Client hides HEAD bodies even if a server incorrectly sends one.
			conn, err := net.DialTimeout("tcp", request.URL.Host, time.Second)
			require.NoError(t, err)
			defer conn.Close()
			require.NoError(t, conn.SetDeadline(time.Now().Add(3*time.Second)))
			require.NoError(t, request.Write(conn))
			wire, err := io.ReadAll(conn)
			require.NoError(t, err)
			parts := strings.SplitN(string(wire), "\r\n\r\n", 2)
			require.Len(t, parts, 2)
			require.Empty(t, parts[1], "net/http must suppress HEAD bodies, including errors")
			response, err := http.ReadResponse(bufio.NewReader(strings.NewReader(string(wire))), request)
			require.NoError(t, err)
			defer response.Body.Close()
			require.Equal(t, tt.wantStatus, response.StatusCode)
			require.Empty(t, response.Header.Get("Content-Range"))
			if tt.wantStatus == http.StatusOK {
				require.Equal(t, tt.wantContentType, response.Header.Get("Content-Type"))
				require.Equal(t, tt.wantLength, response.Header.Get("Content-Length"))
				require.Equal(t, `"etag"`, response.Header.Get("ETag"))
				require.Equal(t, "version", response.Header.Get("X-Object-Version-Id"))
				require.Equal(t, "bytes", response.Header.Get("Accept-Ranges"))
				require.Equal(t, "nosniff", response.Header.Get("X-Content-Type-Options"))
			}
			require.Equal(t, tt.wantCalls, objects.HeadAfterCounter())
			require.Zero(t, objects.GetAfterCounter())
			require.Zero(t, objects.PutAfterCounter())
			require.Zero(t, objects.DeleteAfterCounter())
		})
	}
}
