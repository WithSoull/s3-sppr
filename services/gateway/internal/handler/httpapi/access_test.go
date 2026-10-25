package httpapi_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/handler/httpapi"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	"github.com/alesplll/opens3-rebac/services/gateway/pkg/mocks"
	access "github.com/alesplll/opens3-rebac/shared/pkg/go-kit/events/access"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestGetAccessOutcome(t *testing.T) {
	logger.SetNopLogger()
	for _, tt := range []struct {
		name        string
		body        func() io.Reader
		size        int64
		serviceErr  error
		writerFails bool
		wantResult  string
		wantBytes   int64
		wantPanic   bool
	}{
		{name: "complete", body: func() io.Reader { return strings.NewReader("data") }, size: 4, wantResult: "success", wantBytes: 4},
		{name: "empty", body: func() io.Reader { return strings.NewReader("") }, wantResult: "success"},
		{name: "upstream failure", serviceErr: errors.New("metadata failed"), wantResult: "error"},
		{name: "short stream", body: func() io.Reader { return strings.NewReader("data") }, size: 8, wantResult: "error", wantBytes: 4, wantPanic: true},
		{name: "EOF verification failure", body: func() io.Reader { return io.MultiReader(strings.NewReader("data"), failedReader{}) }, size: 4, wantResult: "error", wantBytes: 3, wantPanic: true},
		{name: "client write failure", body: func() io.Reader { return strings.NewReader("data") }, size: 4, writerFails: true, wantResult: "error", wantBytes: 2, wantPanic: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			objects := mocks.NewObjectServiceMock(mc)
			result := model.GetResult{}
			version := ""
			if tt.body != nil {
				version = "v1"
				result = model.GetResult{Body: io.NopCloser(tt.body()), Size: tt.size, VersionID: version}
			}
			objects.GetMock.Expect(minimock.AnyContext, "bucket", "key").Return(result, tt.serviceErr)
			recorder := mocks.NewAccessRecorderMock(mc)
			recorder.RecordMock.Set(func(_ context.Context, e access.Event) {
				require.Equal(t, access.Event{Bucket: "bucket", ObjectKey: "key", VersionID: version, Operation: "read", Origin: "client", Result: tt.wantResult, Bytes: tt.wantBytes}, e)
			})
			response := httptest.NewRecorder()
			var writer http.ResponseWriter = response
			if tt.writerFails {
				writer = &failedResponse{ResponseWriter: response}
			}
			handler := httpapi.NewHandler(objects, recorder)
			call := func() { handler.ServeHTTP(writer, httptest.NewRequest(http.MethodGet, "/objects/bucket/key", nil)) }
			if tt.wantPanic {
				require.PanicsWithValue(t, http.ErrAbortHandler, call)
			} else {
				require.NotPanics(t, call)
			}
			require.Equal(t, uint64(1), recorder.RecordAfterCounter())
		})
	}
}

type failedResponse struct{ http.ResponseWriter }

func (w *failedResponse) Write(p []byte) (int, error) {
	n := min(2, len(p))
	_, _ = w.ResponseWriter.Write(p[:n])
	return n, io.ErrClosedPipe
}

func TestPutAccessOutcome(t *testing.T) {
	logger.SetNopLogger()
	for _, tt := range []struct {
		name              string
		length            int64
		consume           int64
		serviceErr        error
		cancelAfterCommit bool
		wantResult        string
		wantVersion       string
	}{
		{name: "known length", length: 4, consume: 4, wantResult: "success", wantVersion: "v1"},
		{name: "chunked", length: -1, consume: 4, wantResult: "success", wantVersion: "v1"},
		{name: "partial failed upload", length: 99, consume: 2, serviceErr: errors.New("storage failed"), wantResult: "error"},
		{name: "commit survives disconnect", length: 4, consume: 4, cancelAfterCommit: true, wantResult: "success", wantVersion: "v1"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			objects := mocks.NewObjectServiceMock(mc)
			objects.PutMock.Set(func(_ context.Context, bucket, key string, body io.Reader, _ *int64, _ string) (model.PutResult, error) {
				require.Equal(t, "bucket", bucket)
				require.Equal(t, "key", key)
				n, err := io.CopyN(io.Discard, body, tt.consume)
				require.NoError(t, err)
				require.Equal(t, tt.consume, n)
				if tt.cancelAfterCommit {
					cancel()
				}
				return model.PutResult{VersionID: tt.wantVersion}, tt.serviceErr
			})
			recorder := mocks.NewAccessRecorderMock(mc)
			recorder.RecordMock.Set(func(_ context.Context, e access.Event) {
				require.Equal(t, access.Event{Bucket: "bucket", ObjectKey: "key", VersionID: tt.wantVersion, Operation: "write", Origin: "client", Result: tt.wantResult, Bytes: tt.consume}, e)
			})
			req := httptest.NewRequest(http.MethodPut, "/objects/bucket/key", strings.NewReader("data")).WithContext(ctx)
			req.ContentLength = tt.length
			httpapi.NewHandler(objects, recorder).ServeHTTP(httptest.NewRecorder(), req)
			require.Equal(t, uint64(1), recorder.RecordAfterCounter())
		})
	}
}

func TestNonObjectRequestsDoNotRecordAccess(t *testing.T) {
	for _, tt := range []struct{ method, path string }{
		{http.MethodGet, "/healthz"}, {http.MethodGet, "/swagger/doc.json"}, {http.MethodGet, "/objects/bucket/"}, {http.MethodPut, "/bad/path"}, {http.MethodDelete, "/objects/bucket/key"},
	} {
		t.Run(tt.method+tt.path, func(t *testing.T) {
			mc := minimock.NewController(t)
			objects := mocks.NewObjectServiceMock(mc)
			recorder := mocks.NewAccessRecorderMock(mc)
			httpapi.NewHandler(objects, recorder).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(tt.method, tt.path, nil))
			require.Zero(t, objects.GetAfterCounter())
			require.Zero(t, objects.PutAfterCounter())
			require.Zero(t, recorder.RecordAfterCounter())
		})
	}
}
