package httpapi_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	domainerrors "github.com/alesplll/opens3-rebac/services/gateway/internal/errors/domain_errors"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/handler/httpapi"
	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	"github.com/alesplll/opens3-rebac/services/gateway/pkg/mocks"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestPutObject(t *testing.T) {
	logger.SetNopLogger()
	type args struct {
		path, contentType string
		length            int64
	}
	type mockBuilder func(*minimock.Controller, *http.Request) *mocks.ObjectServiceMock
	size := int64(4)
	tests := []struct {
		name       string
		args       args
		wantStatus int
		wantBody   string
		wantCalls  uint64
		buildMock  mockBuilder
	}{
		{
			name:       "known size and explicit content type",
			args:       args{"/objects/bucket/nested/key", "text/plain", 4},
			wantStatus: http.StatusOK,
			wantCalls:  1,
			buildMock: func(mc *minimock.Controller, r *http.Request) *mocks.ObjectServiceMock {
				objects := mocks.NewObjectServiceMock(mc)
				objects.PutMock.Expect(r.Context(), "bucket", "nested/key", r.Body, &size, "text/plain").Return(model.PutResult{ETag: "etag", VersionID: "version"}, nil)
				return objects
			},
		},
		{
			name:       "unknown size and default content type",
			args:       args{"/objects/bucket/key", "", -1},
			wantStatus: http.StatusOK,
			wantCalls:  1,
			buildMock: func(mc *minimock.Controller, r *http.Request) *mocks.ObjectServiceMock {
				objects := mocks.NewObjectServiceMock(mc)
				objects.PutMock.Expect(r.Context(), "bucket", "key", r.Body, nil, "application/octet-stream").Return(model.PutResult{ETag: "etag", VersionID: "version"}, nil)
				return objects
			},
		},
		{
			name:       "invalid path",
			args:       args{"/objects/bucket/", "", 4},
			wantStatus: http.StatusNotFound,
			wantBody:   "404 page not found\n",
			buildMock: func(mc *minimock.Controller, _ *http.Request) *mocks.ObjectServiceMock {
				return mocks.NewObjectServiceMock(mc)
			},
		},
		{
			name:       "service error is mapped",
			args:       args{"/objects/bucket/key", "", 4},
			wantStatus: http.StatusNotFound,
			wantBody:   "bucket not found\n",
			wantCalls:  1,
			buildMock: func(mc *minimock.Controller, r *http.Request) *mocks.ObjectServiceMock {
				objects := mocks.NewObjectServiceMock(mc)
				objects.PutMock.Expect(r.Context(), "bucket", "key", r.Body, &size, "application/octet-stream").Return(model.PutResult{}, domainerrors.ErrBucketNotFound)
				return objects
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			request := httptest.NewRequest(http.MethodPut, tt.args.path, strings.NewReader("data"))
			request.ContentLength = tt.args.length
			request.Header.Set("Content-Type", tt.args.contentType)
			objects := tt.buildMock(mc, request)
			response := httptest.NewRecorder()
			httpapi.NewHandler(objects).ServeHTTP(response, request)
			require.Equal(t, tt.wantStatus, response.Code)
			require.Equal(t, tt.wantBody, response.Body.String())
			require.Equal(t, tt.wantCalls, objects.PutAfterCounter())
			require.Zero(t, objects.GetAfterCounter())
			if tt.wantStatus == http.StatusOK {
				require.Equal(t, "\"etag\"", response.Header().Get("ETag"))
				require.Equal(t, "version", response.Header().Get("X-Object-Version-Id"))
			}
		})
	}
}
