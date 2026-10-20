package httpapi_test

import (
	"io"
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
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The body tracks ownership; service interactions use generated mocks.
type trackedBody struct {
	io.Reader
	closes int
}

func (b *trackedBody) Close() error { b.closes++; return nil }

func TestGetObject(t *testing.T) {
	logger.SetNopLogger()
	type args struct{ path string }
	type mockBuilder func(*minimock.Controller, *http.Request, *trackedBody) *mocks.ObjectServiceMock
	tests := []struct {
		name            string
		args            args
		wantStatus      int
		wantBody        string
		wantCloses      int
		wantCalls       uint64
		buildMock       mockBuilder
		wantContentType string
	}{
		{
			name:            "success",
			wantCalls:       1,
			args:            args{"/objects/bucket/nested/key"},
			wantStatus:      http.StatusOK,
			wantBody:        "data",
			wantCloses:      1,
			wantContentType: "text/plain",
			buildMock: func(mc *minimock.Controller, r *http.Request, body *trackedBody) *mocks.ObjectServiceMock {
				objects := mocks.NewObjectServiceMock(mc)
				objects.GetMock.Expect(r.Context(), "bucket", "nested/key").Return(model.GetResult{Body: body, Size: 4, ETag: "etag", VersionID: "version", ContentType: "text/plain"}, nil)
				return objects
			},
		},
		{
			name:            "empty content type",
			wantCalls:       1,
			args:            args{"/objects/bucket/nested/key"},
			wantStatus:      http.StatusOK,
			wantBody:        "data",
			wantCloses:      1,
			wantContentType: "application/octet-stream",
			buildMock: func(mc *minimock.Controller, r *http.Request, body *trackedBody) *mocks.ObjectServiceMock {
				objects := mocks.NewObjectServiceMock(mc)
				objects.GetMock.Expect(r.Context(), "bucket", "nested/key").Return(model.GetResult{Body: body, Size: 4, ETag: "etag", VersionID: "version", ContentType: ""}, nil)
				return objects
			},
		},
		{
			name:       "invalid path",
			args:       args{"/objects/bucket/"},
			wantStatus: http.StatusNotFound,
			wantBody:   "404 page not found\n",
			buildMock: func(mc *minimock.Controller, _ *http.Request, _ *trackedBody) *mocks.ObjectServiceMock {
				return mocks.NewObjectServiceMock(mc)
			},
		},
		{
			name:       "missing object",
			wantCalls:  1,
			args:       args{"/objects/bucket/key"},
			wantStatus: http.StatusNotFound,
			wantBody:   "Not Found\n",
			buildMock: func(mc *minimock.Controller, r *http.Request, _ *trackedBody) *mocks.ObjectServiceMock {
				objects := mocks.NewObjectServiceMock(mc)
				objects.GetMock.Expect(r.Context(), "bucket", "key").Return(model.GetResult{}, status.Error(codes.NotFound, "private detail"))
				return objects
			},
		},
		{
			name:       "empty storage stream",
			wantCalls:  1,
			args:       args{"/objects/bucket/key"},
			wantStatus: http.StatusBadGateway,
			wantBody:   "storage returned an empty stream\n",
			buildMock: func(mc *minimock.Controller, r *http.Request, _ *trackedBody) *mocks.ObjectServiceMock {
				objects := mocks.NewObjectServiceMock(mc)
				objects.GetMock.Expect(r.Context(), "bucket", "key").Return(model.GetResult{}, domainerrors.ErrEmptyStorageStream)
				return objects
			},
		},
		{
			name:       "upstream deadline",
			wantCalls:  1,
			args:       args{"/objects/bucket/key"},
			wantStatus: http.StatusGatewayTimeout,
			wantBody:   "Gateway Timeout\n",
			buildMock: func(mc *minimock.Controller, r *http.Request, _ *trackedBody) *mocks.ObjectServiceMock {
				objects := mocks.NewObjectServiceMock(mc)
				objects.GetMock.Expect(r.Context(), "bucket", "key").Return(model.GetResult{}, status.Error(codes.DeadlineExceeded, "private detail"))
				return objects
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			request := httptest.NewRequest(http.MethodGet, tt.args.path, nil)
			body := &trackedBody{Reader: strings.NewReader("data")}
			objects := tt.buildMock(mc, request, body)
			response := httptest.NewRecorder()
			httpapi.NewHandler(objects).ServeHTTP(response, request)
			require.Equal(t, tt.wantStatus, response.Code)
			require.Equal(t, tt.wantBody, response.Body.String())
			require.Equal(t, tt.wantCloses, body.closes)
			require.Zero(t, objects.PutAfterCounter())
			if tt.wantStatus == http.StatusOK {
				require.Equal(t, "4", response.Header().Get("Content-Length"))
				require.Equal(t, tt.wantContentType, response.Header().Get("Content-Type"))
				require.Equal(t, "nosniff", response.Header().Get("X-Content-Type-Options"))
				require.Equal(t, "\"etag\"", response.Header().Get("ETag"))
				require.Equal(t, "version", response.Header().Get("X-Object-Version-Id"))
			}
			require.Equal(t, tt.wantCalls, objects.GetAfterCounter())
		})
	}
}
