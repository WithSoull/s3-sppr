package access_test

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/access"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const userID = "11111111-1111-4111-8111-111111111111"

func token(claims string) string {
	return "header." + base64.RawURLEncoding.EncodeToString([]byte(claims)) + ".signature"
}
func TestAuthorizer(t *testing.T) {
	good := token(fmt.Sprintf(`{"user_id":%q,"token_type":"access","exp":%d}`, userID, time.Now().Add(time.Hour).Unix()))
	cases := []struct {
		name, header, cookie string
		build                func(*mocks.AuthClientMock, *mocks.PermissionClientMock)
		want                 int
	}{
		{name: "no token", want: 401},
		{name: "malformed header does not fall back to cookie", header: "Basic bad", cookie: good, want: 401},
		{name: "bad token", header: "Bearer bad", build: func(a *mocks.AuthClientMock, z *mocks.PermissionClientMock) {
			a.ValidateMock.Expect(minimock.AnyContext, "bad").Return(status.Error(codes.Unauthenticated, "invalid"))
		}, want: 401},
		{name: "Auth down", header: "Bearer " + good, build: func(a *mocks.AuthClientMock, z *mocks.PermissionClientMock) {
			a.ValidateMock.Return(status.Error(codes.Unavailable, "down"))
		}, want: 503},
		{name: "Auth deadline", header: "Bearer " + good, build: func(a *mocks.AuthClientMock, z *mocks.PermissionClientMock) {
			a.ValidateMock.Return(status.Error(codes.DeadlineExceeded, "timeout"))
		}, want: 503},
		{name: "not admin", header: "Bearer " + good, build: func(a *mocks.AuthClientMock, z *mocks.PermissionClientMock) {
			a.ValidateMock.Return(nil)
			z.AdminMock.Expect(minimock.AnyContext, userID).Return(false, nil)
		}, want: 403},
		{name: "AuthZ down", header: "Bearer " + good, build: func(a *mocks.AuthClientMock, z *mocks.PermissionClientMock) {
			a.ValidateMock.Return(nil)
			z.AdminMock.Return(false, status.Error(codes.Unknown, "graph down"))
		}, want: 503},
		{name: "admin header", header: "Bearer " + good, build: func(a *mocks.AuthClientMock, z *mocks.PermissionClientMock) {
			a.ValidateMock.Expect(minimock.AnyContext, good).Return(nil)
			z.AdminMock.Expect(minimock.AnyContext, userID).Return(true, nil)
		}, want: 204},
		{name: "admin cookie", cookie: good, build: func(a *mocks.AuthClientMock, z *mocks.PermissionClientMock) {
			a.ValidateMock.Expect(minimock.AnyContext, good).Return(nil)
			z.AdminMock.Expect(minimock.AnyContext, userID).Return(true, nil)
		}, want: 204},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			a := mocks.NewAuthClientMock(mc)
			z := mocks.NewPermissionClientMock(mc)
			if tt.build != nil {
				tt.build(a, z)
			}
			req := httptest.NewRequest("GET", "http://sppr/api/planner/status", nil)
			if tt.header != "" {
				req.Header.Set("Authorization", tt.header)
			}
			if tt.cookie != "" {
				req.AddCookie(&http.Cookie{Name: access.CookieName, Value: tt.cookie})
			}
			w := httptest.NewRecorder()
			access.Middleware(access.NewAuthorizer(a, z), http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, userID, access.Initiator(r.Context()))
				w.WriteHeader(204)
			})).ServeHTTP(w, req)
			require.Equal(t, tt.want, w.Code)
			if tt.want != 204 {
				require.Contains(t, w.Body.String(), "ACCESS_DENIED")
			}
		})
	}
}
func TestInvalidValidatedClaims(t *testing.T) {
	cases := []struct{ name, claims string }{
		{"refresh", fmt.Sprintf(`{"user_id":%q,"token_type":"refresh","exp":%d}`, userID, time.Now().Add(time.Hour).Unix())},
		{"sub is not user id", fmt.Sprintf(`{"sub":%q,"token_type":"access","exp":%d}`, userID, time.Now().Add(time.Hour).Unix())},
		{"invalid uuid", fmt.Sprintf(`{"user_id":"alice","token_type":"access","exp":%d}`, time.Now().Add(time.Hour).Unix())},
		{"expired", fmt.Sprintf(`{"user_id":%q,"token_type":"access","exp":1}`, userID)},
		{"missing expiry", fmt.Sprintf(`{"user_id":%q,"token_type":"access"}`, userID)},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			a := mocks.NewAuthClientMock(mc)
			z := mocks.NewPermissionClientMock(mc)
			a.ValidateMock.Return(nil)
			r := httptest.NewRequest("GET", "/", nil)
			r.Header.Set("Authorization", "Bearer "+token(tt.claims))
			_, err := access.NewAuthorizer(a, z).Authorize(r)
			require.ErrorIs(t, err, access.ErrUnauthenticated)
		})
	}
}
func TestOrigin(t *testing.T) {
	cases := []struct {
		name, method, origin, referer string
		want                          bool
	}{
		{"GET", "GET", "https://foreign/", "", true}, {"CLI", "POST", "", "", true}, {"same origin", "POST", "https://sppr:8086", "", true}, {"foreign origin", "POST", "https://evil", "", false}, {"foreign referer", "POST", "", "https://evil/form", false}, {"both checked", "POST", "https://sppr:8086", "https://evil/form", false}, {"null origin", "POST", "null", "", false}, {"bad scheme", "POST", "file://sppr:8086", "", false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(tt.method, "https://sppr:8086/admin", nil)
			r.Header.Set("Origin", tt.origin)
			r.Header.Set("Referer", tt.referer)
			require.Equal(t, tt.want, access.SameOrigin(r))
		})
	}
}
