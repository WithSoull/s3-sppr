package admin_test

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/access"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/admin"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	"github.com/gojuno/minimock/v3"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const userID = "11111111-1111-4111-8111-111111111111"
const commandID = "22222222-2222-4222-8222-222222222222"
const planID = "33333333-3333-4333-8333-333333333333"
const recID = "44444444-4444-4444-8444-444444444444"

func accessToken() string {
	return "header." + base64.RawURLEncoding.EncodeToString([]byte(fmt.Sprintf(`{"user_id":%q,"token_type":"access","exp":%d}`, userID, time.Now().Add(time.Hour).Unix()))) + ".signature"
}
func TestLoginLogout(t *testing.T) {
	token := accessToken()
	cases := []struct {
		name, method, path, origin string
		build                      func(*mocks.AuthClientMock, *mocks.AuthorizerMock)
		want                       int
		cookie                     bool
		clear                      bool
	}{
		{name: "login form", method: "GET", path: "/admin/login", want: 200},
		{name: "login", method: "POST", path: "/admin/login", build: func(a *mocks.AuthClientMock, z *mocks.AuthorizerMock) {
			a.LoginMock.Expect(minimock.AnyContext, "admin@example.test", "secret").Return(token, nil)
			z.AuthorizeMock.Set(func(r *http.Request) (string, error) {
				require.Equal(t, "Bearer "+token, r.Header.Get("Authorization"))
				return userID, nil
			})
		}, want: 303, cookie: true},
		{name: "invalid credentials", method: "POST", path: "/admin/login", build: func(a *mocks.AuthClientMock, z *mocks.AuthorizerMock) {
			a.LoginMock.Return("", status.Error(codes.Unauthenticated, "bad"))
		}, want: 401},
		{name: "Auth unavailable", method: "POST", path: "/admin/login", build: func(a *mocks.AuthClientMock, z *mocks.AuthorizerMock) {
			a.LoginMock.Return("", status.Error(codes.Unavailable, "down"))
		}, want: 503},
		{name: "valid credentials without admin", method: "POST", path: "/admin/login", build: func(a *mocks.AuthClientMock, z *mocks.AuthorizerMock) {
			a.LoginMock.Return(token, nil)
			z.AuthorizeMock.Return("", model.ErrAccessDenied)
		}, want: 403},
		{name: "AuthZ unavailable", method: "POST", path: "/admin/login", build: func(a *mocks.AuthClientMock, z *mocks.AuthorizerMock) {
			a.LoginMock.Return(token, nil)
			z.AuthorizeMock.Return("", model.ErrUpstreamUnavailable)
		}, want: 503},
		{name: "foreign login origin", method: "POST", path: "/admin/login", origin: "https://foreign.test", want: 403},
		{name: "logout", method: "POST", path: "/admin/logout", want: 303, clear: true},
		{name: "logout foreign origin", method: "POST", path: "/admin/logout", origin: "https://foreign.test", want: 403},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			a := mocks.NewAuthClientMock(mc)
			z := mocks.NewAuthorizerMock(mc)
			if tt.build != nil {
				tt.build(a, z)
			}
			h := admin.NewHandler(mocks.NewPlannerServiceMock(mc), mocks.NewMigrationServiceMock(mc), mocks.NewFormCommandsMock(mc), a, z)
			r := httptest.NewRequest(tt.method, "https://sppr.test"+tt.path, strings.NewReader("email=admin%40example.test&password=secret&command_id="+commandID))
			r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			r.Header.Set("Origin", tt.origin)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			require.Equal(t, tt.want, w.Code, w.Body.String())
			require.Equal(t, "no-store", w.Header().Get("Cache-Control"))
			require.NotContains(t, w.Body.String(), "<nav>")
			require.NotContains(t, w.Body.String(), `action="/admin/logout"`)
			if tt.cookie || tt.clear {
				cookies := w.Result().Cookies()
				require.Len(t, cookies, 1)
				c := cookies[0]
				require.Equal(t, access.CookieName, c.Name)
				require.True(t, c.HttpOnly)
				require.True(t, c.Secure)
				require.Equal(t, http.SameSiteStrictMode, c.SameSite)
				require.Equal(t, "/", c.Path)
				if tt.clear {
					require.Equal(t, -1, c.MaxAge)
				} else {
					require.Equal(t, token, c.Value)
					require.True(t, c.MaxAge > 0)
					require.Equal(t, "/admin", w.Header().Get("Location"))
				}
			}
			if tt.name == "login form" {
				require.Contains(t, w.Body.String(), `name="command_id"`)
				require.Contains(t, w.Body.String(), `type="password"`)
			}
		})
	}
}
func TestPanelAccess(t *testing.T) {
	for _, tt := range []struct {
		name string
		err  error
		want int
	}{{"expired", access.ErrUnauthenticated, 303}, {"forbidden", model.ErrAccessDenied, 403}, {"unavailable", model.ErrUpstreamUnavailable, 503}} {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			z := mocks.NewAuthorizerMock(mc)
			z.AuthorizeMock.Return("", tt.err)
			h := admin.NewHandler(mocks.NewPlannerServiceMock(mc), mocks.NewMigrationServiceMock(mc), nil, nil, z)
			r := httptest.NewRequest("GET", "/admin", nil)
			w := httptest.NewRecorder()
			h.ServeHTTP(w, r)
			require.Equal(t, tt.want, w.Code)
			require.NotContains(t, w.Body.String(), "<nav>")
			require.NotContains(t, w.Body.String(), `action="/admin/logout"`)
			if tt.want == 303 {
				require.Equal(t, "/admin/login", w.Header().Get("Location"))
			}
		})
	}
}
func post(path string, fields url.Values) *http.Request {
	r := httptest.NewRequest("POST", "https://sppr.test"+path, strings.NewReader(fields.Encode()))
	r.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	return r
}
func assertUUID(t *testing.T, s string) { t.Helper(); _, err := uuid.Parse(s); require.NoError(t, err) }
