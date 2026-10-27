package identity_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client/identity"
	"github.com/alesplll/opens3-rebac/services/sppr/pkg/mocks"
	authv1 "github.com/alesplll/opens3-rebac/shared/pkg/go/auth/v1"
	authzv1 "github.com/alesplll/opens3-rebac/shared/pkg/go/authz/v1"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

func TestValidateMetadata(t *testing.T) {
	mc := minimock.NewController(t)
	rpc := mocks.NewAuthV1ClientMock(mc)
	rpc.ValidateTokenMock.Set(func(ctx context.Context, req *emptypb.Empty, opts ...grpc.CallOption) (*emptypb.Empty, error) {
		md, ok := metadata.FromOutgoingContext(ctx)
		require.True(t, ok)
		require.Equal(t, []string{"Bearer access-token"}, md.Get("authorization"))
		deadline, ok := ctx.Deadline()
		require.True(t, ok)
		require.WithinDuration(t, time.Now().Add(time.Second), deadline, 100*time.Millisecond)
		return &emptypb.Empty{}, nil
	})
	require.NoError(t, identity.NewAuth(rpc, time.Second).Validate(context.Background(), "access-token"))
}
func TestLoginRPC(t *testing.T) {
	boom := errors.New("rpc down")
	for _, tt := range []struct {
		name                string
		loginErr, accessErr error
		want                string
	}{{name: "refresh exchanged for access", want: "access"}, {name: "login failure stops exchange", loginErr: boom}, {name: "exchange failure", accessErr: boom}} {
		t.Run(tt.name, func(t *testing.T) {
			mc := minimock.NewController(t)
			rpc := mocks.NewAuthV1ClientMock(mc)
			rpc.LoginMock.Expect(minimock.AnyContext, &authv1.LoginRequest{Email: "a@b.test", Password: "secret"}).Return(&authv1.LoginResponse{RefreshToken: "refresh"}, tt.loginErr)
			if tt.loginErr == nil {
				rpc.GetAccessTokenMock.Expect(minimock.AnyContext, &authv1.GetAccessTokenRequest{RefreshToken: "refresh"}).Return(&authv1.GetAccessTokenResponse{AccessToken: "access"}, tt.accessErr)
			}
			token, err := identity.NewAuth(rpc, time.Second).Login(context.Background(), "a@b.test", "secret")
			if tt.loginErr != nil || tt.accessErr != nil {
				require.ErrorIs(t, err, boom)
			} else {
				require.NoError(t, err)
			}
			require.Equal(t, tt.want, token)
		})
	}
}
func TestAdminRPC(t *testing.T) {
	mc := minimock.NewController(t)
	rpc := mocks.NewPermissionServiceClientMock(mc)
	rpc.CheckMock.Expect(minimock.AnyContext, &authzv1.CheckRequest{Subject: "user:uuid", Action: authzv1.Action_ACTION_ADMIN, Object: "system:placement"}).Return(&authzv1.CheckResponse{Allowed: true}, nil)
	allowed, err := identity.NewPermission(rpc, time.Second).Admin(context.Background(), "uuid")
	require.NoError(t, err)
	require.True(t, allowed)
}
