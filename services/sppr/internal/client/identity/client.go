// Package identity adapts Auth and AuthZ to administrator HTTP access.
package identity

import (
	"context"
	"time"

	authv1 "github.com/alesplll/opens3-rebac/shared/pkg/go/auth/v1"
	authzv1 "github.com/alesplll/opens3-rebac/shared/pkg/go/authz/v1"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/types/known/emptypb"
)

const AdminResource = "system:placement"

type AuthClient interface {
	Validate(context.Context, string) error
	Login(context.Context, string, string) (string, error)
}
type PermissionClient interface {
	Admin(context.Context, string) (bool, error)
}

type authClient struct {
	api     authv1.AuthV1Client
	timeout time.Duration
}

func NewAuth(api authv1.AuthV1Client, timeout time.Duration) AuthClient {
	return &authClient{api, timeout}
}
func (c *authClient) Validate(ctx context.Context, token string) error {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	md, _ := metadata.FromOutgoingContext(ctx)
	md = md.Copy()
	md.Set("authorization", "Bearer "+token)
	ctx = metadata.NewOutgoingContext(ctx, md)
	_, err := c.api.ValidateToken(ctx, &emptypb.Empty{})
	return err
}
func (c *authClient) Login(ctx context.Context, email, password string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	login, err := c.api.Login(ctx, &authv1.LoginRequest{Email: email, Password: password})
	if err != nil {
		return "", err
	}
	token, err := c.api.GetAccessToken(ctx, &authv1.GetAccessTokenRequest{RefreshToken: login.GetRefreshToken()})
	if err != nil {
		return "", err
	}
	return token.GetAccessToken(), nil
}

type permissionClient struct {
	api     authzv1.PermissionServiceClient
	timeout time.Duration
}

func NewPermission(api authzv1.PermissionServiceClient, timeout time.Duration) PermissionClient {
	return &permissionClient{api, timeout}
}
func (c *permissionClient) Admin(ctx context.Context, user string) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	res, err := c.api.Check(ctx, &authzv1.CheckRequest{Subject: "user:" + user, Action: authzv1.Action_ACTION_ADMIN, Object: AdminResource})
	if err != nil {
		return false, err
	}
	return res.GetAllowed(), nil
}
