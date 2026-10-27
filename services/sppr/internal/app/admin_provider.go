package app

import (
	"context"
	"fmt"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client/identity"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/access"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/tracing"
	authv1 "github.com/alesplll/opens3-rebac/shared/pkg/go/auth/v1"
	authzv1 "github.com/alesplll/opens3-rebac/shared/pkg/go/authz/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (s *serviceProvider) AdminAccess(ctx context.Context) (identity.AuthClient, access.Authorizer, error) {
	cfg := s.cfg.Admin
	if cfg == nil || !cfg.Enabled() {
		logger.Warn(ctx, "ADMIN_AUTH_ENABLED=false: administrator API is open; local tests only")
		return nil, access.AllowAll(), nil
	}
	dial := func(address, name string) (*grpc.ClientConn, error) {
		c, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithUnaryInterceptor(tracing.UnaryClientInterceptor(name)))
		if err != nil {
			return nil, fmt.Errorf("create %s client: %w", name, err)
		}
		s.closer.AddNamed(name+" gRPC", func(context.Context) error { return c.Close() })
		return c, nil
	}
	a, err := dial(cfg.AuthAddress(), "sppr.auth")
	if err != nil {
		return nil, nil, err
	}
	z, err := dial(cfg.AuthZAddress(), "sppr.authz")
	if err != nil {
		return nil, nil, err
	}
	auth := identity.NewAuth(authv1.NewAuthV1Client(a), cfg.AuthTimeout())
	return auth, access.NewAuthorizer(auth, identity.NewPermission(authzv1.NewPermissionServiceClient(z), cfg.AuthZTimeout())), nil
}
