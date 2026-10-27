// Package adminseed provisions the explicitly requested local planner test user.
package adminseed

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client/identity"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/handler/access"
	authv1 "github.com/alesplll/opens3-rebac/shared/pkg/go/auth/v1"
	authzv1 "github.com/alesplll/opens3-rebac/shared/pkg/go/authz/v1"
	userv1 "github.com/alesplll/opens3-rebac/shared/pkg/go/user/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct{ Users, Auth, AuthZ, Email, Password string }

func Env(key, fallback string) string {
	if s := os.Getenv(key); s != "" {
		return s
	}
	return fallback
}
func FromEnv() Config {
	return Config{Env("USERS_GRPC_ADDR", "localhost:50054"), Env("AUTH_GRPC_ADDR", "localhost:50050"), Env("AUTHZ_GRPC_ADDR", "localhost:50051"), Env("PLANNER_ADMIN_EMAIL", "planner-admin@example.test"), Env("PLANNER_ADMIN_PASSWORD", "PlannerAdmin123!")}
}

type User struct{ ID, Token string }

func Seed(ctx context.Context, c Config, admin bool) (User, error) {
	dial := func(address string) (*grpc.ClientConn, error) {
		return grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithDefaultCallOptions(grpc.WaitForReady(true)))
	}
	users, err := dial(c.Users)
	if err != nil {
		return User{}, err
	}
	defer users.Close()
	auth, err := dial(c.Auth)
	if err != nil {
		return User{}, err
	}
	defer auth.Close()
	u := userv1.NewUserV1Client(users)
	// ValidateCredentials makes repeated seeding safe without resetting passwords.
	call, cancel := context.WithTimeout(ctx, 5*time.Second)
	creds, err := u.ValidateCredentials(call, &userv1.ValidateCredentialsRequest{Email: c.Email, Password: c.Password})
	cancel()
	if err != nil {
		return User{}, fmt.Errorf("Users.ValidateCredentials: %w", err)
	}
	id := creds.GetUserId()
	if !creds.GetValid() {
		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		created, err := u.Create(call, &userv1.CreateRequest{UserInfo: &userv1.UserInfo{Name: "Planner test user", Email: c.Email}, Password: c.Password, PasswordConfirm: c.Password})
		cancel()
		if err != nil {
			return User{}, fmt.Errorf("Users.Create (existing email with a different password is not overwritten): %w", err)
		}
		id = created.GetId()
	}
	a := identity.NewAuth(authv1.NewAuthV1Client(auth), 5*time.Second)
	token, err := a.Login(ctx, c.Email, c.Password)
	if err != nil {
		return User{}, fmt.Errorf("Auth.Login/GetAccessToken: %w", err)
	}
	if err := a.Validate(ctx, token); err != nil {
		return User{}, err
	}
	claims, err := access.ValidatedClaims(token)
	if err != nil || claims.UserID != id {
		return User{}, fmt.Errorf("Auth returned inconsistent user_id")
	}
	if admin {
		z, err := dial(c.AuthZ)
		if err != nil {
			return User{}, err
		}
		defer z.Close()
		call, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		result, err := authzv1.NewPermissionServiceClient(z).WriteTuple(call, &authzv1.WriteTupleRequest{Subject: "user:" + id, Relation: authzv1.Relation_RELATION_HAS_PERMISSION, Object: identity.AdminResource, Level: authzv1.PermissionLevel_PERMISSION_LEVEL_ADMIN, Actor: "planner-admin-seed"})
		if err != nil {
			return User{}, fmt.Errorf("AuthZ.WriteTuple: %w", err)
		}
		if !result.GetSuccess() {
			return User{}, fmt.Errorf("AuthZ.WriteTuple refused the tuple")
		}
	}
	return User{id, token}, nil
}
