package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/adminseed"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/client/identity"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cfg := adminseed.FromEnv()
	user, err := adminseed.Seed(ctx, cfg, true)
	if err != nil {
		fmt.Fprintln(os.Stderr, "seed failed:", err)
		os.Exit(1)
	}
	fmt.Printf("Panel: http://localhost:%s/admin\nEmail: %s\nPassword: %s\nUser: %s\nADMIN resource: %s\n", adminseed.Env("SPPR_HTTP_PORT", "8086"), cfg.Email, cfg.Password, user.ID, identity.AdminResource)
}
