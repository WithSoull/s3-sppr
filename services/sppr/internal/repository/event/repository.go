package event

import (
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
)

type repo struct{ db db.Client }

func NewRepository(client db.Client) repository.EventRepository { return &repo{db: client} }
