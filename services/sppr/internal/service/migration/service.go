package migration

import (
	"github.com/alesplll/opens3-rebac/services/sppr/internal/repository"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/service"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/client/db"
)

const (
	maxActionsPerSubmit = 1000
	defaultPageSize     = 100
	maxPageSize         = 1000
)

type migrationService struct {
	actions  repository.ActionRepository
	settings repository.SettingsRepository
	pins     repository.PinRepository
	tx       db.TxManager
}

func NewService(
	actions repository.ActionRepository,
	settings repository.SettingsRepository,
	pins repository.PinRepository,
	tx db.TxManager,
) service.MigrationService {
	return &migrationService{actions: actions, settings: settings, pins: pins, tx: tx}
}
