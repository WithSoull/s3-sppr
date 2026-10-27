package service

import (
	"context"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

// FormCommands journals legacy executor settings/pin operations atomically with
// their effects. Planner commands continue to use PlannerService.Execute.
type FormCommands interface {
	Execute(context.Context, model.CommandRequest, string, func(context.Context) error) (model.Command, error)
	Get(context.Context, string) (model.Command, error)
	List(context.Context, string) (model.CommandPage, error)
}
