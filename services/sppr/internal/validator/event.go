package validator

import (
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	access "github.com/alesplll/opens3-rebac/shared/pkg/go-kit/events/access"
)

func AccessEvent(e model.AccessEvent) error { return access.Validate(e) }
