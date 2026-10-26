package placement

import (
	"context"
	"regexp"

	"github.com/alesplll/opens3-rebac/services/metadata/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/sys/validate"
	"github.com/google/uuid"
)

var md5Pattern = regexp.MustCompile(`^[0-9a-f]{32}$`)

func uuidField(name, value string) validate.Condition {
	return func(context.Context) error {
		if _, err := uuid.Parse(value); err != nil {
			return validate.NewValidationErrors(name + " must be a UUID")
		}
		return nil
	}
}

func storageClassField(name string, value model.StorageClass) validate.Condition {
	return func(context.Context) error {
		if !value.Valid() {
			return validate.NewValidationErrors(name + " must be HOT or COLD")
		}
		return nil
	}
}
