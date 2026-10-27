package decoder

import (
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	events_v1 "github.com/alesplll/opens3-rebac/shared/pkg/go/events/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// A missing timestamp stays zero so validation rejects it; AsTime would
// turn nil into the Unix epoch.
func occurredAt(ts *timestamppb.Timestamp) time.Time {
	if ts == nil {
		return time.Time{}
	}
	return ts.AsTime()
}

// Unknown origins map to "" and fail validation.
func origin(o events_v1.Origin) string {
	switch o {
	case events_v1.Origin_ORIGIN_CLIENT:
		return model.OriginClient
	case events_v1.Origin_ORIGIN_MIGRATION:
		return model.OriginMigration
	default:
		return ""
	}
}
