package validator_test

import (
	"strings"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/validator"
	"github.com/stretchr/testify/require"
)

func TestAccessEvent(t *testing.T) {
	valid := model.AccessEvent{GatewayID: "gateway-1", StreamID: "4f3c2a1e-0d6b-4a7e-9f10-2b3c4d5e6f70", Sequence: 1,
		OccurredAt: time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC), Bucket: "bucket", ObjectKey: "a/b",
		VersionID: "v1", Operation: model.OperationRead, Origin: model.OriginClient, Result: model.ResultSuccess}
	tests := []struct {
		name    string
		change  func(*model.AccessEvent)
		wantErr bool
	}{
		{name: "valid", change: func(*model.AccessEvent) {}},
		{name: "migration write", change: func(e *model.AccessEvent) { e.Operation, e.Origin = model.OperationWrite, model.OriginMigration }},
		{name: "empty gateway", change: func(e *model.AccessEvent) { e.GatewayID = "" }, wantErr: true},
		{name: "slash in stream", change: func(e *model.AccessEvent) { e.StreamID = "a/b" }, wantErr: true},
		{name: "zero sequence", change: func(e *model.AccessEvent) { e.Sequence = 0 }, wantErr: true},
		{name: "missing time", change: func(e *model.AccessEvent) { e.OccurredAt = time.Time{} }, wantErr: true},
		{name: "empty key", change: func(e *model.AccessEvent) { e.ObjectKey = "" }, wantErr: true},
		{name: "key too long", change: func(e *model.AccessEvent) { e.ObjectKey = strings.Repeat("k", 1025) }, wantErr: true},
		{name: "NUL in bucket", change: func(e *model.AccessEvent) { e.Bucket = "b\x00" }, wantErr: true},
		{name: "missing version", change: func(e *model.AccessEvent) { e.VersionID = "" }, wantErr: true},
		{name: "unknown origin", change: func(e *model.AccessEvent) { e.Origin = "" }, wantErr: true},
		{name: "not success", change: func(e *model.AccessEvent) { e.Result = "error" }, wantErr: true},
		{name: "negative bytes", change: func(e *model.AccessEvent) { e.Bytes = -1 }, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := valid
			tt.change(&e)

			err := validator.AccessEvent(e)

			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
		})
	}
}
