package tests

import (
	"context"
	"errors"
	"testing"

	objectservice "github.com/alesplll/opens3-rebac/services/gateway/internal/service/object"
	"github.com/gojuno/minimock/v3"
	"github.com/stretchr/testify/require"
)

func TestDelete(t *testing.T) {
	upstreamErr := errors.New("upstream failed")
	tests := []struct {
		name    string
		metaErr error
	}{
		{name: "success"},
		{name: "metadata error", metaErr: upstreamErr},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			mc := minimock.NewController(t)
			m := newUpstreamMocks(mc)
			m.meta.DeleteObjectMock.Expect(ctx, "bucket", "nested/key").Return(tt.metaErr)
			err := objectservice.NewService(m.meta, m.storage(), m.access).Delete(ctx, "bucket", "nested/key")
			require.ErrorIs(t, err, tt.metaErr)
		})
	}
}
