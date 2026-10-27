package decoder

import (
	"fmt"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/converter/kafka"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	events_v1 "github.com/alesplll/opens3-rebac/shared/pkg/go/events/v1"
	"google.golang.org/protobuf/proto"
)

type objectWrittenDecoder struct{}

func NewObjectWrittenDecoder() kafka.ObjectWrittenDecoder {
	return &objectWrittenDecoder{}
}

func (d *objectWrittenDecoder) Decode(data []byte) (model.AccessEvent, error) {
	var pb events_v1.ObjectWritten
	if err := proto.Unmarshal(data, &pb); err != nil {
		return model.AccessEvent{}, fmt.Errorf("failed to unmarshal protobuf: %w", err)
	}

	return model.AccessEvent{
		GatewayID:  pb.GetPosition().GetGatewayId(),
		StreamID:   pb.GetPosition().GetStreamId(),
		Sequence:   pb.GetPosition().GetSequence(),
		OccurredAt: occurredAt(pb.GetOccurredAt()),
		Bucket:     pb.GetBucket(),
		ObjectKey:  pb.GetObjectKey(),
		VersionID:  pb.GetVersionId(),
		Operation:  model.OperationWrite,
		Origin:     origin(pb.GetOrigin()),
		Result:     model.ResultSuccess,
		Bytes:      pb.GetBytes(),
		Payload:    data,
	}, nil
}
