package kafka

import "github.com/alesplll/opens3-rebac/services/sppr/internal/model"

type ObjectReadDecoder interface {
	Decode(data []byte) (model.AccessEvent, error)
}

type ObjectWrittenDecoder interface {
	Decode(data []byte) (model.AccessEvent, error)
}
