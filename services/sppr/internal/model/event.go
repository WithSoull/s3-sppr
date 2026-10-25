package model

import access "github.com/alesplll/opens3-rebac/shared/pkg/go-kit/events/access"

const EventSchemaVersion = access.SchemaVersion

type AccessEvent = access.Event

type Delivery struct {
	Topic     string
	Partition int32
	Offset    int64
}

type IngestResult struct {
	Duplicate bool
	Missing   int64
}
