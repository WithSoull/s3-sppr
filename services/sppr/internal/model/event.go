package model

import (
	"errors"
	"time"
)

// ErrRejected marks an event that retrying cannot store. Its offset is
// committed, and the skipped sequence remains visible as a gap.
var ErrRejected = errors.New("access event rejected")

const (
	OperationRead  = "read"
	OperationWrite = "write"

	OriginClient    = "client"
	OriginMigration = "migration"

	// Gateway publishes only successful operations.
	ResultSuccess = "success"
)

type AccessEvent struct {
	GatewayID  string
	StreamID   string
	Sequence   int64
	OccurredAt time.Time
	Bucket     string
	ObjectKey  string
	VersionID  string
	Operation  string
	Origin     string
	Result     string
	Bytes      int64
	// Payload is the original Kafka value; a redelivery must match its hash.
	Payload []byte
}

func (e AccessEvent) IsDemand() bool {
	return e.Operation == OperationRead && e.Origin == OriginClient && e.Result == ResultSuccess
}

type Delivery struct {
	Topic     string
	Partition int32
	Offset    int64
}

type IngestResult struct {
	Duplicate bool
	Missing   int64
}

// StreamHeartbeat is a barrier, not a demand event.
type StreamHeartbeat struct {
	GatewayID, StreamID string
	Sequence            int64
	At                  time.Time
	Closing             bool
	Payload             []byte
}
