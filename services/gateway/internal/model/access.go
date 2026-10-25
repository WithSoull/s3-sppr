package model

// Origin separates client demand from Migration Executor traffic.
type Origin int

const (
	OriginClient Origin = iota
	OriginMigration
)

type ObjectReadEvent struct {
	Bucket    string
	ObjectKey string
	VersionID string
	// Bytes is the length of the served range or of the whole object.
	Bytes  int64
	Origin Origin
}

type ObjectWrittenEvent struct {
	Bucket    string
	ObjectKey string
	VersionID string
	Bytes     int64
	Origin    Origin
}
