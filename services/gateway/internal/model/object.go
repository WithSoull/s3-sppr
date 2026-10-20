package model

import "io"

type ObjectMeta struct {
	BlobID      string
	VersionID   string
	Size        int64
	ETag        string
	ContentType string
}

type StoredObject struct {
	BlobID string
	ETag   string
	Size   int64
}

type PutResult struct {
	ETag      string
	VersionID string
}

// GetResult transfers ownership of Body to the caller, which must close it.
type GetResult struct {
	Body        io.ReadCloser
	Size        int64
	ETag        string
	VersionID   string
	ContentType string
}
