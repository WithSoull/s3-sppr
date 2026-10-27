package model

import "io"

type StorageClass string

const (
	StorageClassHot  StorageClass = "HOT"
	StorageClassCold StorageClass = "COLD"
)

type ObjectMeta struct {
	BlobID       string
	VersionID    string
	Size         int64
	ETag         string
	ContentType  string
	StorageClass StorageClass
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

// ByteRange is one range of a Range header before the object size is known.
// Without Suffix it covers First..Last inclusive, Last < 0 meaning the end of the object;
// with Suffix > 0 it covers the last Suffix bytes. A non-empty IfRangeETag
// (from If-Range) applies the range only while it equals the current ETag.
type ByteRange struct {
	First       int64
	Last        int64
	Suffix      int64
	IfRangeETag string
}

// Resolve returns the offset and length of the range in an object of the given size.
// ok is false when the range does not overlap the object.
func (r ByteRange) Resolve(size int64) (offset, length int64, ok bool) {
	if r.Suffix > 0 {
		if size == 0 {
			return 0, 0, false
		}
		length = min(r.Suffix, size)
		return size - length, length, true
	}
	if r.First >= size {
		return 0, 0, false
	}
	last := r.Last
	if last < 0 || last >= size {
		last = size - 1
	}
	return r.First, last - r.First + 1, true
}

// ContentRange is the part of the object a range response carries.
type ContentRange struct {
	Offset int64
	Length int64
}

// GetResult transfers ownership of Body to the caller, which must close it.
// Body holds Range.Length bytes when Range is set and Size bytes otherwise.
type GetResult struct {
	Body        io.ReadCloser
	Size        int64
	Range       *ContentRange
	ETag        string
	VersionID   string
	ContentType string
}
