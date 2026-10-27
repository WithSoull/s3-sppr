package domainerrors

import "errors"

var (
	ErrBucketNotFound      = errors.New("bucket not found")
	ErrBodyRead            = errors.New("could not read request body")
	ErrEmptyStorageStream  = errors.New("storage returned an empty stream")
	ErrInsufficientStorage = errors.New("insufficient storage")
	ErrMetadataOverloaded  = errors.New("metadata is overloaded")
	ErrBlobMissing         = errors.New("object data is missing in storage")
	ErrUnknownStorageClass = errors.New("unknown storage class")
	ErrRangeNotSatisfiable = errors.New("range not satisfiable")
)

// RangeNotSatisfiableError carries the object size for the Content-Range of a 416 response.
type RangeNotSatisfiableError struct{ Size int64 }

func (e *RangeNotSatisfiableError) Error() string        { return ErrRangeNotSatisfiable.Error() }
func (e *RangeNotSatisfiableError) Is(target error) bool { return target == ErrRangeNotSatisfiable }

type BodyReadError struct{ Err error }

func (e *BodyReadError) Error() string        { return e.Err.Error() }
func (e *BodyReadError) Unwrap() error        { return e.Err }
func (e *BodyReadError) Is(target error) bool { return target == ErrBodyRead }
