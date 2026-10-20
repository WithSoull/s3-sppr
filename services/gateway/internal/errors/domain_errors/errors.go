package domainerrors

import "errors"

var (
	ErrBucketNotFound     = errors.New("bucket not found")
	ErrBodyRead           = errors.New("could not read request body")
	ErrEmptyStorageStream = errors.New("storage returned an empty stream")
)

type BodyReadError struct{ Err error }

func (e *BodyReadError) Error() string        { return e.Err.Error() }
func (e *BodyReadError) Unwrap() error        { return e.Err }
func (e *BodyReadError) Is(target error) bool { return target == ErrBodyRead }
