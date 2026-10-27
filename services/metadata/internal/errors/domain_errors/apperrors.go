package domain_errors

import (
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/sys"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/sys/codes"
)

var (
	ErrBucketNotFound      = sys.NewCommonError("bucket not found", codes.NotFound)
	ErrObjectNotFound      = sys.NewCommonError("object not found", codes.NotFound)
	ErrBucketAlreadyExists = sys.NewCommonError("bucket already exists", codes.AlreadyExists)
	ErrBucketNotEmpty      = sys.NewCommonError("bucket is not empty", codes.FailedPrecondition)
	ErrInternal            = sys.NewCommonError("internal error", codes.Internal)

	ErrPlacementNotFound    = sys.NewCommonError("placement not found", codes.NotFound)
	ErrCopyNotFound         = sys.NewCommonError("placement copy not found", codes.NotFound)
	ErrVersionNotCurrent    = sys.NewCommonError("version is not current", codes.Aborted)
	ErrPlacementChanged     = sys.NewCommonError("placement generation or class changed", codes.Aborted)
	ErrCopyAlreadyReserved  = sys.NewCommonError("version already has a reserved copy", codes.Aborted)
	ErrInsufficientCapacity = sys.NewCommonError("not enough free space in target storage class", codes.ResourceExhausted)
	ErrMigrationMismatch    = sys.NewCommonError("migration_id is already used with other parameters", codes.FailedPrecondition)
	ErrInvalidCopyState     = sys.NewCommonError("operation is not allowed in current copy state", codes.FailedPrecondition)
	ErrCopySizeMismatch     = sys.NewCommonError("verified size differs from version size", codes.FailedPrecondition)
)
