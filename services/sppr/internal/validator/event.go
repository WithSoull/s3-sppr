package validator

import (
	"fmt"
	"regexp"
	"strings"
	"unicode/utf8"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

var streamID = regexp.MustCompile(`^[A-Za-z0-9_.-]{1,128}$`)

func AccessEvent(e model.AccessEvent) error {
	switch {
	case !streamID.MatchString(e.GatewayID) || !streamID.MatchString(e.StreamID):
		return fmt.Errorf("gateway_id and stream_id must match %s", streamID)
	case e.Sequence < 1:
		return fmt.Errorf("sequence must be positive")
	case e.OccurredAt.IsZero() || e.OccurredAt.Year() < 1970 || e.OccurredAt.Year() > 9999:
		return fmt.Errorf("invalid occurred_at")
	case !validText(e.Bucket, 255) || !validText(e.ObjectKey, 1024):
		return fmt.Errorf("invalid bucket or object_key")
	case len(e.VersionID) > 256 || strings.ContainsRune(e.VersionID, 0) || !utf8.ValidString(e.VersionID):
		return fmt.Errorf("invalid version_id")
	case e.Operation != model.OperationRead && e.Operation != model.OperationWrite:
		return fmt.Errorf("operation must be read or write")
	case e.Origin != model.OriginClient && e.Origin != model.OriginMigration:
		return fmt.Errorf("origin must be client or migration")
	case e.Result != model.ResultSuccess:
		return fmt.Errorf("result must be success")
	case e.VersionID == "":
		return fmt.Errorf("successful operations require version_id")
	case e.Bytes < 0:
		return fmt.Errorf("bytes must not be negative")
	}
	return nil
}

func validText(s string, limit int) bool {
	return s != "" && len(s) <= limit && !strings.ContainsRune(s, 0) && utf8.ValidString(s)
}
