package httpapi

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/alesplll/opens3-rebac/services/gateway/internal/model"
	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"go.uber.org/zap"
)

// GetObject godoc
// @Summary Read the current object version
// @Description Development-only API. Streams object bytes from the Storage of the version's class. Supports one byte range; a Range header with several ranges or invalid syntax is ignored. Version selection is not supported. The whole request is bounded by GATEWAY_MAX_READ_DURATION.
// @Tags objects
// @Produce octet-stream
// @Param bucket path string true "Bucket name"
// @Param key path string true "Object key (may contain slashes)"
// @Param Range header string false "One byte range: bytes=first-last, bytes=first- or bytes=-suffix"
// @Param If-Range header string false "Strong ETag; the range applies only to the version with this ETag, otherwise the whole object is returned"
// @Success 200 {file} binary
// @Success 206 {file} binary
// @Failure 400 {string} string
// @Failure 404 {string} string
// @Failure 416 {string} string
// @Failure 502 {string} string
// @Failure 503 {string} string
// @Failure 504 {string} string
// @Router /objects/{bucket}/{key} [get]
func (h *Handler) GetObject(w http.ResponseWriter, r *http.Request) {
	bucket, key, ok := objectPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	// A source blob is deleted only after a migration delay longer than this bound.
	deadline := time.Now().Add(h.maxReadDuration)
	ctx, cancel := context.WithDeadline(r.Context(), deadline)
	defer cancel()
	// The context stops Storage, but a Write to a client that stopped reading
	// blocks until the connection itself has a deadline. net/http clears it
	// after the request, so keep-alive requests do not inherit it. Writers
	// without deadline support (tests) return ErrNotSupported.
	_ = http.NewResponseController(w).SetWriteDeadline(deadline)
	result, err := h.objects.Get(ctx, bucket, key, requestedRange(r.Header))
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	defer result.Body.Close()
	if result.Size < 0 {
		writeServiceError(w, r, errors.New("invalid object size"))
		return
	}
	length := result.Size
	if result.Range != nil {
		length = result.Range.Length
	}
	// With Content-Length: 0, HTTP clients complete as soon as headers arrive.
	if length == 0 {
		if err := verifyStreamEnd(result.Body); err != nil {
			writeServiceError(w, r, err)
			return
		}
	}
	contentType := result.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("Content-Length", strconv.FormatInt(length, 10))
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("ETag", strconv.Quote(result.ETag))
	w.Header().Set("X-Object-Version-Id", result.VersionID)
	status := http.StatusOK
	if result.Range != nil {
		status = http.StatusPartialContent
		last := result.Range.Offset + result.Range.Length - 1
		w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", result.Range.Offset, last, result.Size))
	}
	w.WriteHeader(status)
	if length == 0 {
		return
	}
	err = copyVerifiedObject(w, result.Body, length)
	if err != nil {
		logger.Error(r.Context(), "object response interrupted", zap.String("bucket", bucket), zap.String("key", key), zap.Error(err))
		// Headers may already be on the wire; abort instead of completing a successful response.
		panic(http.ErrAbortHandler)
	}
}

// Keep the final byte off the wire until Storage confirms EOF. Otherwise a
// Content-Length response can look complete even if the stream ends in error.
func copyVerifiedObject(dst io.Writer, src io.Reader, size int64) error {
	if _, err := io.CopyN(dst, src, size-1); err != nil {
		return err
	}
	tail := bufio.NewReaderSize(src, 1)
	var last [1]byte
	if _, err := io.ReadFull(tail, last[:]); err != nil {
		return err
	}
	if err := verifyStreamEnd(tail); err != nil {
		return err
	}
	n, err := dst.Write(last[:])
	if err == nil && n != len(last) {
		return io.ErrShortWrite
	}
	return err
}

func verifyStreamEnd(src io.Reader) error {
	n, err := io.CopyN(io.Discard, src, 1)
	if n != 0 {
		return errors.New("storage returned more bytes than declared")
	}
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

// requestedRange applies If-Range (RFC 9110 §13.1.5): only a strong ETag can
// keep the range. A weak ETag or a date never matches, because Gateway sends a
// strong ETag and no Last-Modified, so the whole object is served.
func requestedRange(header http.Header) *model.ByteRange {
	byteRange := parseRange(header.Get("Range"))
	ifRange := strings.TrimSpace(header.Get("If-Range"))
	if byteRange == nil || ifRange == "" {
		return byteRange
	}
	if len(ifRange) < 3 || ifRange[0] != '"' || ifRange[len(ifRange)-1] != '"' {
		return nil
	}
	byteRange.IfRangeETag = ifRange[1 : len(ifRange)-1]
	return byteRange
}

// parseRange returns nil for a missing header, several ranges or invalid syntax:
// RFC 9110 lets the server ignore such a Range and send the whole object.
func parseRange(header string) *model.ByteRange {
	spec, ok := strings.CutPrefix(header, "bytes=")
	if !ok || strings.Contains(spec, ",") {
		return nil
	}
	firstText, lastText, ok := strings.Cut(strings.TrimSpace(spec), "-")
	if !ok {
		return nil
	}
	if firstText == "" {
		suffix, err := strconv.ParseInt(lastText, 10, 64)
		if err != nil || suffix <= 0 {
			return nil
		}
		return &model.ByteRange{Suffix: suffix}
	}
	first, err := strconv.ParseInt(firstText, 10, 64)
	if err != nil || first < 0 {
		return nil
	}
	if lastText == "" {
		return &model.ByteRange{First: first, Last: -1}
	}
	last, err := strconv.ParseInt(lastText, 10, 64)
	if err != nil || last < first {
		return nil
	}
	return &model.ByteRange{First: first, Last: last}
}
