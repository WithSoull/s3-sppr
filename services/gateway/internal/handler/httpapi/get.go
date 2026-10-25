package httpapi

import (
	"bufio"
	"errors"
	access "github.com/alesplll/opens3-rebac/shared/pkg/go-kit/events/access"
	"io"
	"net/http"
	"strconv"

	"github.com/alesplll/opens3-rebac/shared/pkg/go-kit/logger"
	"go.uber.org/zap"
)

// GetObject godoc
// @Summary Read the current object version
// @Description Development-only API. Streams object bytes; range and version selection are not supported.
// @Tags objects
// @Produce octet-stream
// @Param bucket path string true "Bucket name"
// @Param key path string true "Object key (may contain slashes)"
// @Success 200 {file} binary
// @Failure 400 {string} string
// @Failure 404 {string} string
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
	event := access.Event{Bucket: bucket, ObjectKey: key, Operation: "read", Origin: "client", Result: "error"}
	defer func() {
		if h.access != nil {
			h.access.Record(r.Context(), event)
		}
	}()
	result, err := h.objects.Get(r.Context(), bucket, key)
	event.VersionID = result.VersionID
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	defer result.Body.Close()
	if result.Size < 0 {
		writeServiceError(w, r, errors.New("invalid object size"))
		return
	}
	// With Content-Length: 0, HTTP clients complete as soon as headers arrive.
	if result.Size == 0 {
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
	w.Header().Set("Content-Length", strconv.FormatInt(result.Size, 10))
	w.Header().Set("ETag", strconv.Quote(result.ETag))
	w.Header().Set("X-Object-Version-Id", result.VersionID)
	if result.Size == 0 {
		w.WriteHeader(http.StatusOK)
		if r.Context().Err() == nil {
			event.Result = "success"
		}
		return
	}
	var dst io.Writer = w
	if h.access != nil {
		dst = &countWriter{writer: w, count: &event.Bytes}
	}
	err = copyVerifiedObject(dst, result.Body, result.Size)
	if err != nil {
		logger.Error(r.Context(), "object response interrupted", zap.String("bucket", bucket), zap.String("key", key), zap.Error(err))
		// Headers may already be on the wire; abort instead of completing a successful response.
		panic(http.ErrAbortHandler)
	}
	if r.Context().Err() == nil {
		event.Result = "success"
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
