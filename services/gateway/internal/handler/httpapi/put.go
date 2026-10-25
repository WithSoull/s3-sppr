package httpapi

import (
	access "github.com/alesplll/opens3-rebac/shared/pkg/go-kit/events/access"
	"io"
	"net/http"
	"strconv"
)

// PutObject godoc
// @Summary Store an object in an existing bucket
// @Description Development-only API. No authentication, authorization or quota enforcement. A failed response after Storage may have an unknown Metadata outcome; do not retry blindly.
// @Tags objects
// @Accept octet-stream
// @Param bucket path string true "Bucket name"
// @Param key path string true "Object key (may contain slashes)"
// @Param body body string true "Raw object bytes"
// @Param Content-Type header string false "MIME type"
// @Success 200
// @Failure 400 {string} string
// @Failure 404 {string} string
// @Failure 502 {string} string
// @Failure 503 {string} string
// @Failure 504 {string} string
// @Failure 507 {string} string
// @Router /objects/{bucket}/{key} [put]
func (h *Handler) PutObject(w http.ResponseWriter, r *http.Request) {
	bucket, key, ok := objectPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	event := access.Event{Bucket: bucket, ObjectKey: key, Operation: "write", Origin: "client", Result: "error"}
	defer func() {
		if h.access != nil {
			h.access.Record(r.Context(), event)
		}
	}()
	contentType := r.Header.Get("Content-Type")
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	var size *int64
	if r.ContentLength >= 0 {
		length := r.ContentLength
		size = &length
	}
	var body io.Reader = r.Body
	if h.access != nil {
		body = &countReader{reader: r.Body, count: &event.Bytes}
	}
	result, err := h.objects.Put(r.Context(), bucket, key, body, size, contentType)
	event.VersionID = result.VersionID
	if err != nil {
		writeServiceError(w, r, err)
		return
	}
	event.Result = "success"
	w.Header().Set("ETag", strconv.Quote(result.ETag))
	w.Header().Set("X-Object-Version-Id", result.VersionID)
	w.WriteHeader(http.StatusOK)
}
