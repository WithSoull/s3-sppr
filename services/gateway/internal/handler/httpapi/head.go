package httpapi

import (
	"errors"
	"net/http"
	"strconv"
)

// HeadObject godoc
// @Summary Read metadata of the current object version
// @Description Development-only API. Calls Metadata only, without reading Storage or publishing object-read. Returns the headers of a full GET without a body. Range and If-Range are ignored. Bounded by METADATA_RPC_TIMEOUT, not GATEWAY_MAX_READ_DURATION.
// @Tags objects
// @Param bucket path string true "Bucket name"
// @Param key path string true "Object key (may contain slashes)"
// @Success 200
// @Header 200 {string} Content-Type "Object content type; application/octet-stream when empty"
// @Header 200 {integer} Content-Length "Full object size in bytes"
// @Header 200 {string} ETag "Quoted object ETag"
// @Header 200 {string} X-Object-Version-Id "Current version ID"
// @Header 200 {string} Accept-Ranges "bytes"
// @Header 200 {string} X-Content-Type-Options "nosniff"
// @Failure 404
// @Failure 502
// @Failure 503
// @Failure 504
// @Router /objects/{bucket}/{key} [head]
func (h *Handler) HeadObject(w http.ResponseWriter, r *http.Request) {
	bucket, key, ok := objectPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	meta, err := h.objects.Head(r.Context(), bucket, key)
	if err != nil {
		// net/http suppresses the error body on the wire for HEAD as well.
		writeServiceError(w, r, err)
		return
	}
	if meta.Size < 0 {
		writeServiceError(w, r, errors.New("invalid object size"))
		return
	}
	contentType := meta.ContentType
	if contentType == "" {
		contentType = "application/octet-stream"
	}
	w.Header().Set("Content-Type", contentType)
	w.Header().Set("Content-Length", strconv.FormatInt(meta.Size, 10))
	w.Header().Set("ETag", strconv.Quote(meta.ETag))
	w.Header().Set("X-Object-Version-Id", meta.VersionID)
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(http.StatusOK)
}
