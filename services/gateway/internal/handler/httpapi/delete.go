package httpapi

import "net/http"

// DeleteObject godoc
// @Summary Delete an object from the catalog
// @Description Development-only API. Removes the object from Metadata, so later GET returns 404. Blobs stay in Storage: cleanup is not implemented yet.
// @Tags objects
// @Param bucket path string true "Bucket name"
// @Param key path string true "Object key (may contain slashes)"
// @Success 204
// @Failure 404 {string} string
// @Failure 502 {string} string
// @Failure 503 {string} string
// @Failure 504 {string} string
// @Router /objects/{bucket}/{key} [delete]
func (h *Handler) DeleteObject(w http.ResponseWriter, r *http.Request) {
	bucket, key, ok := objectPath(r.URL.Path)
	if !ok {
		http.NotFound(w, r)
		return
	}
	if err := h.objects.Delete(r.Context(), bucket, key); err != nil {
		writeServiceError(w, r, err)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}
