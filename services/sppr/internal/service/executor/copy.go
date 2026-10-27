package executor

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/client"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
)

type copyResult struct {
	bytes      int64
	sourceMD5  string
	targetMD5  string
	sourceDone bool
	sourceErr  error
	err        error
}

// copyBlob streams the source blob into the target under targetBlobID. The MD5
// of the bytes read is compared with the MD5 the target computed.
func copyBlob(ctx context.Context, source client.StorageClient, sourceBlobID string, target client.StorageClient, targetBlobID string, size int64) copyResult {
	body, err := source.Open(ctx, sourceBlobID)
	if err != nil {
		return copyResult{sourceErr: err, err: err}
	}
	defer body.Close()
	reader := &countingReader{r: body, hash: md5.New()}
	targetMD5, err := target.Store(ctx, targetBlobID, size, reader)
	if err == nil && reader.err != nil {
		err = reader.err
	}
	return copyResult{
		bytes:      reader.n,
		sourceMD5:  hex.EncodeToString(reader.hash.Sum(nil)),
		targetMD5:  targetMD5,
		sourceDone: reader.eof,
		sourceErr:  reader.err,
		err:        err,
	}
}

func (r copyResult) failure(size int64) model.Reason {
	switch {
	case r.sourceErr != nil && errors.Is(r.sourceErr, model.ErrUpstreamNotFound):
		return model.ReasonSourceMissing
	case r.err != nil && client.IsTransient(r.err):
		return model.ReasonUpstreamUnavailable
	// Storage rejects a body of another size, so check the length first.
	case r.sourceDone && r.bytes != size:
		return model.ReasonVerifyMismatch
	case r.err != nil && errors.Is(r.err, model.ErrNoCapacity):
		return model.ReasonNoCapacity
	case r.err != nil:
		return model.ReasonInternal
	case r.sourceMD5 != r.targetMD5:
		return model.ReasonVerifyMismatch
	}
	return model.ReasonNone
}

func (r copyResult) describe(size int64) error {
	if r.err != nil {
		return fmt.Errorf("copied %d of %d bytes: %w", r.bytes, size, r.err)
	}
	return fmt.Errorf("copied %d of %d bytes, source MD5 %s, target MD5 %s", r.bytes, size, r.sourceMD5, r.targetMD5)
}

type countingReader struct {
	r    io.Reader
	hash hash.Hash
	n    int64
	eof  bool
	err  error
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.hash.Write(p[:n])
	c.n += int64(n)
	if errors.Is(err, io.EOF) {
		c.eof = true
	} else if err != nil {
		c.err = err
	}
	return n, err
}
