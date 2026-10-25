package httpapi_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestPutStreamsBeforeEOFAndPublishesOnlyAfterStorageCommit(t *testing.T) {
	server, upstream := newGateway(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	chunkReceived, atCommit, allowCommit := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	upstream.OnChunk = func() { once.Do(func() { close(chunkReceived) }) }
	upstream.BeforeCommit = func(ctx context.Context) error {
		close(atCommit)
		select {
		case <-allowCommit:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, server.URL+"/objects/fixture/stream", reader)
	require.NoError(t, err)
	done := make(chan error, 1)
	go func() {
		response, err := server.Client().Do(req)
		if err == nil {
			response.Body.Close()
			if response.StatusCode != http.StatusOK {
				err = fmt.Errorf("HTTP %d", response.StatusCode)
			}
		}
		done <- err
	}()
	_, err = io.WriteString(writer, strings.Repeat("x", 128*1024))
	require.NoError(t, err)
	select {
	case <-chunkReceived:
	case <-ctx.Done():
		t.Fatal("Gateway buffered PUT until EOF")
	}
	require.NoError(t, writer.Close())
	select {
	case <-atCommit:
	case <-ctx.Done():
		t.Fatal("Storage did not reach commit")
	}
	_, versions := upstream.Stats()
	require.Zero(t, versions, "Metadata published before Storage acknowledged commit")
	close(allowCommit)
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	_, versions = upstream.Stats()
	require.Equal(t, 1, versions, "successful PUT did not publish Metadata")
}

// A broken request body cannot be sent through a real HTTP client, so the handler is called directly.
func TestPutBodyReadFailureDoesNotPublishMetadata(t *testing.T) {
	handler, upstream := newGatewayHandler(t)
	request := httptest.NewRequest(http.MethodPut, "/objects/fixture/key", io.MultiReader(strings.NewReader("prefix"), failedReader{}))
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusBadRequest, response.Code)
	blobs, versions := upstream.Stats()
	require.Zero(t, blobs)
	require.Zero(t, versions)
}
