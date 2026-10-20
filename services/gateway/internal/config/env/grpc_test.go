package env

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStorageConfigRetrieveChunkSize(t *testing.T) {
	t.Setenv("STORAGE_RETRIEVE_CHUNK_SIZE_BYTES", "262144")

	cfg, err := NewStorageConfig()
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.RetrieveChunkSizeBytes(); got != 262144 {
		t.Fatalf("RetrieveChunkSizeBytes() = %d, want 262144", got)
	}
}

func TestStorageConfigRejectsInvalidRetrieveChunkSize(t *testing.T) {
	t.Setenv("STORAGE_RETRIEVE_CHUNK_SIZE_BYTES", "0")

	if _, err := NewStorageConfig(); err == nil {
		t.Fatal("NewStorageConfig() accepted a zero chunk size")
	}
}

func TestMetadataConfigTimeout(t *testing.T) {
	tests := []struct {
		name, timeout string
		wantErr       bool
	}{
		{name: "valid", timeout: "200ms"},
		{name: "zero", timeout: "0s", wantErr: true},
		{name: "negative", timeout: "-1s", wantErr: true},
		{name: "invalid duration", timeout: "bad", wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("METADATA_RPC_TIMEOUT", tt.timeout)
			cfg, err := NewMetadataConfig()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, 200*time.Millisecond, cfg.Timeout())
		})
	}
}
