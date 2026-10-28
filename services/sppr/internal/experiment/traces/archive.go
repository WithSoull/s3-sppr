package traces

import (
	"archive/tar"
	"bufio"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/dataset"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/model"
	"github.com/alesplll/opens3-rebac/services/sppr/internal/validator"
)

func validateTrace(c Config) error {
	if c.Input == "" || c.Bucket == "" || len(c.Bucket) > 255 || !utf8.ValidString(c.Bucket) || strings.ContainsRune(c.Bucket, 0) || math.IsNaN(c.KeyShare) || c.KeyShare <= 0 || c.KeyShare > 1 {
		return fmt.Errorf("input, valid bucket and key-share in (0,1] required")
	}
	return nil
}

// Never extract archive paths. The callback receives only a bounded member reader.
// IBM's .tgz downloads actually use Unix compress, which Go's gzip cannot read.
func scanArchive(ctx context.Context, input string, plain bool, visit func(string, io.Reader) error) (string, error) {
	f, err := os.Open(input)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	raw := bufio.NewReaderSize(io.TeeReader(f, h), 256*1024)
	magic, err := raw.Peek(2)
	if err != nil {
		return "", err
	}
	var r io.Reader = raw
	var cmd *exec.Cmd
	if magic[0] == 0x1f && magic[1] == 0x8b {
		z, err := gzip.NewReader(raw)
		if err != nil {
			return "", err
		}
		defer z.Close()
		r = z
	} else if magic[0] == 0x1f && magic[1] == 0x9d {
		cmd = exec.CommandContext(ctx, "/usr/bin/gzip", "-cd")
		cmd.Stdin = raw
		// No shell, no archive-supplied arguments or executable content.
		pipe, err := cmd.StdoutPipe()
		if err != nil {
			return "", err
		}
		if err = cmd.Start(); err != nil {
			return "", fmt.Errorf("Unix compress requires /usr/bin/gzip: %w", err)
		}
		defer func() {
			_ = pipe.Close()
			if cmd.ProcessState == nil {
				_ = cmd.Process.Kill()
				_ = cmd.Wait()
			}
		}()
		r = pipe
	} else if plain {
		if err := visit(filepath.Base(input), raw); err != nil {
			return "", err
		}
		return hex.EncodeToString(h.Sum(nil)), nil
	} else {
		return "", fmt.Errorf("expected gzip/Unix compress tar archive")
	}
	t := tar.NewReader(r)
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		header, err := t.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", err
		}
		if header.Typeflag == tar.TypeReg || header.Typeflag == tar.TypeRegA {
			if err = visit(header.Name, t); err != nil {
				return "", fmt.Errorf("member %q: %w", header.Name, err)
			}
		}
	}
	// Validate the compression trailer and hash the complete original, including
	// tar padding and members outside the selected zone/day range.
	if _, err = io.Copy(io.Discard, r); err != nil {
		return "", err
	}
	if cmd != nil {
		if err = cmd.Wait(); err != nil {
			return "", fmt.Errorf("Unix compress decode: %w", err)
		}
	}
	if _, err = io.Copy(io.Discard, raw); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func writeCatalog(dir string, versions []model.Version) error {
	sort.Slice(versions, func(i, j int) bool {
		a, b := versions[i], versions[j]
		if a.ObjectKey != b.ObjectKey {
			return a.ObjectKey < b.ObjectKey
		}
		if !a.CreatedAt.Equal(b.CreatedAt) {
			return a.CreatedAt.Before(b.CreatedAt)
		}
		return a.VersionID < b.VersionID
	})
	f, err := os.OpenFile(filepath.Join(dir, "catalog.json"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := bufio.NewWriterSize(f, 256*1024)
	if _, err = w.WriteString("[\n"); err != nil {
		return err
	}
	enc := json.NewEncoder(w)
	for i, v := range versions {
		if i > 0 {
			if _, err = w.WriteString(",\n"); err != nil {
				return err
			}
		}
		if err = enc.Encode(v); err != nil {
			return err
		}
	}
	if _, err = w.WriteString("]\n"); err != nil {
		return err
	}
	if err = w.Flush(); err != nil {
		return err
	}
	return f.Close()
}

func finishManifest(m Manifest, c Config, dir, inputHash string) (Manifest, error) {
	m.InputSHA256 = map[string]string{filepath.Base(c.Input): inputHash}
	m.SHA256 = map[string]string{}
	for _, name := range []string{"catalog.json", "events.jsonl"} {
		sum, err := dataset.HashFile(filepath.Join(dir, name))
		if err != nil {
			return m, err
		}
		m.SHA256[name] = sum
	}
	executable, err := os.Executable()
	if err != nil {
		return m, err
	}
	m.ConverterSHA256, err = dataset.HashFile(executable)
	if err != nil {
		return m, err
	}
	b, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return m, err
	}
	return m, os.WriteFile(filepath.Join(dir, "manifest.json"), append(b, '\n'), 0644)
}

func sampling(c Config) map[string]any {
	return map[string]any{"key_share": c.KeyShare, "seed": c.SampleSeed, "algorithm": "SampleKey: first 64 bits of SHA256(seed NUL original key), big endian, < floor(share * 2^64)", "counts_rescaled": false}
}
func versionID(key string, generation int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s\x00%d", key, generation)))
	return hex.EncodeToString(sum[:])
}
func validKey(key string) bool {
	return key != "" && utf8.ValidString(key) && !strings.ContainsRune(key, 0)
}
func emitRead(enc *json.Encoder, c Config, source string, v model.Version, at model.AccessEvent) error {
	at.GatewayID = "trace-" + source
	at.StreamID = source + "-v1"
	at.Bucket = c.Bucket
	at.ObjectKey = v.ObjectKey
	at.VersionID = v.VersionID
	at.Operation = model.OperationRead
	at.Origin = model.OriginClient
	at.Result = model.ResultSuccess
	if err := validator.AccessEvent(at); err != nil {
		return err
	}
	return enc.Encode(at)
}
