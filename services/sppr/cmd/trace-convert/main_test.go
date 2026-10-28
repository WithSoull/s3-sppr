package main

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"flag"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/traces"
	"github.com/stretchr/testify/require"
)

func invoke(t *testing.T, args []string) error {
	t.Helper()
	previousArgs, previousFlags := os.Args, flag.CommandLine
	t.Cleanup(func() { os.Args = previousArgs; flag.CommandLine = previousFlags })
	os.Args = append([]string{"trace-convert"}, args...)
	flag.CommandLine = flag.NewFlagSet("trace-convert", flag.ContinueOnError)
	flag.CommandLine.SetOutput(io.Discard)
	return run()
}
func dockerArchive(t *testing.T) string {
	t.Helper()
	data := `[{"http.request.method":"GET","http.request.uri":"v2/repo/blobs/digest","http.response.status":200,"http.response.written":12,"timestamp":"2017-07-01T00:00:00Z"}]`
	p := filepath.Join(t.TempDir(), "fixture.tar.gz")
	f, err := os.Create(p)
	require.NoError(t, err)
	z := gzip.NewWriter(f)
	w := tar.NewWriter(z)
	require.NoError(t, w.WriteHeader(&tar.Header{Name: "data_centers/syd01/prod-syd01-logstash-2017.07.01-0.json", Mode: 0644, Size: int64(len(data))}))
	_, err = w.Write([]byte(data))
	require.NoError(t, err)
	require.NoError(t, w.Close())
	require.NoError(t, z.Close())
	require.NoError(t, f.Close())
	return p
}
func TestSourcesAndPublication(t *testing.T) {
	for _, tt := range []struct {
		name            string
		args            []string
		version, bucket string
	}{
		{"meta default remains compatible", []string{"-input", "../../internal/experiment/traces/testdata/meta.csv", "-source-url", traces.SourceRoot + "fixture.csv", "-source-share", "0.2", "-allow-inferred-success"}, traces.Version, "meta-eag"},
		{"COS", []string{"-source", "ibm-cos", "-input", "../../internal/experiment/traces/testdata/ibm-cos.txt", "-bucket", "ibm-cos-001", "-origin", "2020-01-01T00:00:00Z", "-allow-inferred-success"}, "ibm-cos-v1", "ibm-cos-001"},
		{"Docker requires no opt in", []string{"-source", "docker", "-input", dockerArchive(t), "-zone", "syd01"}, "ibm-docker-v1", "docker-syd01"},
		{"Docker explicit bucket wins", []string{"-source", "docker", "-input", dockerArchive(t), "-zone", "syd01", "-bucket", "meta-eag"}, "ibm-docker-v1", "meta-eag"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "out")
			require.NoError(t, invoke(t, append(tt.args, "-out", dir)))
			b, err := os.ReadFile(filepath.Join(dir, "manifest.json"))
			require.NoError(t, err)
			var m traces.Manifest
			require.NoError(t, json.Unmarshal(b, &m))
			require.Equal(t, tt.version, m.ConverterVersion)
			f, err := os.Open(filepath.Join(dir, "events.jsonl"))
			require.NoError(t, err)
			defer f.Close()
			var e struct{ Bucket string }
			require.NoError(t, json.NewDecoder(f).Decode(&e))
			require.Equal(t, tt.bucket, e.Bucket)
		})
	}
}
func TestFailedConversionIsNotPublished(t *testing.T) {
	for _, tt := range []struct {
		name string
		args []string
	}{
		{"COS origin required", []string{"-source", "ibm-cos", "-bucket", "ibm-cos-001", "-input", "../../internal/experiment/traces/testdata/ibm-cos.txt", "-allow-inferred-success"}},
		{"COS opt in required", []string{"-source", "ibm-cos", "-bucket", "ibm-cos-001", "-input", "../../internal/experiment/traces/testdata/ibm-cos.txt", "-origin", "2020-01-01T00:00:00Z"}},
		{"unknown source", []string{"-source", "unknown"}},
		{"Docker zone required", []string{"-source", "docker", "-input", dockerArchive(t)}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			parent := t.TempDir()
			dir := filepath.Join(parent, "out")
			require.Error(t, invoke(t, append(tt.args, "-out", dir)))
			entries, err := os.ReadDir(parent)
			require.NoError(t, err)
			require.Empty(t, entries, "neither completed output nor staging left behind")
		})
	}
}
