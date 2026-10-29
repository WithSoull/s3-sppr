package traces

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func dockerConfig(t *testing.T) DockerConfig {
	raw, err := os.ReadFile("testdata/docker.json")
	require.NoError(t, err)
	return DockerConfig{Config: Config{Input: makeArchive(t, map[string]string{"data_centers/syd01/prod-syd01-logstash-2017.07.01-0.json": string(raw), "data_centers/dal09/dal09-logstash-2017.07.01-0.json": "invalid ignored zone", "../../outside": "never extracted"}), Bucket: "docker-syd01", KeyShare: 1, SampleSeed: "0"}, Zone: "syd01"}
}
func TestDockerPolicies(t *testing.T) {
	c := dockerConfig(t)
	dir := t.TempDir()
	m, err := ConvertDocker(context.Background(), c, dir)
	require.NoError(t, err)
	for key, want := range map[string]int64{"input_rows": 23, "files_selected": 1, "events": 5, "versions": 2, "head_excluded": 1, "manifests_excluded": 2, "uploads_excluded": 2, "ping_excluded": 2, "other_excluded": 1, "method_excluded": 1, "non_2xx": 5, "status_excluded": 4, "invalid_timestamp": 1, "invalid_written": 3, "unknown_full_size_reads": 1, "size_conflict_objects": 1, "size_conflict_rows": 2, "digests_multiple_repos": 1, "digests_multiple_full_get_repos": 1, "digests_cross_repo_max_size_conflicts": 1, "digests_full_size_conflicts": 1, "out_of_order_timestamps": 1} {
		require.Equal(t, want, m.Counts[key], key)
	}
	vs, es := readTrace(t, dir)
	require.Len(t, vs, 2)
	require.Equal(t, "empty", vs[0].ObjectKey)
	require.Equal(t, int64(0), vs[0].SizeBytes)
	require.Equal(t, "shared", vs[1].ObjectKey)
	require.Equal(t, int64(120), vs[1].SizeBytes)
	require.Equal(t, "2017-07-01T00:00:01Z", vs[1].CreatedAt.Format(time.RFC3339))
	for i, e := range es {
		require.Equal(t, int64(i+1), e.Sequence)
		require.True(t, e.IsDemand())
		if i > 0 {
			require.False(t, e.OccurredAt.Before(es[i-1].OccurredAt))
		}
	}
	require.Equal(t, int64(20), es[0].Bytes, "206 before full 200 must retain one read")
	require.Equal(t, es[0].VersionID, es[1].VersionID, "digest shared across repos is one immutable version")
	require.Equal(t, int64(0), es[4].Bytes)
	require.Equal(t, "not stated; used with the authors' permission", m.License["data"])
}
func TestDockerFilterAndFailures(t *testing.T) {
	for _, tt := range []struct {
		name                string
		from, to, zone, raw string
		wantErr             bool
		files               int64
	}{
		{name: "day inclusive exclusive", from: "2017-07-01", to: "2017-07-02", files: 1},
		{name: "from excludes", from: "2017-07-02", wantErr: true},
		{name: "to excludes", to: "2017-07-01", wantErr: true},
		{name: "wrong day", from: "2017-02-30", wantErr: true},
		{name: "reverse", from: "2017-07-02", to: "2017-07-01", wantErr: true},
		{name: "zone path", zone: "../syd01", wantErr: true},
		{name: "trailing JSON", raw: "[] {}", wantErr: true},
		{name: "not array", raw: "{}", wantErr: true},
		{name: "truncated", raw: "[{", wantErr: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := dockerConfig(t)
			c.From = tt.from
			c.To = tt.to
			if tt.zone != "" {
				c.Zone = tt.zone
			}
			if tt.raw != "" {
				c.Input = makeArchive(t, map[string]string{"data_centers/syd01/syd01-logstash-2017.07.01-0.json": tt.raw})
			}
			m, err := ConvertDocker(context.Background(), c, t.TempDir())
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.files, m.Counts["files_selected"])
			}
		})
	}
}
func TestDockerSamplingAndKeyMapping(t *testing.T) {
	c := dockerConfig(t)
	c.KeyShare = .33
	a, b := t.TempDir(), t.TempDir()
	m, err := ConvertDocker(context.Background(), c, a)
	require.NoError(t, err)
	n, err := ConvertDocker(context.Background(), c, b)
	require.NoError(t, err)
	require.Equal(t, m, n)
	require.Positive(t, m.Counts["sampled_out"])
	vs, es := readTrace(t, a)
	for _, v := range vs {
		require.True(t, SampleKey(v.ObjectKey, c.SampleSeed, c.KeyShare))
	}
	for _, e := range es {
		require.True(t, SampleKey(e.ObjectKey, c.SampleSeed, c.KeyShare))
	}
	for _, name := range []string{"catalog.json", "events.jsonl"} {
		x, err := os.ReadFile(a + "/" + name)
		require.NoError(t, err)
		y, err := os.ReadFile(b + "/" + name)
		require.NoError(t, err)
		require.Equal(t, x, y)
	}
	for _, key := range []string{strings.Repeat("a", 1025), "sha256-literal"} {
		c.KeyShare = 1
		c.Input = makeArchive(t, map[string]string{"data_centers/syd01/syd01-logstash-2017.07.01-0.json": `[{"http.request.method":"GET","http.request.uri":"v2/repo/blobs/` + key + `","http.response.status":200,"http.response.written":1,"timestamp":"2017-07-01T00:00:00Z"}]`})
		dir := t.TempDir()
		_, err := ConvertDocker(context.Background(), c, dir)
		require.NoError(t, err)
		vs, es := readTrace(t, dir)
		require.Equal(t, mapKey(key), vs[0].ObjectKey)
		require.Equal(t, vs[0].ObjectKey, es[0].ObjectKey)
	}
}
