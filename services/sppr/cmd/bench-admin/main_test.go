package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alesplll/opens3-rebac/services/sppr/internal/experiment/testhttp"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestPercentile(t *testing.T) {
	for _, tt := range []struct {
		name    string
		samples []float64
		p, want float64
	}{
		{"empty", nil, .95, 0}, {"single", []float64{7}, .95, 7},
		{"median nearest rank", []float64{4, 1, 3, 2}, .5, 2},
		{"p95", []float64{20, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19}, .95, 19},
		{"maximum", []float64{2, 9, 4}, 1, 9}, {"minimum", []float64{2, 9, 4}, 0, 2},
	} {
		t.Run(tt.name, func(t *testing.T) {
			before := append([]float64(nil), tt.samples...)
			require.Equal(t, tt.want, percentile(tt.samples, tt.p))
			require.Equal(t, before, tt.samples)
		})
	}
	require.Nil(t, summarize(nil))
}

func TestRecovery(t *testing.T) {
	type probe struct {
		ready, status int
		id            string
	}
	for _, tt := range []struct {
		name        string
		probes      []probe
		timeout     bool
		statusCalls bool
	}{
		{"immediate", []probe{{200, 200, "saved"}}, false, true},
		{"ready unavailable", []probe{{503, 200, "saved"}, {200, 200, "saved"}}, false, true},
		{"status unavailable", []probe{{200, 503, ""}, {200, 200, "saved"}}, false, true},
		{"different plan", []probe{{200, 200, "new"}, {200, 200, "saved"}}, false, true},
		{"no latest plan", []probe{{200, 200, ""}, {200, 200, "saved"}}, false, true},
		{"never ready", []probe{{503, 200, "saved"}}, true, false},
		{"never same plan", []probe{{200, 200, "other"}}, true, true},
		// A previously successful ready probe must not be reused with a later status.
		{"both conditions same probe", []probe{{200, 200, "other"}, {503, 200, "saved"}}, true, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var rounds, states atomic.Int32
			server := testhttp.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "Bearer secret", r.Header.Get("Authorization"))
				switch r.URL.Path {
				case "/readyz":
					i := int(rounds.Add(1)) - 1
					w.WriteHeader(tt.probes[min(i, len(tt.probes)-1)].ready)
				case "/api/planner/status":
					states.Add(1)
					p := tt.probes[min(int(rounds.Load())-1, len(tt.probes)-1)]
					w.WriteHeader(p.status)
					if p.id == "" {
						fmt.Fprint(w, `{"latest_plan":null}`)
					} else {
						fmt.Fprintf(w, `{"latest_plan":{"plan_id":%q}}`, p.id)
					}
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			defer cancel()
			c := client{server.URL, "secret", server.Client()}
			err := c.recovery(ctx, "saved", time.Millisecond)
			if tt.timeout {
				require.ErrorIs(t, err, context.DeadlineExceeded)
			} else {
				require.NoError(t, err)
				require.GreaterOrEqual(t, int(rounds.Load()), len(tt.probes))
			}
			require.Equal(t, tt.statusCalls, states.Load() > 0)
		})
	}
}

func TestMeasure(t *testing.T) {
	for _, paused := range []bool{true, false} {
		t.Run(fmt.Sprint(paused), func(t *testing.T) {
			var recs, commands atomic.Int32
			seen := map[string]bool{}
			server := testhttp.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "Bearer token", r.Header.Get("Authorization"))
				switch r.URL.Path {
				case "/api/planner/plans":
					require.Equal(t, "100", r.URL.Query().Get("page_size"))
					if r.URL.Query().Get("page_token") == "" {
						fmt.Fprint(w, `{"plans":[{"plan_id":"latest","revision":3,"status":"confirmed","created_at":"2026-10-28T00:00:00Z","finished_at":"2026-10-28T00:00:02Z"}],"next_page_token":"3"}`)
					} else {
						require.Equal(t, "3", r.URL.Query().Get("page_token"))
						fmt.Fprint(w, `{"plans":[{"plan_id":"ready","revision":2,"status":"ready","created_at":"2026-10-28T00:00:00Z","finished_at":"2026-10-28T00:00:01Z"},{"plan_id":"unfinished","revision":1,"status":"computing"}]}`)
					}
				case "/api/planner/plans/ready/recommendations":
					require.Equal(t, "GET", r.Method)
					require.Equal(t, "100", r.URL.Query().Get("page_size"))
					recs.Add(1)
					fmt.Fprint(w, `{"recommendations":[]}`)
				case "/api/planner/status":
					fmt.Fprintf(w, `{"paused":%v,"latest_plan":{"plan_id":"latest"}}`, paused)
				case "/api/planner/commands/pause", "/api/planner/commands/resume":
					require.Equal(t, "POST", r.Method)
					expected := "/api/planner/commands/resume"
					if paused {
						expected = "/api/planner/commands/pause"
					}
					require.Equal(t, expected, r.URL.Path)
					var cmd map[string]string
					require.NoError(t, json.NewDecoder(r.Body).Decode(&cmd))
					_, err := uuid.Parse(cmd["command_id"])
					require.NoError(t, err)
					require.False(t, seen[cmd["command_id"]])
					seen[cmd["command_id"]] = true
					commands.Add(1)
					w.WriteHeader(202)
					fmt.Fprintf(w, `{"command_id":%q,"status":"accepted"}`, cmd["command_id"])
				case "/readyz":
					w.WriteHeader(200)
				default:
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			r, err := measure(context.Background(), client{server.URL, "token", server.Client()}, 3, 1e9, "true", time.Second)
			require.NoError(t, err)
			require.EqualValues(t, 3, recs.Load())
			require.EqualValues(t, 3, commands.Load())
			require.Equal(t, "ready", r.RecommendationPlan)
			require.Equal(t, &statistics{Count: 2, P50: 1000, P95: 2000, Max: 2000}, r.PlanDurations)
			require.Equal(t, 1, r.Unfinished)
			require.Equal(t, 3, r.Acceptance.Count)
			require.GreaterOrEqual(t, r.Recommendations.Max, r.Recommendations.P95)
			require.NotNil(t, r.RecoverySeconds)
			require.Greater(t, *r.RecoverySeconds, 0.0)
			require.Equal(t, "latest", r.RecoveryPlan)
		})
	}
}

func TestRecoveryCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := (client{api: "http://127.0.0.1:1", http: &http.Client{}}).recovery(ctx, "saved", time.Millisecond)
	require.True(t, errors.Is(err, context.Canceled))
}

func TestRecommendationPlan(t *testing.T) {
	tests := []struct {
		name  string
		plans []plan
		want  string
	}{
		{"none", nil, ""},
		{"ready preferred over newer confirmed", []plan{{ID: "r", Revision: 1, Status: "ready"}, {ID: "c", Revision: 2, Status: "confirmed"}}, "r"},
		{"latest ready", []plan{{ID: "r1", Revision: 1, Status: "ready"}, {ID: "r2", Revision: 3, Status: "ready"}}, "r2"},
		{"latest confirmed without ready", []plan{{ID: "c1", Revision: 4, Status: "confirmed"}, {ID: "c2", Revision: 7, Status: "confirmed"}}, "c2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := recommendationPlan(tt.plans)
			if tt.want == "" {
				require.Nil(t, got)
				return
			}
			require.Equal(t, tt.want, got.ID)
		})
	}
}
