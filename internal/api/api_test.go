package api

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"rpchub/internal/pool"
	"rpchub/internal/registry"
)

func newAPI(t *testing.T, started time.Time, markHealthy bool) *httptest.Server {
	t.Helper()
	entries := []registry.ChainEntry{{
		Name: "Ethereum Mainnet", ChainID: 1, ChainSlug: "ethereum", ShortName: "eth",
		RPC: []registry.RPCEntry{{URL: "https://node.example/v2/supersecretkey"}},
	}}
	reg := registry.New()
	if err := reg.Update(entries, registry.BuildOptions{ChainIDs: []int64{1}}); err != nil {
		t.Fatal(err)
	}
	pools := pool.NewSet()
	ch, _ := reg.Resolve("1")
	pl := pools.Ensure("1", 10)
	pl.SetEndpoints(ch.Endpoints)
	if markHealthy {
		pl.SetVerified(ch.Endpoints[0])
		pl.ReportSuccess(ch.Endpoints[0], 42*time.Millisecond, 1000)
		pl.SetArchive(ch.Endpoints[0], true)
	}

	s := &Server{Reg: reg, Pools: pools, Started: started}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /chains", s.Chains)
	mux.HandleFunc("GET /health", s.Health)
	mux.HandleFunc("GET /{chain}/health", s.ChainHealth)
	mux.HandleFunc("GET /", s.Root)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func get(t *testing.T, url string) (int, string) {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestChains(t *testing.T) {
	srv := newAPI(t, time.Now(), true)
	code, body := get(t, srv.URL+"/chains")
	if code != 200 {
		t.Fatalf("code = %d", code)
	}
	var out []map[string]any
	if err := json.Unmarshal([]byte(body), &out); err != nil || len(out) != 1 {
		t.Fatalf("body = %s", body)
	}
	if out[0]["chain"] != "1" || out[0]["healthy"] != float64(1) || out[0]["height"] != float64(1000) {
		t.Fatalf("chain info = %v", out[0])
	}
	if out[0]["archive_healthy"] != float64(1) {
		t.Fatalf("archive_healthy = %v, want 1", out[0]["archive_healthy"])
	}
}

func TestHealthStates(t *testing.T) {
	// healthy pool -> ok
	if code, body := get(t, newAPI(t, time.Now(), true).URL+"/health"); code != 200 || !strings.Contains(body, `"ok"`) {
		t.Fatalf("healthy: %d %s", code, body)
	}
	// no healthy endpoints, fresh boot -> warming, still 200
	if code, body := get(t, newAPI(t, time.Now(), false).URL+"/health"); code != 200 || !strings.Contains(body, `"warming"`) {
		t.Fatalf("warming: %d %s", code, body)
	}
	// no healthy endpoints, past grace -> 503 degraded
	if code, body := get(t, newAPI(t, time.Now().Add(-5*time.Minute), false).URL+"/health"); code != 503 || !strings.Contains(body, `"degraded"`) {
		t.Fatalf("degraded: %d %s", code, body)
	}
}

func TestChainHealthRedactsURLs(t *testing.T) {
	srv := newAPI(t, time.Now(), true)
	code, body := get(t, srv.URL+"/eth/health")
	if code != 200 {
		t.Fatalf("code = %d, body = %s", code, body)
	}
	if strings.Contains(body, "supersecretkey") {
		t.Fatalf("endpoint path leaked: %s", body)
	}
	if !strings.Contains(body, "https://node.example/…") {
		t.Fatalf("redacted host missing: %s", body)
	}
	if code, _ := get(t, srv.URL+"/nope/health"); code != 404 {
		t.Fatalf("unknown chain health = %d", code)
	}
}

func TestRoot(t *testing.T) {
	srv := newAPI(t, time.Now(), true)
	if code, body := get(t, srv.URL+"/"); code != 200 || !strings.Contains(body, "rpchub") {
		t.Fatalf("root: %d %s", code, body)
	}
	if code, _ := get(t, srv.URL+"/deep/unknown/path"); code != 404 {
		t.Fatal("subtree should 404")
	}
}
