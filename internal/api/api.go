// Package api serves rpchub's operational endpoints: chain listing, overall
// health (Dokploy/docker healthcheck friendly) and per-chain endpoint detail.
package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"rpchub/internal/pool"
	"rpchub/internal/registry"
)

// warmupGrace keeps /health green right after boot, before the first probe
// sweep has finished, so orchestrators don't kill a starting container.
const warmupGrace = 90 * time.Second

type Server struct {
	Reg     *registry.Registry
	Pools   *pool.Set
	WSPools *pool.Set // nil when WS_ENABLED is off
	Started time.Time
}

type chainInfo struct {
	Chain          string   `json:"chain"`
	ChainID        int64    `json:"chain_id,omitempty"`
	Name           string   `json:"name"`
	Kind           string   `json:"kind"`
	Tokens         []string `json:"tokens,omitempty"`
	Healthy        int      `json:"healthy"`
	ArchiveHealthy int      `json:"archive_healthy"`
	WSHealthy      int      `json:"ws_healthy"`
	Total          int      `json:"total"`
	WSTotal        int      `json:"ws_total"`
	Height         uint64   `json:"height,omitempty"`
}

// Chains handles GET /chains.
func (s *Server) Chains(w http.ResponseWriter, _ *http.Request) {
	out := make([]chainInfo, 0)
	for _, ch := range s.Reg.Chains() {
		pl, ok := s.Pools.Get(ch.Key)
		if !ok {
			continue
		}
		snap := pl.Snapshot(false)
		var wsSnap pool.Snapshot
		if s.WSPools != nil {
			if wsPl, ok := s.WSPools.Get(ch.Key); ok {
				wsSnap = wsPl.Snapshot(false)
			}
		}
		out = append(out, chainInfo{
			Chain:          ch.Key,
			ChainID:        ch.ChainID,
			Name:           ch.Name,
			Kind:           ch.Kind.String(),
			Tokens:         s.Reg.Tokens(ch.Key),
			Healthy:        snap.Healthy,
			ArchiveHealthy: snap.ArchiveHealthy,
			WSHealthy:      wsSnap.Healthy,
			Total:          snap.Total,
			WSTotal:        wsSnap.Total,
			Height:         snap.RefHeight,
		})
	}
	writeJSON(w, http.StatusOK, out)
}

// Health handles GET /health: 200 when every enabled chain has at least one
// healthy endpoint, 200 "warming" during the boot grace period, 503 otherwise.
func (s *Server) Health(w http.ResponseWriter, _ *http.Request) {
	healthy := map[string]int{}
	var degraded []string
	for _, ch := range s.Reg.Chains() {
		count := 0
		if pl, ok := s.Pools.Get(ch.Key); ok {
			count = pl.HealthyCount()
		}
		healthy[ch.Key] = count
		if count == 0 {
			degraded = append(degraded, ch.Key)
		}
	}
	status, code := "ok", http.StatusOK
	if len(degraded) > 0 {
		if time.Since(s.Started) < warmupGrace {
			status = "warming"
		} else {
			status, code = "degraded", http.StatusServiceUnavailable
		}
	}
	body := map[string]any{"status": status, "healthy_endpoints": healthy}
	if len(degraded) > 0 {
		body["degraded_chains"] = degraded
	}
	writeJSON(w, code, body)
}

// chainHealth is the per-chain health document: the HTTP pool inline, with
// the WebSocket pool nested when the chain has one.
type chainHealth struct {
	pool.Snapshot
	WebSocket *pool.Snapshot `json:"websocket,omitempty"`
}

// ChainHealth handles GET /{chain}/health with per-endpoint scores. Endpoint
// URLs are redacted (host only): chainlist and EXTRA_RPCS URLs can embed API
// keys in their paths.
func (s *Server) ChainHealth(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("chain")
	ch, ok := s.Reg.Resolve(token)
	if !ok {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": fmt.Sprintf("unknown chain %q; see GET /chains", token)})
		return
	}
	pl, ok := s.Pools.Get(ch.Key)
	if !ok {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "chain not ready"})
		return
	}
	snap := pl.Snapshot(true)
	for i := range snap.Endpoints {
		snap.Endpoints[i].URL = registry.RedactURL(snap.Endpoints[i].URL)
	}
	out := chainHealth{Snapshot: snap}
	if s.WSPools != nil {
		if wsPl, ok := s.WSPools.Get(ch.Key); ok {
			ws := wsPl.Snapshot(true)
			for i := range ws.Endpoints {
				ws.Endpoints[i].URL = registry.RedactURL(ws.Endpoints[i].URL)
			}
			out.WebSocket = &ws
		}
	}
	writeJSON(w, http.StatusOK, out)
}

// Root handles GET / with a short service description; anything else under
// the subtree falls through to 404.
func (s *Server) Root(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "not found"})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "rpchub",
		"usage":   "POST /{chainId|slug|alias} with a JSON-RPC body (e.g. POST /1, /ethereum, /56, /solana); POST /{chain}/archive for archive-verified upstreams (EVM only)",
		"ops":     []string{"GET /chains", "GET /health", "GET /{chain}/health"},
	})
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.WriteHeader(code)
	json.NewEncoder(w).Encode(v)
}
