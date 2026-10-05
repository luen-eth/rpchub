// Package proxy forwards validated JSON-RPC requests with provider-aware
// failover for safe reads and a consistent, capability-verified indexer route.
package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"rpchub/internal/pool"
	"rpchub/internal/registry"
)

const (
	maxRequestBody  = 8 << 20
	maxResponseBody = 32 << 20
)

type Handler struct {
	Reg         *registry.Registry
	Pools       *pool.Set
	Client      *http.Client
	MaxRetries  int
	Timeout     time.Duration
	Log         *slog.Logger
	indexMu     sync.Mutex
	indexStates map[string]*indexState
}

// Proxy handles "POST /{chain}".
func (h *Handler) Proxy(w http.ResponseWriter, r *http.Request) {
	h.proxy(w, r, false, false)
}

// ProxyArchive handles "POST /{chain}/archive": same forwarding machinery,
// but only endpoints positively verified as archive-capable are eligible.
func (h *Handler) ProxyArchive(w http.ResponseWriter, r *http.Request) {
	h.proxy(w, r, true, false)
}

func (h *Handler) proxy(w http.ResponseWriter, r *http.Request, archiveOnly, indexer bool) {
	token := r.PathValue("chain")
	ch, ok := h.Reg.Resolve(token)
	if !ok {
		writeRPCError(w, http.StatusNotFound, fmt.Sprintf("rpchub: unknown chain %q; see GET /chains", token))
		return
	}
	if (archiveOnly || indexer) && ch.Kind != registry.KindEVM {
		writeRPCError(w, http.StatusNotFound,
			fmt.Sprintf("rpchub: archive/indexer routes are EVM-only (%s)", ch.Key))
		return
	}
	pl, ok := h.Pools.Get(ch.Key)
	if !ok {
		writeRPCError(w, http.StatusServiceUnavailable, "rpchub: chain not ready")
		return
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxRequestBody))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeRPCError(w, http.StatusRequestEntityTooLarge, "rpchub: request body too large")
		} else {
			writeRPCError(w, http.StatusBadRequest, "rpchub: unreadable request body")
		}
		return
	}

	h.relay(w, r, pl, body, archiveOnly, indexer)
}

// Options handles CORS preflight for "OPTIONS /{chain}".
func (h *Handler) Options(w http.ResponseWriter, _ *http.Request) {
	setCORS(w)
	w.WriteHeader(http.StatusNoContent)
}

// MethodHint handles "GET /{chain}" with a friendly pointer.
func (h *Handler) MethodHint(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("chain")
	ch, ok := h.Reg.Resolve(token)
	if !ok {
		writeRPCError(w, http.StatusNotFound, fmt.Sprintf("rpchub: unknown chain %q; see GET /chains", token))
		return
	}
	setCORS(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusMethodNotAllowed)
	json.NewEncoder(w).Encode(map[string]string{
		"error": fmt.Sprintf("send JSON-RPC via POST /%s; endpoint health at GET /%s/health", ch.Key, ch.Key),
	})
}

// ArchiveHint handles "GET /{chain}/archive" with a friendly pointer.
func (h *Handler) ArchiveHint(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("chain")
	ch, ok := h.Reg.Resolve(token)
	if !ok {
		writeRPCError(w, http.StatusNotFound, fmt.Sprintf("rpchub: unknown chain %q; see GET /chains", token))
		return
	}
	setCORS(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusMethodNotAllowed)
	json.NewEncoder(w).Encode(map[string]string{
		"error": fmt.Sprintf("send JSON-RPC via POST /%s/archive; endpoint health at GET /%s/health", ch.Key, ch.Key),
	})
}

type upstreamResult struct {
	status int
	body   []byte
	dur    time.Duration
}

// retryable reports whether another endpoint should be tried instead of
// returning this response. 2xx JSON and 400 JSON (a malformed client request
// judged by the upstream) pass through; everything else — 429, 5xx,
// key-gated 401/403/404, HTML error pages — is an endpoint problem.
func (r *upstreamResult) retryable() bool {
	if r.status < 300 || r.status == http.StatusBadRequest {
		return !looksJSON(r.body)
	}
	return true
}

func (h *Handler) forward(ctx context.Context, u string, body []byte) (*upstreamResult, error) {
	ctx, cancel := context.WithTimeout(ctx, h.Timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "rpchub/1.0")
	start := time.Now()
	resp, err := h.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rb, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody+1))
	if err != nil {
		return nil, err
	}
	if len(rb) > maxResponseBody {
		return nil, fmt.Errorf("upstream response exceeds size limit")
	}
	return &upstreamResult{status: resp.StatusCode, body: rb, dur: time.Since(start)}, nil
}

func (h *Handler) write(w http.ResponseWriter, res *upstreamResult, upstream string) {
	setCORS(w)
	w.Header().Set("Content-Type", "application/json")
	if upstream != "" {
		if u, err := url.Parse(upstream); err == nil {
			w.Header().Set("X-Rpchub-Upstream", u.Host)
		}
	}
	w.WriteHeader(res.status)
	w.Write(res.body)
}

func setCORS(w http.ResponseWriter) {
	hd := w.Header()
	hd.Set("Access-Control-Allow-Origin", "*")
	hd.Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	hd.Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
	hd.Set("Access-Control-Max-Age", "86400")
}

func writeRPCError(w http.ResponseWriter, httpStatus int, msg string) {
	setCORS(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(httpStatus)
	json.NewEncoder(w).Encode(map[string]any{
		"jsonrpc": "2.0",
		"id":      nil,
		"error":   map[string]any{"code": -32000, "message": msg},
	})
}

func looksJSON(b []byte) bool {
	b = bytes.TrimSpace(b)
	return len(b) > 0 && (b[0] == '{' || b[0] == '[') && json.Valid(b)
}
