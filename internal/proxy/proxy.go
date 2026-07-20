// Package proxy forwards JSON-RPC requests (single or batch, treated as an
// opaque body) to the best endpoint of a chain's pool, failing over to other
// endpoints on transport-level errors, 429/5xx and non-JSON responses.
// JSON-RPC-level errors are the upstream's answer and pass through verbatim.
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
	"time"

	"rpchub/internal/pool"
	"rpchub/internal/registry"
)

const (
	maxRequestBody  = 8 << 20
	maxResponseBody = 32 << 20
)

type Handler struct {
	Reg        *registry.Registry
	Pools      *pool.Set
	Client     *http.Client
	MaxRetries int
	Timeout    time.Duration
	Log        *slog.Logger
}

// Proxy handles "POST /{chain}".
func (h *Handler) Proxy(w http.ResponseWriter, r *http.Request) {
	h.proxy(w, r, false)
}

// ProxyArchive handles "POST /{chain}/archive": same forwarding machinery,
// but only endpoints positively verified as archive-capable are eligible.
func (h *Handler) ProxyArchive(w http.ResponseWriter, r *http.Request) {
	h.proxy(w, r, true)
}

func (h *Handler) proxy(w http.ResponseWriter, r *http.Request, archiveOnly bool) {
	token := r.PathValue("chain")
	ch, ok := h.Reg.Resolve(token)
	if !ok {
		writeRPCError(w, http.StatusNotFound, fmt.Sprintf("rpchub: unknown chain %q; see GET /chains", token))
		return
	}
	if archiveOnly && ch.Kind != registry.KindEVM {
		writeRPCError(w, http.StatusNotFound,
			fmt.Sprintf("rpchub: no archive pool for %s (archive detection is EVM-only)", ch.Key))
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

	tried := make(map[string]bool, h.MaxRetries)
	var last *upstreamResult
	var lastURL, lastErr string
	for attempt := 0; attempt < h.MaxRetries; attempt++ {
		u, ok := pl.Pick(tried, archiveOnly)
		if !ok {
			break
		}
		tried[u] = true
		res, err := h.forward(r.Context(), u, body)
		if err != nil {
			pl.ReportFailure(u, registry.ErrString(err))
			lastErr = registry.ErrString(err)
			if r.Context().Err() != nil {
				return // client is gone; nothing sensible to write
			}
			continue
		}
		if res.retryable() {
			pl.ReportFailure(u, fmt.Sprintf("HTTP %d", res.status))
			last, lastURL = res, u
			continue
		}
		if res.status < 300 {
			pl.ReportSuccess(u, res.dur, 0)
		}
		h.write(w, res, u)
		h.Log.Debug("proxied", "chain", ch.Key, "upstream", registry.RedactURL(u),
			"status", res.status, "ms", res.dur.Milliseconds(), "attempts", attempt+1)
		return
	}

	if last != nil { // retries exhausted: the last upstream answer beats a generic 502
		h.write(w, last, lastURL)
		h.Log.Warn("proxy exhausted retries", "chain", ch.Key, "archive", archiveOnly, "status", last.status, "attempts", len(tried))
		return
	}
	if len(tried) == 0 {
		msg := fmt.Sprintf("rpchub: no healthy upstream for chain %s", ch.Key)
		if archiveOnly {
			msg = fmt.Sprintf("rpchub: no archive-capable upstream known for chain %s yet; detection runs with health probes, see GET /%s/health", ch.Key, ch.Key)
		}
		h.Log.Warn("proxy has no eligible upstream", "chain", ch.Key, "archive", archiveOnly)
		writeRPCError(w, http.StatusServiceUnavailable, msg)
		return
	}
	if lastErr == "" {
		lastErr = "no healthy upstream"
	}
	h.Log.Warn("proxy failed", "chain", ch.Key, "archive", archiveOnly, "err", lastErr, "attempts", len(tried))
	writeRPCError(w, http.StatusBadGateway, fmt.Sprintf("rpchub: all upstreams failed for chain %s: %s", ch.Key, lastErr))
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
	rb, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBody))
	if err != nil {
		return nil, err
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
	for _, c := range b {
		switch c {
		case ' ', '\t', '\r', '\n':
			continue
		default:
			return c == '{' || c == '['
		}
	}
	return false
}
