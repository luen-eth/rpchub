package proxy

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"rpchub/internal/pool"
	"rpchub/internal/registry"
	"rpchub/internal/rpcutil"
	"time"
)

func (h *Handler) ProxyIndexer(w http.ResponseWriter, r *http.Request) { h.proxy(w, r, false, true) }

type pendingCall struct {
	raw json.RawMessage
	req rpcutil.Request
}

func decodeCalls(body []byte) ([]pendingCall, bool, error) {
	batch := len(bytes.TrimSpace(body)) > 0 && bytes.TrimSpace(body)[0] == '['
	raws := []json.RawMessage{body}
	if batch {
		if err := json.Unmarshal(body, &raws); err != nil {
			return nil, batch, err
		}
	}
	if len(raws) == 0 {
		return nil, batch, fmt.Errorf("empty batch")
	}
	seen := map[string]bool{}
	calls := make([]pendingCall, 0, len(raws))
	for _, raw := range raws {
		var req rpcutil.Request
		if err := json.Unmarshal(raw, &req); err != nil || req.JSONRPC != "2.0" || req.Method == "" {
			return nil, batch, fmt.Errorf("invalid request")
		}
		if len(req.Params) > 0 && string(req.Params) != "null" && req.Params[0] != '[' && req.Params[0] != '{' {
			return nil, batch, fmt.Errorf("invalid params")
		}
		if len(req.ID) > 0 && string(req.ID) != "null" {
			var id any
			if json.Unmarshal(req.ID, &id) != nil {
				return nil, batch, fmt.Errorf("invalid ID")
			}
			switch id.(type) {
			case string, float64:
			default:
				return nil, batch, fmt.Errorf("invalid ID type")
			}
		}
		if len(req.ID) > 0 {
			key := string(req.ID)
			if seen[key] {
				return nil, batch, fmt.Errorf("duplicate ID")
			}
			seen[key] = true
		}
		calls = append(calls, pendingCall{raw, req})
	}
	return calls, batch, nil
}

func callBody(calls []pendingCall, batch bool) []byte {
	if !batch {
		return calls[0].raw
	}
	raws := make([]json.RawMessage, len(calls))
	for i, c := range calls {
		raws[i] = c.raw
	}
	b, _ := json.Marshal(raws)
	return b
}

func (h *Handler) relay(w http.ResponseWriter, r *http.Request, pl *pool.Pool, body []byte, archive, indexer bool) {
	calls, batch, err := decodeCalls(body)
	if err != nil {
		code := -32600
		if !json.Valid(body) {
			code = -32700
		}
		setCORS(w)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(400)
		json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": nil, "error": map[string]any{"code": code, "message": "invalid JSON-RPC request or duplicate batch ID"}})
		return
	}
	if indexer {
		for _, c := range calls {
			if !rpcutil.ReadOnly(c.req.Method) || len(c.req.ID) == 0 {
				writeRPCError(w, 400, "rpchub: indexer route requires read requests with IDs")
				return
			}
		}
	}
	// One total budget, even when several providers time out.
	ctx, cancel := context.WithTimeout(r.Context(), h.Timeout)
	defer cancel()
	done := map[string]json.RawMessage{}
	pending := calls
	tried := map[string]bool{}
	lastURL := ""
	providerFailed := false
	for attempt := 0; attempt < max(1, h.MaxRetries) && len(pending) > 0 && ctx.Err() == nil; attempt++ {
		// A provider must support every currently pending request shape.
		candidates := pl.Candidates(tried, pending[0].req.Capability(), indexer, archive)
		for _, c := range pending[1:] {
			allowed := map[string]bool{}
			for _, u := range pl.Candidates(tried, c.req.Capability(), indexer, archive) {
				allowed[u] = true
			}
			filtered := candidates[:0]
			for _, u := range candidates {
				if allowed[u] {
					filtered = append(filtered, u)
				}
			}
			candidates = filtered
		}
		if len(candidates) == 0 {
			break
		}
		u := pl.Choose(candidates)
		if indexer {
			u, err = h.indexerCandidate(ctx, pl, candidates, pending[0].req)
			if err != nil {
				providerFailed = true
				break
			}
		}
		tried[u] = true
		lastURL = u
		deadline, _ := ctx.Deadline()
		attemptCtx, attemptCancel := context.WithTimeout(ctx, time.Until(deadline)/time.Duration(max(1, h.MaxRetries-attempt)))
		res, forwardErr := h.forward(attemptCtx, u, callBody(pending, batch))
		attemptCancel()
		notificationsOnly := true
		for _, c := range pending {
			if len(c.req.ID) > 0 {
				notificationsOnly = false
				break
			}
		}
		if forwardErr == nil && notificationsOnly && (res.status == 204 || res.status == 200 && len(bytes.TrimSpace(res.body)) == 0) {
			pl.ReportSuccess(u, res.dur, 0)
			pending = nil
			break
		}
		if forwardErr != nil || res.retryable() {
			providerFailed = true
			detail := "invalid upstream response"
			if forwardErr != nil {
				detail = registry.ErrString(forwardErr)
			} else {
				detail = fmt.Sprintf("HTTP %d", res.status)
				if res.status == 429 {
					pl.BlockCapability(u, "", true)
				}
			}
			pl.ReportFailure(u, detail)
			// Never resend notifications or writes after ambiguous transport failure.
			next := make([]pendingCall, 0, len(pending))
			for _, c := range pending {
				if rpcutil.ReadOnly(c.req.Method) && len(c.req.ID) > 0 {
					next = append(next, c)
				} else if len(c.req.ID) > 0 {
					done[string(c.req.ID)] = rpcutil.ErrorResponse(c.req.ID, "rpchub: provider failure; write was not retried")
				}
			}
			pending = next
			continue
		}
		responses := []json.RawMessage{res.body}
		if batch && json.Unmarshal(res.body, &responses) != nil {
			responses = nil
		}
		expected := map[string]bool{}
		for _, c := range pending {
			if len(c.req.ID) > 0 {
				expected[string(c.req.ID)] = true
			}
		}
		byID := map[string]json.RawMessage{}
		ambiguous := map[string]bool{}
		for _, raw := range responses {
			rr, e := rpcutil.ParseResponse(raw)
			key := string(rr.ID)
			if e != nil || !expected[key] {
				continue
			}
			if byID[key] != nil {
				ambiguous[key] = true
			}
			byID[key] = raw
		}
		for key := range ambiguous {
			delete(byID, key)
		}
		next := make([]pendingCall, 0, len(pending))
		attemptFailed := false
		for _, c := range pending {
			if len(c.req.ID) == 0 {
				continue
			}
			raw := byID[string(c.req.ID)]
			if raw == nil {
				pl.ReportFailure(u, "invalid JSON-RPC envelope or response IDs")
				attemptFailed = true
				providerFailed = true
				if rpcutil.ReadOnly(c.req.Method) {
					next = append(next, c)
				} else {
					done[string(c.req.ID)] = rpcutil.ErrorResponse(c.req.ID, "rpchub: invalid provider response; write was not retried")
				}
				continue
			}
			rr, _ := rpcutil.ParseResponse(raw)
			retry, quota := rpcutil.ProviderError(rr.Error, c.req.Method)
			if retry && rpcutil.ReadOnly(c.req.Method) {
				pl.BlockCapability(u, c.req.Capability(), quota)
				pl.ReportFailure(u, "provider restricted or rate-limited this request shape")
				attemptFailed = true
				providerFailed = true
				next = append(next, c)
				continue
			}
			if indexer && (len(rr.Error) == 0 || string(rr.Error) == "null") {
				if e := h.validateIndexerReply(ctx, pl, u, c.req, rr); e != nil {
					pl.ReportFailure(u, e.Error())
					attemptFailed = true
					providerFailed = true
					next = append(next, c)
					continue
				}
			}
			done[string(c.req.ID)] = raw
		}
		if !attemptFailed {
			pl.ReportSuccess(u, res.dur, 0)
		}
		if h.Log != nil {
			h.Log.Debug("RPC attempt", "chain", pl.Key(), "upstream", registry.RedactURL(u), "attempt", attempt+1, "remaining", len(next), "indexer", indexer)
		}
		pending = next
	}
	// Preserve completed batch entries even when a read cannot be satisfied.
	for _, c := range pending {
		if len(c.req.ID) > 0 {
			done[string(c.req.ID)] = rpcutil.ErrorResponse(c.req.ID, fmt.Sprintf("rpchub: no eligible provider completed this read (archive=%t, indexer=%t); retry later", archive, indexer))
		}
	}
	results := make([]json.RawMessage, 0, len(calls))
	for _, c := range calls {
		if len(c.req.ID) > 0 {
			if raw := done[string(c.req.ID)]; raw != nil {
				results = append(results, raw)
			}
		}
	}
	status := http.StatusOK
	if len(pending) > 0 {
		if providerFailed {
			status = http.StatusBadGateway
		} else {
			status = http.StatusServiceUnavailable
		}
	}
	if batch {
		for _, c := range calls {
			if !rpcutil.ReadOnly(c.req.Method) {
				status = http.StatusOK
				break
			}
		}
	}
	if len(results) == 0 {
		setCORS(w)
		w.WriteHeader(http.StatusNoContent)
		return
	}
	var output []byte
	if batch {
		output, _ = json.Marshal(results)
	} else {
		output = results[0]
	}
	h.write(w, &upstreamResult{status: status, body: output}, lastURL)
}
