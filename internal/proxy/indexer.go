package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"rpchub/internal/pool"
	"rpchub/internal/registry"
	"rpchub/internal/rpcutil"
	"strconv"
	"strings"
	"sync"
	"time"
)

type blockHeader struct {
	Number     string `json:"number"`
	Hash       string `json:"hash"`
	ParentHash string `json:"parentHash"`
}

func (b blockHeader) height() (uint64, error) {
	return strconv.ParseUint(strings.TrimPrefix(b.Number, "0x"), 16, 64)
}

type indexState struct {
	mu           sync.Mutex
	leader       string
	tip          blockHeader
	history      map[uint64]string
	lastAccepted time.Time
	reason       string
}

func (h *Handler) state(key string) *indexState {
	h.indexMu.Lock()
	defer h.indexMu.Unlock()
	if h.indexStates == nil {
		h.indexStates = map[string]*indexState{}
	}
	if h.indexStates[key] == nil {
		h.indexStates[key] = &indexState{history: map[uint64]string{}}
	}
	return h.indexStates[key]
}
func isHead(req rpcutil.Request) bool {
	if req.Method == "eth_blockNumber" {
		return true
	}
	var params []json.RawMessage
	_ = json.Unmarshal(req.Params, &params)
	return req.Method == "eth_getBlockByNumber" && len(params) > 0 && string(params[0]) == `"latest"`
}
func (h *Handler) header(ctx context.Context, u, tag string) (blockHeader, error) {
	var b blockHeader
	payload, _ := json.Marshal(rpcutil.Request{JSONRPC: "2.0", ID: json.RawMessage("1"), Method: "eth_getBlockByNumber", Params: json.RawMessage(fmt.Sprintf(`[%q,false]`, tag))})
	res, err := h.forward(ctx, u, payload)
	if err != nil {
		return b, err
	}
	if res.status != 200 {
		return b, fmt.Errorf("head verification HTTP %d", res.status)
	}
	rr, err := rpcutil.ParseResponse(res.body)
	if err != nil {
		return b, err
	}
	if len(rr.Error) > 0 && string(rr.Error) != "null" {
		return b, fmt.Errorf("head verification RPC error")
	}
	if err = json.Unmarshal(rr.Result, &b); err != nil {
		return b, err
	}
	n, numberErr := b.height()
	if tag != "latest" {
		if expected, e := strconv.ParseUint(strings.TrimPrefix(tag, "0x"), 16, 64); e == nil && expected != n {
			return b, fmt.Errorf("provider returned the wrong block height")
		}
	}
	if numberErr != nil || len(b.Hash) != 66 || len(b.ParentHash) != 66 {
		return b, fmt.Errorf("invalid block header")
	}
	return b, nil
}

func (h *Handler) indexerCandidate(ctx context.Context, pl *pool.Pool, candidates []string, req rpcutil.Request) (string, error) {
	s := h.state(pl.Key())
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, u := range candidates {
		if u == s.leader {
			return u, nil
		}
	}
	for _, u := range candidates {
		if s.tip.Hash != "" {
			b, err := h.header(ctx, u, "latest")
			if err != nil {
				continue
			}
			if err = h.acceptHead(ctx, pl, s, u, b); err != nil {
				s.reason = err.Error()
				continue
			}
		}
		s.leader = u
		s.reason = ""
		return u, nil
	}
	s.reason = "no backup with verified canonical head"
	return "", fmt.Errorf("%s", s.reason)
}

// acceptHead is called under s.mu. Stale providers never make latest regress.
// A changed fork needs a second host and an observed ancestor within 64 blocks.
func (h *Handler) acceptHead(ctx context.Context, pl *pool.Pool, s *indexState, u string, b blockHeader) error {
	n, err := b.height()
	if err != nil || len(b.Hash) != 66 || len(b.ParentHash) != 66 {
		return fmt.Errorf("invalid head")
	}
	if s.tip.Hash != "" {
		old, _ := s.tip.height()
		same := false
		if n == old {
			same = b.Hash == s.tip.Hash
		} else if n == old+1 {
			same = b.ParentHash == s.tip.Hash
		} else if n > old {
			at, err := h.header(ctx, u, s.tip.Number)
			same = err == nil && at.Hash == s.tip.Hash
		} else {
			// Even if it shares the chain, an older backup must wait to catch up.
			if s.history[n] == "" || s.history[n] == b.Hash {
				return fmt.Errorf("backup head is behind accepted head")
			}
		}
		if !same {
			witnessed := false
			candidateHost, _ := url.Parse(u)
			for _, v := range pl.Candidates(nil, "eth_getBlockByNumber", true, false) {
				witnessHost, _ := url.Parse(v)
				if witnessHost.Hostname() == candidateHost.Hostname() {
					continue
				}
				at, e := h.header(ctx, v, b.Number)
				if e == nil && at.Hash == b.Hash {
					witnessed = true
					break
				}
			}
			if !witnessed {
				return fmt.Errorf("fork requires an independent witness")
			}
			common := false
			commonAt := uint64(0)
			low := uint64(0)
			if old > 64 {
				low = old - 64
			}
			if n < low {
				return fmt.Errorf("fork exceeds 64-block verification window")
			}
			for k := min(n, old); ; k-- {
				if known := s.history[k]; known != "" {
					at, e := h.header(ctx, u, fmt.Sprintf("0x%x", k))
					if e == nil && at.Hash == known {
						common = true
						commonAt = k
						break
					}
				}
				if k == low {
					break
				}
			}
			if !common {
				return fmt.Errorf("no verified common ancestor within 64 blocks")
			}
			for k := range s.history {
				if k > commonAt {
					delete(s.history, k)
				}
			}
		}
	}
	s.tip = b
	s.history[n] = b.Hash
	if n > 0 {
		s.history[n-1] = b.ParentHash
	}
	for k := range s.history {
		if n > 64 && k < n-64 {
			delete(s.history, k)
		}
	}
	s.lastAccepted = time.Now()
	s.reason = ""
	return nil
}

func (h *Handler) validateIndexerReply(ctx context.Context, pl *pool.Pool, u string, req rpcutil.Request, rr rpcutil.Response) error {
	if req.Method == "eth_getBlockByHash" || req.Method == "eth_getBlockByNumber" {
		if string(rr.Result) == "null" {
			return fmt.Errorf("provider is missing requested block")
		}
	}
	if req.Method == "eth_getLogs" {
		var logs []json.RawMessage
		if string(rr.Result) == "null" || json.Unmarshal(rr.Result, &logs) != nil {
			return fmt.Errorf("invalid log array")
		}
	}
	if !isHead(req) {
		if req.Method == "eth_getBlockByNumber" || req.Method == "eth_getBlockByHash" {
			var b blockHeader
			var params []json.RawMessage
			if json.Unmarshal(rr.Result, &b) != nil || len(b.Hash) != 66 || len(b.ParentHash) != 66 {
				return fmt.Errorf("invalid block header")
			}
			_ = json.Unmarshal(req.Params, &params)
			if len(params) > 0 {
				var tag string
				_ = json.Unmarshal(params[0], &tag)
				if req.Method == "eth_getBlockByHash" && tag != b.Hash {
					return fmt.Errorf("provider returned wrong block hash")
				}
				if req.Method == "eth_getBlockByNumber" && strings.HasPrefix(tag, "0x") {
					want, e := strconv.ParseUint(tag[2:], 16, 64)
					got, e2 := b.height()
					if e != nil || e2 != nil || want != got {
						return fmt.Errorf("provider returned wrong block number")
					}
				}
			}
		}
		return nil
	}
	s := h.state(pl.Key())
	s.mu.Lock()
	defer s.mu.Unlock()
	var b blockHeader
	if req.Method == "eth_blockNumber" {
		var tag string
		if json.Unmarshal(rr.Result, &tag) != nil {
			return fmt.Errorf("invalid block number")
		}
		var err error
		b, err = h.header(ctx, u, tag)
		if err != nil {
			return err
		}
	} else if json.Unmarshal(rr.Result, &b) != nil {
		return fmt.Errorf("invalid head response")
	}
	if err := h.acceptHead(ctx, pl, s, u, b); err != nil {
		s.reason = err.Error()
		return err
	}
	return nil
}

func (h *Handler) IndexerHealth(w http.ResponseWriter, r *http.Request) {
	ch, ok := h.Reg.Resolve(r.PathValue("chain"))
	if !ok {
		writeRPCError(w, 404, "unknown chain")
		return
	}
	pl, ok := h.Pools.Get(ch.Key)
	if !ok || ch.Kind != registry.KindEVM {
		writeRPCError(w, 404, "no EVM indexer pool")
		return
	}
	s := h.state(pl.Key())
	s.mu.Lock()
	defer s.mu.Unlock()
	count := len(pl.Candidates(nil, "", true, false))
	status := 200
	if count == 0 || s.reason != "" {
		status = 503
	}
	setCORS(w)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(map[string]any{"chain": ch.Key, "eligible": count, "leader": registry.RedactURL(s.leader), "accepted_head": s.tip, "last_accepted_at": s.lastAccepted, "waiting_reason": s.reason})
}
