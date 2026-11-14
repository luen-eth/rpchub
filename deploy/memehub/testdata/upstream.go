// Command upstream is a deterministic local-only RPC fixture for profile tests.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
)

type request struct {
	JSONRPC string            `json:"jsonrpc"`
	ID      json.RawMessage   `json:"id"`
	Method  string            `json:"method"`
	Params  []json.RawMessage `json:"params"`
}

var state = struct {
	sync.Mutex
	IDs                 map[string]int `json:"ids"`
	AuthorizationLeaked int            `json:"authorizationLeaked"`
	CookieLeaked        int            `json:"cookieLeaked"`
	MissingOrNullIDs    int            `json:"missingOrNullIds"`
}{IDs: make(map[string]int)}

func block(number uint64) map[string]any {
	parent := uint64(0)
	if number > 0 {
		parent = number - 1
	}
	return map[string]any{
		"number": fmt.Sprintf("0x%x", number), "hash": fmt.Sprintf("0x%064x", number),
		"parentHash": fmt.Sprintf("0x%064x", parent), "timestamp": "0x60000000",
		"transactions": []any{},
	}
}

func result(chain uint64, call request) (any, map[string]any) {
	switch call.Method {
	case "eth_chainId":
		return fmt.Sprintf("0x%x", chain), nil
	case "eth_blockNumber":
		return "0x3e8", nil
	case "eth_getBlockByNumber", "eth_getBlockByHash":
		var tag string
		if len(call.Params) > 0 {
			_ = json.Unmarshal(call.Params[0], &tag)
		}
		n := uint64(1000)
		if strings.HasPrefix(tag, "0x") {
			var err error
			n, err = strconv.ParseUint(strings.TrimPrefix(tag, "0x"), 16, 64)
			if err != nil {
				return nil, map[string]any{"code": -32602, "message": "invalid fixture block"}
			}
		}
		return block(n), nil
	case "eth_getLogs":
		return []any{}, nil
	case "eth_getBalance":
		return "0x2a", nil
	case "eth_getCode":
		return "0x60006000f3", nil
	case "eth_getStorageAt", "eth_call":
		return "0x" + strings.Repeat("0", 63) + "1", nil
	default:
		return nil, map[string]any{"code": -32601, "message": "unsupported fixture method"}
	}
}

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /__stats", func(w http.ResponseWriter, _ *http.Request) {
		state.Lock()
		defer state.Unlock()
		_ = json.NewEncoder(w).Encode(&state)
	})
	mux.HandleFunc("POST /", func(w http.ResponseWriter, r *http.Request) {
		segments := strings.Split(strings.Trim(r.URL.Path, "/"), "/")
		chain, err := strconv.ParseUint(segments[0], 10, 64)
		if err != nil || (chain != 56 && chain != 8453) {
			http.NotFound(w, r)
			return
		}
		body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 2<<20))
		if err != nil {
			http.Error(w, "invalid fixture body", http.StatusBadRequest)
			return
		}
		var calls []request
		batch := strings.HasPrefix(strings.TrimSpace(string(body)), "[")
		if batch {
			err = json.Unmarshal(body, &calls)
		} else {
			var call request
			err = json.Unmarshal(body, &call)
			calls = []request{call}
		}
		if err != nil {
			http.Error(w, "invalid fixture JSON", http.StatusBadRequest)
			return
		}
		state.Lock()
		if r.Header.Get("Authorization") != "" {
			state.AuthorizationLeaked++
		}
		if r.Header.Get("Cookie") != "" {
			state.CookieLeaked++
		}
		for _, call := range calls {
			if len(call.ID) == 0 || strings.TrimSpace(string(call.ID)) == "null" {
				state.MissingOrNullIDs++
			}
			var id string
			if json.Unmarshal(call.ID, &id) == nil && strings.HasPrefix(id, "smoke-") {
				state.IDs[id]++
			}
		}
		state.Unlock()
		var responses []map[string]any
		for _, call := range calls {
			if len(call.ID) == 0 {
				continue
			}
			response := map[string]any{"jsonrpc": "2.0", "id": call.ID}
			value, rpcError := result(chain, call)
			if rpcError != nil {
				response["error"] = rpcError
			} else {
				response["result"] = value
			}
			responses = append(responses, response)
		}
		w.Header().Set("Content-Type", "application/json")
		if len(responses) == 0 {
			w.WriteHeader(http.StatusNoContent)
		} else if batch {
			_ = json.NewEncoder(w).Encode(responses)
		} else {
			_ = json.NewEncoder(w).Encode(responses[0])
		}
	})
	if err := http.ListenAndServe(":9570", mux); err != nil {
		panic(err)
	}
}
