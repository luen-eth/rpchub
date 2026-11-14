// Command upstream is a deterministic local-only RPC fixture for profile tests.
package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type request struct {
	JSONRPC string            `json:"jsonrpc"`
	ID      json.RawMessage   `json:"id"`
	Method  string            `json:"method"`
	Params  []json.RawMessage `json:"params"`
}

type readinessRequest struct {
	AtMS  int64    `json:"atMs"`
	Chain uint64   `json:"chain"`
	IDs   []string `json:"ids"`
}

var state = struct {
	sync.Mutex
	IDs                 map[string]int     `json:"ids"`
	AuthorizationLeaked int                `json:"authorizationLeaked"`
	CookieLeaked        int                `json:"cookieLeaked"`
	MissingOrNullIDs    int                `json:"missingOrNullIds"`
	ReadinessRequests   []readinessRequest `json:"readinessRequests"`
	RateRequests        []readinessRequest `json:"rateRequests"`
}{IDs: make(map[string]int)}

var fixtureTimestamp = time.Now().Unix()
var fixtureStarted = time.Now()
var fixtureReadinessDelay = func() time.Duration {
	value := os.Getenv("FIXTURE_READINESS_DELAY_MS")
	if value == "" {
		return 0
	}
	milliseconds, err := strconv.ParseInt(value, 10, 64)
	if err != nil || milliseconds < 0 || milliseconds > 5000 {
		panic("invalid fixture readiness delay")
	}
	return time.Duration(milliseconds) * time.Millisecond
}()

func word(value uint64) string { return fmt.Sprintf("%064x", value) }

// These selectors are recorded from the real Memehub config-readiness client.
// Only deterministic synthetic factory state is exposed by this local fixture.
func contractResult(call request) string {
	var target struct {
		Data string `json:"data"`
	}
	if len(call.Params) > 0 {
		_ = json.Unmarshal(call.Params[0], &target)
	}
	switch {
	case target.Data == "0x2ae9c600": // protocolVersion()
		version := "memehub-v1"
		encoded := fmt.Sprintf("%x", version)
		return "0x" + word(32) + word(uint64(len(version))) + encoded + strings.Repeat("0", 64-len(encoded))
	case strings.HasPrefix(target.Data, "0x2e5a5b86"): // routeEnabled(uint8)
		for _, route := range []uint64{0, 1, 2, 3, 4} {
			if target.Data == "0x2e5a5b86"+word(route) {
				return "0x" + word(1)
			}
		}
		return "0x" + word(0)
	case strings.HasPrefix(target.Data, "0x3d069542"): // quoteTarget(address)
		return "0x" + word(4_500_000_000_000_000_000)
	default:
		return "0x" + word(1) // Includes stablecoins(address).
	}
}

func block(number uint64) map[string]any {
	parent := uint64(0)
	if number > 0 {
		parent = number - 1
	}
	return map[string]any{
		"number": fmt.Sprintf("0x%x", number), "hash": fmt.Sprintf("0x%064x", number),
		"parentHash": fmt.Sprintf("0x%064x", parent), "timestamp": fmt.Sprintf("0x%x", fixtureTimestamp),
		"transactions": []any{},
	}
}

func result(chain uint64, call request) (any, map[string]any) {
	switch call.Method {
	case "eth_chainId":
		return fmt.Sprintf("0x%x", chain), nil
	case "eth_blockNumber":
		return "0x64", nil
	case "eth_getBlockByNumber", "eth_getBlockByHash":
		var tag string
		if len(call.Params) > 0 {
			_ = json.Unmarshal(call.Params[0], &tag)
		}
		n := uint64(100)
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
		return "0x6000600055", nil
	case "eth_call":
		return contractResult(call), nil
	case "eth_getStorageAt":
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
		var readinessIDs []string
		var rateIDs []string
		for _, call := range calls {
			if len(call.ID) == 0 || strings.TrimSpace(string(call.ID)) == "null" {
				state.MissingOrNullIDs++
			}
			var id string
			if json.Unmarshal(call.ID, &id) == nil && strings.HasPrefix(id, "smoke-") {
				state.IDs[id]++
				if strings.HasPrefix(id, "smoke-readiness-") {
					readinessIDs = append(readinessIDs, id)
				}
				if strings.HasPrefix(id, "smoke-rate-") {
					rateIDs = append(rateIDs, id)
				}
			}
		}
		if len(readinessIDs) > 0 {
			state.ReadinessRequests = append(state.ReadinessRequests, readinessRequest{
				AtMS: time.Since(fixtureStarted).Milliseconds(), Chain: chain, IDs: readinessIDs,
			})
		}
		if len(rateIDs) > 0 {
			state.RateRequests = append(state.RateRequests, readinessRequest{
				AtMS: time.Since(fixtureStarted).Milliseconds(), Chain: chain, IDs: rateIDs,
			})
		}
		state.Unlock()
		if len(readinessIDs) > 0 {
			time.Sleep(fixtureReadinessDelay)
		}
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
