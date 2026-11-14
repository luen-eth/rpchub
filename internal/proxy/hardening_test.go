package proxy

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"rpchub/internal/pool"
	"rpchub/internal/registry"
	"rpchub/internal/rpcutil"
)

func policyHub(t *testing.T, endpoint string, readOnly bool, maxBatchItems int) http.Handler {
	t.Helper()
	reg := registry.New()
	err := reg.Update([]registry.ChainEntry{{Name: "Ethereum", ChainID: 1, RPC: []registry.RPCEntry{{URL: endpoint}}}}, registry.BuildOptions{ChainIDs: []int64{1}, AllowHTTP: true})
	if err != nil {
		t.Fatal(err)
	}
	pools := pool.NewSet()
	pl := pools.Ensure("1", 10)
	pl.SetEndpoints([]string{endpoint})
	pl.SetVerified(endpoint)
	pl.ReportSuccess(endpoint, time.Millisecond, 100)
	pl.SetArchive(endpoint, true)
	pl.SetIndexer(endpoint, true)
	h := &Handler{
		Reg: reg, Pools: pools, Client: &http.Client{}, MaxRetries: 1,
		ReadOnly: readOnly, MaxBatchItems: maxBatchItems, Timeout: time.Second,
		Log: slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /{chain}", h.Proxy)
	mux.HandleFunc("POST /{chain}/archive", h.ProxyArchive)
	mux.HandleFunc("POST /{chain}/indexer", h.ProxyIndexer)
	return mux
}

func policyRequest(handler http.Handler, path, body string) *httptest.ResponseRecorder {
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, strings.NewReader(body)))
	return recorder
}

func TestReadOnlyProfileRejectsBeforeUpstream(t *testing.T) {
	requests := map[string]string{
		"write":         `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0x00"]}`,
		"unknown":       `{"jsonrpc":"2.0","id":1,"method":"debug_unknown"}`,
		"notification":  `{"jsonrpc":"2.0","method":"eth_chainId"}`,
		"null ID":       `{"jsonrpc":"2.0","id":null,"method":"eth_chainId"}`,
		"mixed write":   `[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},{"jsonrpc":"2.0","id":2,"method":"eth_sendRawTransaction","params":["0x00"]}]`,
		"mixed notice":  `[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},{"jsonrpc":"2.0","method":"eth_blockNumber"}]`,
		"mixed null ID": `[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},{"jsonrpc":"2.0","id":null,"method":"eth_blockNumber"}]`,
		"malformed":     `{"jsonrpc":"2.0","id":1,`,
		"duplicate ID":  `[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}]`,
	}
	for _, path := range []string{"/1", "/1/archive", "/1/indexer"} {
		for name, body := range requests {
			t.Run(path+"/"+name, func(t *testing.T) {
				up, count := upstream(t, okJSON(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
				res := policyRequest(policyHub(t, up.URL, true, 0), path, body)
				if res.Code != http.StatusBadRequest || !json.Valid(res.Body.Bytes()) {
					t.Fatalf("rejected request response = %d %s", res.Code, res.Body.String())
				}
				if count.hits.Load() != 0 {
					t.Fatalf("rejected request reached upstream %d times", count.hits.Load())
				}
			})
		}
	}
}

func echoPolicyReads(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	batch := len(bytes.TrimSpace(body)) > 0 && bytes.TrimSpace(body)[0] == '['
	var requests []rpcutil.Request
	if batch {
		_ = json.Unmarshal(body, &requests)
	} else {
		var request rpcutil.Request
		_ = json.Unmarshal(body, &request)
		requests = []rpcutil.Request{request}
	}
	responses := make([]map[string]any, 0, len(requests))
	for _, request := range requests {
		if len(request.ID) == 0 {
			continue
		}
		responses = append(responses, map[string]any{"jsonrpc": "2.0", "id": request.ID, "result": "0x1"})
	}
	w.Header().Set("Content-Type", "application/json")
	if len(responses) == 0 {
		w.WriteHeader(http.StatusNoContent)
	} else if batch {
		_ = json.NewEncoder(w).Encode(responses)
	} else {
		_ = json.NewEncoder(w).Encode(responses[0])
	}
}

func TestReadOnlyProfileAllowsKnownReadsAndExplicitZeroOrStringIDs(t *testing.T) {
	for _, path := range []string{"/1", "/1/archive"} {
		for _, body := range []string{
			`{"jsonrpc":"2.0","id":0,"method":"eth_chainId"}`,
			`{"jsonrpc":"2.0","id":"","method":"eth_getBalance","params":["0x0000000000000000000000000000000000000000","0x1"]}`,
			`[{"jsonrpc":"2.0","id":"a","method":"eth_call","params":[]},{"jsonrpc":"2.0","id":2,"method":"eth_getStorageAt","params":[]}]`,
		} {
			t.Run(path+body, func(t *testing.T) {
				up, count := upstream(t, echoPolicyReads)
				res := policyRequest(policyHub(t, up.URL, true, 2), path, body)
				if res.Code != http.StatusOK || count.hits.Load() != 1 {
					t.Fatalf("read rejected: %d %s, upstream hits %d", res.Code, res.Body.String(), count.hits.Load())
				}
			})
		}
	}
}

func TestBatchItemLimitRejectsWholeBatchBeforeUpstream(t *testing.T) {
	body := `[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"}]`
	for _, path := range []string{"/1", "/1/archive", "/1/indexer"} {
		for _, readOnly := range []bool{false, true} {
			up, count := upstream(t, echoPolicyReads)
			res := policyRequest(policyHub(t, up.URL, readOnly, 1), path, body)
			if res.Code != http.StatusBadRequest || count.hits.Load() != 0 || !strings.Contains(res.Body.String(), "batch item limit") {
				t.Fatalf("batch cap failed on %s: %d %s, upstream hits %d", path, res.Code, res.Body.String(), count.hits.Load())
			}
		}
	}
	for _, path := range []string{"/1", "/1/archive"} {
		up, count := upstream(t, echoPolicyReads)
		res := policyRequest(policyHub(t, up.URL, false, 0), path, body)
		if res.Code != http.StatusOK || count.hits.Load() != 1 {
			t.Fatalf("zero cap changed legacy behavior: %d %s", res.Code, res.Body.String())
		}
	}
}

func TestLegacyProfileStillAllowsWritesAndNotifications(t *testing.T) {
	for _, path := range []string{"/1", "/1/archive"} {
		for _, body := range []string{
			`{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0x00"]}`,
			`{"jsonrpc":"2.0","method":"eth_chainId"}`,
		} {
			up, count := upstream(t, echoPolicyReads)
			res := policyRequest(policyHub(t, up.URL, false, 0), path, body)
			if (res.Code != http.StatusOK && res.Code != http.StatusNoContent) || count.hits.Load() != 1 {
				t.Fatalf("legacy request behavior changed on %s: %d %s", path, res.Code, res.Body.String())
			}
		}
	}
	up, count := upstream(t, echoPolicyReads)
	res := policyRequest(policyHub(t, up.URL, false, 0), "/1/indexer", `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0x00"]}`)
	if res.Code != http.StatusBadRequest || count.hits.Load() != 0 {
		t.Fatal("legacy indexer write gate changed")
	}
}
