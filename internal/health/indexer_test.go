package health

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"rpchub/internal/pool"
	"rpchub/internal/rpcutil"
	"testing"
	"time"
)

func TestIndexerProbeChecksPonderShapes(t *testing.T) {
	for _, unsupported := range []string{"", "full", "hashLogs", "rangeLogs", "byHash"} {
		t.Run(unsupported, func(t *testing.T) {
			got := map[string]bool{}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var req rpcutil.Request
				json.NewDecoder(r.Body).Decode(&req)
				got[req.Capability()] = true
				var result any
				bad := false
				switch req.Method {
				case "eth_getBlockByNumber":
					result = map[string]any{"hash": fmt.Sprintf("0x%064x", 100), "number": "0x64", "transactions": []any{map[string]any{"hash": "0x01"}}}
					bad = unsupported == "full"
				case "eth_getLogs":
					result = []any{}
					bad = unsupported == "hashLogs" && req.Capability() == "eth_getLogs:hash" || unsupported == "rangeLogs" && req.Capability() == "eth_getLogs:range"
				case "eth_getBlockByHash":
					result = map[string]any{"hash": fmt.Sprintf("0x%064x", 100)}
					bad = unsupported == "byHash"
				}
				if bad {
					json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "error": map[string]any{"code": -32601, "message": "method not supported"}})
				} else {
					json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": result})
				}
			}))
			defer server.Close()
			pl := pool.New("1", 10)
			pl.SetEndpoints([]string{server.URL})
			pl.SetVerified(server.URL)
			pl.ReportSuccess(server.URL, time.Millisecond, 100)
			p := NewProber(pl, EVM{ChainID: 1}, server.Client(), time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)))
			p.checkIndexer(context.Background(), server.URL, 100)
			if ready, _, _ := pl.IndexerStatus(server.URL); ready != (unsupported == "") {
				t.Fatalf("wrong capability verdict: %t", ready)
			}
			if unsupported == "" {
				for _, shape := range []string{"eth_getBlockByNumber:full", "eth_getLogs:hash", "eth_getLogs:range", "eth_getBlockByHash"} {
					if !got[shape] {
						t.Errorf("missing shape %s", shape)
					}
				}
			}
		})
	}
}
