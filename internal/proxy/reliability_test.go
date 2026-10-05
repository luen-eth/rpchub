package proxy

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"rpchub/internal/rpcutil"
	"strings"
	"testing"
	"time"
)

func TestProviderFailuresFailOverAndContractRevertSurvives(t *testing.T) {
	for _, failure := range []string{`{"code":-32601,"message":"Method not available on this plan with fullTransactions=true"}`, `{"code":-32005,"message":"limit exceeded"}`, `"method not supported"`} {
		t.Run(failure, func(t *testing.T) {
			bad, bc := upstream(t, okJSON(`{"jsonrpc":"2.0","id":1,"error":`+failure+`}`))
			good, gc := upstream(t, okJSON(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
			hub, pools := newHub(t, 3, bad.URL, good.URL)
			pl, _ := pools.Get("1")
			pl.SetPriority([]string{bad.URL})
			res, body := post(t, hub.URL+"/1", rpcReq)
			if res.StatusCode != 200 || !strings.Contains(body, `"result"`) || bc.hits.Load() != 1 || gc.hits.Load() != 1 {
				t.Fatalf("no failover: %d %s hits %d/%d", res.StatusCode, body, bc.hits.Load(), gc.hits.Load())
			}
			pl.ReportSuccess(bad.URL, time.Millisecond, 100)
			post(t, hub.URL+"/1", rpcReq)
			if bc.hits.Load() != 1 {
				t.Fatal("successful health probe cleared provider restriction")
			}
		})
	}
	revert := `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"execution reverted: restricted"}}`
	a, c := upstream(t, okJSON(revert))
	hub, _ := newHub(t, 3, a.URL)
	_, body := post(t, hub.URL+"/1", `{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[]}`)
	if body != revert || c.hits.Load() != 1 {
		t.Fatal("contract error changed")
	}
}
func TestBadJSONAndEnvelopeFailOver(t *testing.T) {
	for _, reply := range []string{`{broken`, `{"result":"0x1"}`, `{"jsonrpc":"2.0","id":999,"result":"0x1"}`} {
		bad, _ := upstream(t, okJSON(reply))
		good, _ := upstream(t, okJSON(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
		hub, pools := newHub(t, 3, bad.URL, good.URL)
		pl, _ := pools.Get("1")
		pl.SetPriority([]string{bad.URL})
		res, body := post(t, hub.URL+"/1", rpcReq)
		if res.StatusCode != 200 || !strings.Contains(body, `"result"`) {
			t.Fatalf("bad response escaped: %s", body)
		}
	}
}
func TestBatchRetriesOnlyFailedRead(t *testing.T) {
	bad, _ := upstream(t, okJSON(`[{"jsonrpc":"2.0","id":1,"result":"0xsent"},{"jsonrpc":"2.0","id":2,"result":"0x1"},{"jsonrpc":"2.0","id":3,"error":{"code":-32601,"message":"method not supported"}}]`))
	good, gc := upstream(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var requests []rpcutil.Request
		json.Unmarshal(body, &requests)
		if len(requests) != 1 || string(requests[0].ID) != "3" {
			t.Errorf("successful reads or writes replayed: %s", body)
		}
		io.WriteString(w, `[{"jsonrpc":"2.0","id":3,"result":"0x3"}]`)
	})
	hub, pools := newHub(t, 3, bad.URL, good.URL)
	pl, _ := pools.Get("1")
	pl.SetPriority([]string{bad.URL})
	res, body := post(t, hub.URL+"/1", `[{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0x00"]},{"jsonrpc":"2.0","id":2,"method":"eth_chainId"},{"jsonrpc":"2.0","id":3,"method":"eth_blockNumber"}]`)
	if res.StatusCode != 200 || !strings.Contains(body, "0xsent") || !strings.Contains(body, "0x3") || gc.hits.Load() != 1 {
		t.Fatalf("batch failed: %d %s", res.StatusCode, body)
	}
}
func TestWriteAndNotificationNeverReplay(t *testing.T) {
	for _, id := range []string{`,"id":1`, ``} {
		bad, bc := upstream(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) })
		good, gc := upstream(t, okJSON(`{"jsonrpc":"2.0","id":1,"result":"0xsent"}`))
		hub, pools := newHub(t, 3, bad.URL, good.URL)
		pl, _ := pools.Get("1")
		pl.SetPriority([]string{bad.URL})
		post(t, hub.URL+"/1", `{"jsonrpc":"2.0"`+id+`,"method":"eth_sendRawTransaction","params":["0x00"]}`)
		if bc.hits.Load() != 1 || gc.hits.Load() != 0 {
			t.Fatal("write/notification replayed")
		}
	}
}
func TestIndexerUnverifiedCapabilitiesAndExhaustion(t *testing.T) {
	up, c := upstream(t, okJSON(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"unsupported method"}}`))
	hub, pools := newHub(t, 3, up.URL)
	pl, _ := pools.Get("1")
	pl.SetIndexer(up.URL, false)
	res, _ := post(t, hub.URL+"/1/indexer", rpcReq)
	if res.StatusCode != 503 || c.hits.Load() != 0 {
		t.Fatal("unverified capabilities received traffic")
	}
	pl.SetIndexer(up.URL, true)
	res, body := post(t, hub.URL+"/1/indexer", rpcReq)
	if res.StatusCode != 502 || strings.Contains(body, "-32601") {
		t.Fatalf("fatal provider error escaped: %d %s", res.StatusCode, body)
	}
}
func head(n uint64, fork bool) blockHeader {
	v := n
	if fork {
		v += 1000
	}
	parent := v - 1
	if fork && n == 101 {
		parent = 100
	}
	return blockHeader{Number: fmt.Sprintf("0x%x", n), Hash: fmt.Sprintf("0x%064x", v), ParentHash: fmt.Sprintf("0x%064x", parent)}
}
func TestHeadGuardRejectsStaleAndAcceptsWitnessedReorg(t *testing.T) {
	responder := func(fork bool) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			var req rpcutil.Request
			json.NewDecoder(r.Body).Decode(&req)
			var ps []string
			json.Unmarshal(req.Params, &ps)
			var n uint64
			fmt.Sscanf(ps[0], "0x%x", &n)
			json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": 1, "result": head(n, fork && n > 100)})
		}
	}
	a, _ := upstream(t, responder(true))
	b, _ := upstream(t, responder(true))
	hub, pools := newHub(t, 3, a.URL, strings.Replace(b.URL, "127.0.0.1", "localhost", 1))
	_ = hub
	pl, _ := pools.Get("1")
	h := &Handler{Client: http.DefaultClient, Timeout: time.Second}
	s := h.state("1")
	ctx := context.Background()
	if err := h.acceptHead(ctx, pl, s, a.URL, head(100, false)); err != nil {
		t.Fatal(err)
	}
	if err := h.acceptHead(ctx, pl, s, a.URL, head(101, false)); err != nil {
		t.Fatal(err)
	}
	if err := h.acceptHead(ctx, pl, s, a.URL, head(100, false)); err == nil {
		t.Fatal("stale head accepted")
	}
	if err := h.acceptHead(ctx, pl, s, a.URL, head(101, true)); err != nil {
		t.Fatalf("witnessed fork rejected: %v", err)
	}
}

func TestRequestBudgetAllowsTimeoutFailover(t *testing.T) {
	slow, _ := upstream(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			return
		case <-time.After(1500 * time.Millisecond):
			io.WriteString(w, `{"jsonrpc":"2.0","id":1,"result":"0x1"}`)
		}
	})
	good, c := upstream(t, okJSON(`{"jsonrpc":"2.0","id":1,"result":"0x2"}`))
	hub, pools := newHub(t, 3, slow.URL, good.URL)
	pl, _ := pools.Get("1")
	pl.SetPriority([]string{slow.URL})
	start := time.Now()
	res, body := post(t, hub.URL+"/1", rpcReq)
	if res.StatusCode != 200 || !strings.Contains(body, "0x2") || c.hits.Load() != 1 || time.Since(start) > 2*time.Second {
		t.Fatalf("timeout failover failed: %d %s", res.StatusCode, body)
	}
}
func TestOversizedUpstreamResponseFailsOver(t *testing.T) {
	bad, _ := upstream(t, func(w http.ResponseWriter, r *http.Request) {
		io.WriteString(w, `{"jsonrpc":"2.0","id":1,"result":"0xbad"}`+strings.Repeat(" ", maxResponseBody+1))
	})
	good, c := upstream(t, okJSON(`{"jsonrpc":"2.0","id":1,"result":"0x1"}`))
	hub, pools := newHub(t, 3, bad.URL, good.URL)
	pl, _ := pools.Get("1")
	pl.SetPriority([]string{bad.URL})
	res, body := post(t, hub.URL+"/1", rpcReq)
	if res.StatusCode != 200 || c.hits.Load() != 1 || strings.Contains(body, "0xbad") {
		t.Fatal("oversized upstream prevented failover")
	}
}
func TestMalformedBatchFailsOver(t *testing.T) {
	bad, _ := upstream(t, okJSON(`{"jsonrpc":"2.0","id":1,"result":"wrong shape"}`))
	good, _ := upstream(t, okJSON(`[{"jsonrpc":"2.0","id":1,"result":"0x1"}]`))
	hub, pools := newHub(t, 3, bad.URL, good.URL)
	pl, _ := pools.Get("1")
	pl.SetPriority([]string{bad.URL})
	res, body := post(t, hub.URL+"/1", `[{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}]`)
	if res.StatusCode != 200 || !strings.Contains(body, "0x1") {
		t.Fatalf("batch failover failed: %d %s", res.StatusCode, body)
	}
}

func TestPartialBatchPreservesAcknowledgedWrite(t *testing.T) {
	bad, _ := upstream(t, okJSON(`[{"jsonrpc":"2.0","id":1,"result":"0xsent"},{"jsonrpc":"2.0","id":99,"result":"wrong ID"}]`))
	good, _ := upstream(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if strings.Contains(string(body), "eth_sendRawTransaction") {
			t.Error("write replayed")
		}
		io.WriteString(w, `[{"jsonrpc":"2.0","id":2,"result":"0x1"}]`)
	})
	hub, pools := newHub(t, 3, bad.URL, good.URL)
	pl, _ := pools.Get("1")
	pl.SetPriority([]string{bad.URL})
	res, body := post(t, hub.URL+"/1", `[{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction"},{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"}]`)
	if res.StatusCode != 200 || !strings.Contains(body, "0xsent") || !strings.Contains(body, "0x1") {
		t.Fatalf("valid batch entries lost: %s", body)
	}
}
func TestSuccessfulNotificationDoesNotPenalizeProvider(t *testing.T) {
	up, _ := upstream(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) })
	hub, pools := newHub(t, 3, up.URL)
	res, _ := post(t, hub.URL+"/1", `{"jsonrpc":"2.0","method":"eth_blockNumber"}`)
	pl, _ := pools.Get("1")
	if res.StatusCode != 204 || pl.Snapshot(true).Endpoints[0].TotalFail != 0 {
		t.Fatal("notification success counted as failure")
	}
}
