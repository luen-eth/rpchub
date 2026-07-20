package proxy

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"rpchub/internal/pool"
	"rpchub/internal/registry"
)

// counted wraps a handler and counts hits.
type counted struct {
	hits atomic.Int64
	h    http.HandlerFunc
}

func (c *counted) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.hits.Add(1)
	c.h(w, r)
}

func upstream(t *testing.T, h http.HandlerFunc) (*httptest.Server, *counted) {
	t.Helper()
	c := &counted{h: h}
	srv := httptest.NewServer(c)
	t.Cleanup(srv.Close)
	return srv, c
}

func okJSON(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		io.WriteString(w, body)
	}
}

// newHub builds a registry+pool for chain 1 backed by the given endpoint URLs
// and returns an httptest server running the real mux routes.
func newHub(t *testing.T, retries int, urls ...string) *httptest.Server {
	t.Helper()
	entries := []registry.ChainEntry{{
		Name: "Ethereum Mainnet", ChainID: 1, ChainSlug: "ethereum", ShortName: "eth",
		RPC: func() []registry.RPCEntry {
			var out []registry.RPCEntry
			for _, u := range urls {
				out = append(out, registry.RPCEntry{URL: u})
			}
			return out
		}(),
	}}
	reg := registry.New()
	if err := reg.Update(entries, registry.BuildOptions{ChainIDs: []int64{1}, AllowHTTP: true}); err != nil {
		t.Fatal(err)
	}
	pools := pool.NewSet()
	ch, _ := reg.Resolve("1")
	pools.Ensure("1", 10).SetEndpoints(ch.Endpoints)

	h := &Handler{
		Reg:        reg,
		Pools:      pools,
		Client:     &http.Client{},
		MaxRetries: retries,
		Timeout:    2 * time.Second,
		Log:        slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /{chain}", h.Proxy)
	mux.HandleFunc("OPTIONS /{chain}", h.Options)
	mux.HandleFunc("GET /{chain}", h.MethodHint)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func post(t *testing.T, url, body string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

const rpcReq = `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`

func TestProxySuccess(t *testing.T) {
	want := `{"jsonrpc":"2.0","id":1,"result":"0x64"}`
	up, c := upstream(t, okJSON(want))
	hub := newHub(t, 3, up.URL)

	for _, path := range []string{"/1", "/ethereum", "/eth"} {
		resp, body := post(t, hub.URL+path, rpcReq)
		if resp.StatusCode != 200 || body != want {
			t.Fatalf("POST %s = %d %q", path, resp.StatusCode, body)
		}
		if resp.Header.Get("Access-Control-Allow-Origin") != "*" {
			t.Fatal("CORS header missing")
		}
		if resp.Header.Get("X-Rpchub-Upstream") == "" {
			t.Fatal("X-Rpchub-Upstream missing")
		}
	}
	if c.hits.Load() != 3 {
		t.Fatalf("upstream hits = %d", c.hits.Load())
	}
}

func TestFailoverAcrossBadUpstreams(t *testing.T) {
	want := `{"jsonrpc":"2.0","id":1,"result":"0x1"}`
	bad429, _ := upstream(t, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(429) })
	bad500, _ := upstream(t, func(w http.ResponseWriter, _ *http.Request) { http.Error(w, "boom", 500) })
	badHTML, _ := upstream(t, func(w http.ResponseWriter, _ *http.Request) { io.WriteString(w, "<html>cf</html>") })
	good, gc := upstream(t, okJSON(want))
	hub := newHub(t, 4, bad429.URL, bad500.URL, badHTML.URL, good.URL)

	resp, body := post(t, hub.URL+"/1", rpcReq)
	if resp.StatusCode != 200 || body != want {
		t.Fatalf("resp = %d %q", resp.StatusCode, body)
	}
	if gc.hits.Load() != 1 {
		t.Fatalf("good upstream hits = %d", gc.hits.Load())
	}
}

func TestExhaustedReturnsLastUpstreamResponse(t *testing.T) {
	a, _ := upstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(503)
		io.WriteString(w, `{"error":"overloaded"}`)
	})
	b, _ := upstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(503)
		io.WriteString(w, `{"error":"overloaded"}`)
	})
	hub := newHub(t, 3, a.URL, b.URL)

	resp, body := post(t, hub.URL+"/1", rpcReq)
	if resp.StatusCode != 503 || !strings.Contains(body, "overloaded") {
		t.Fatalf("resp = %d %q, want last upstream 503 passthrough", resp.StatusCode, body)
	}
}

func TestTransportErrorsYield502(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close() // connection refused from now on
	hub := newHub(t, 3, deadURL)

	resp, body := post(t, hub.URL+"/1", rpcReq)
	if resp.StatusCode != 502 {
		t.Fatalf("status = %d, want 502", resp.StatusCode)
	}
	var rpcErr struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if err := json.Unmarshal([]byte(body), &rpcErr); err != nil || rpcErr.Error.Code != -32000 {
		t.Fatalf("body = %q", body)
	}
	if strings.Contains(rpcErr.Error.Message, "127.0.0.1:") && !strings.Contains(rpcErr.Error.Message, "http://127.0.0.1") {
		// host visible is fine; full URL with path would be a leak (none here)
		_ = body
	}
}

func TestBatchPassthrough(t *testing.T) {
	want := `[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"result":"0x2"}]`
	up, _ := upstream(t, func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		if !strings.HasPrefix(strings.TrimSpace(string(body)), "[") {
			t.Errorf("upstream got non-batch body: %q", body)
		}
		okJSON(want)(w, r)
	})
	hub := newHub(t, 3, up.URL)

	resp, body := post(t, hub.URL+"/1", `[{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"},{"jsonrpc":"2.0","id":2,"method":"eth_chainId"}]`)
	if resp.StatusCode != 200 || body != want {
		t.Fatalf("resp = %d %q", resp.StatusCode, body)
	}
}

func TestClientBadRequestPassesThroughWithoutRetry(t *testing.T) {
	a, ca := upstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(400)
		io.WriteString(w, `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse error"}}`)
	})
	b, cb := upstream(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(400)
		io.WriteString(w, `{"jsonrpc":"2.0","id":null,"error":{"code":-32700,"message":"parse error"}}`)
	})
	hub := newHub(t, 3, a.URL, b.URL)

	resp, body := post(t, hub.URL+"/1", `{broken`)
	if resp.StatusCode != 400 || !strings.Contains(body, "-32700") {
		t.Fatalf("resp = %d %q", resp.StatusCode, body)
	}
	if ca.hits.Load()+cb.hits.Load() != 1 {
		t.Fatalf("hits = %d+%d, want exactly 1 (no retry on client error)", ca.hits.Load(), cb.hits.Load())
	}
}

func TestUnknownChain404(t *testing.T) {
	up, _ := upstream(t, okJSON(`{}`))
	hub := newHub(t, 3, up.URL)
	resp, body := post(t, hub.URL+"/137", rpcReq)
	if resp.StatusCode != 404 || !strings.Contains(body, "unknown chain") {
		t.Fatalf("resp = %d %q", resp.StatusCode, body)
	}
}

func TestMethodHintAndPreflight(t *testing.T) {
	up, _ := upstream(t, okJSON(`{}`))
	hub := newHub(t, 3, up.URL)

	resp, err := http.Get(hub.URL + "/1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /1 = %d, want 405", resp.StatusCode)
	}

	req, _ := http.NewRequest(http.MethodOptions, hub.URL+"/1", nil)
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNoContent || resp.Header.Get("Access-Control-Allow-Origin") != "*" {
		t.Fatalf("OPTIONS /1 = %d, CORS %q", resp.StatusCode, resp.Header.Get("Access-Control-Allow-Origin"))
	}
}
