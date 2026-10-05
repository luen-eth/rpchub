package proxy

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"rpchub/internal/pool"
	"rpchub/internal/registry"
	"rpchub/internal/wstest"
	"rpchub/internal/wsutil"
)

// newWSHub wires a registry + ws pool over the given upstream ws:// URLs and
// serves the real routes, so tests exercise the same dispatch main.go uses.
func newWSHub(t *testing.T, retries, max int, wsURLs ...string) (*httptest.Server, *pool.Set) {
	t.Helper()
	rpcs := []registry.RPCEntry{{URL: "https://eth-http.example"}}
	for _, u := range wsURLs {
		rpcs = append(rpcs, registry.RPCEntry{URL: u})
	}
	entries := []registry.ChainEntry{{
		Name: "Ethereum Mainnet", ChainID: 1, ChainSlug: "ethereum", ShortName: "eth", RPC: rpcs,
	}}
	reg := registry.New()
	if err := reg.Update(entries, registry.BuildOptions{
		ChainIDs: []int64{1}, AllowHTTP: true, WSEnabled: true, SolanaEnabled: true,
	}); err != nil {
		t.Fatal(err)
	}
	ch, _ := reg.Resolve("1")

	pools, wsPools := pool.NewSet(), pool.NewSet()
	pools.Ensure("1", 10).SetEndpoints(ch.Endpoints)
	wsPools.Ensure("1", 10).SetEndpoints(ch.WSEndpoints)
	for _, u := range ch.WSEndpoints {
		pl, _ := wsPools.Get("1")
		pl.SetVerified(u)
		pl.ReportSuccess(u, time.Millisecond, 100)
	}
	wsPools.Ensure("solana", 200).SetEndpoints(nil)

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	h := &Handler{Reg: reg, Pools: pools, Client: &http.Client{}, MaxRetries: retries, Timeout: 2 * time.Second, Log: log}
	ws := &WSHandler{Reg: reg, Pools: wsPools, Retries: retries, Timeout: 2 * time.Second, Max: max, Log: log}

	mux := http.NewServeMux()
	mux.HandleFunc("GET /{chain}", GetHandler(h, ws))
	mux.HandleFunc("GET /{chain}/ws", ws.HandleExplicit)
	mux.HandleFunc("POST /{chain}", h.Proxy)
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv, wsPools
}

// rpcCall opens a WebSocket through the hub and does one JSON-RPC round trip.
func rpcCall(t *testing.T, hubURL, path, req string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := wsutil.Dial(ctx, "ws"+strings.TrimPrefix(hubURL, "http")+path, 5*time.Second)
	if err != nil {
		t.Fatalf("dial %s: %v", path, err)
	}
	defer c.Close()
	if err := c.WriteText([]byte(req)); err != nil {
		t.Fatal(err)
	}
	msg, err := c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	return string(msg)
}

func TestWSRelayRoundTrip(t *testing.T) {
	upstream := wstest.NewRPCServer(t, map[string]string{"eth_blockNumber": `"0x64"`})
	hub, _ := newWSHub(t, 3, 10, wstest.URL(upstream))

	// Both the shared path and the explicit /ws path must relay.
	for _, path := range []string{"/1", "/ethereum", "/eth", "/1/ws"} {
		got := rpcCall(t, hub.URL, path, `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`)
		if !strings.Contains(got, `"0x64"`) {
			t.Fatalf("relay via %s = %s", path, got)
		}
	}
}

// TestWSRelayIsBidirectionalAndStreams covers the subscription shape: the
// upstream pushes several messages the client never individually requested.
func TestWSRelayStreamsPushedMessages(t *testing.T) {
	upstream := wstest.NewServer(t, func(msg []byte) []byte {
		return []byte(`{"jsonrpc":"2.0","method":"eth_subscription","params":{"result":"block"}}`)
	})
	hub, _ := newWSHub(t, 3, 10, wstest.URL(upstream))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := wsutil.Dial(ctx, "ws"+strings.TrimPrefix(hub.URL, "http")+"/1", 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for i := 0; i < 5; i++ {
		if err := c.WriteText([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_subscribe","params":["newHeads"]}`)); err != nil {
			t.Fatal(err)
		}
		msg, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("message %d: %v", i, err)
		}
		if !strings.Contains(string(msg), "eth_subscription") {
			t.Fatalf("message %d = %s", i, msg)
		}
	}
}

func TestWSRelayFailsOverAtConnectTime(t *testing.T) {
	dead := wstest.NewRejectingServer(t, http.StatusBadGateway)
	limited := wstest.NewRejectingServer(t, http.StatusTooManyRequests)
	good := wstest.NewRPCServer(t, map[string]string{"eth_chainId": `"0x1"`})
	hub, wsPools := newWSHub(t, 3, 10, wstest.URL(dead), wstest.URL(limited), wstest.URL(good))

	// Force the pick order: both refusing endpoints look healthy, so they are
	// tried first, and the good one is a slower, verified backup.
	pl, _ := wsPools.Get("1")
	pl.ReportSuccess(wstest.URL(dead), 5*time.Millisecond, 1000)
	pl.ReportSuccess(wstest.URL(limited), 5*time.Millisecond, 1000)
	pl.ReportSuccess(wstest.URL(good), 50*time.Millisecond, 1000)

	got := rpcCall(t, hub.URL, "/1", `{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}`)
	if !strings.Contains(got, `"0x1"`) {
		t.Fatalf("relay = %s", got)
	}
	// Both refusing endpoints must be scored down, not silently skipped.
	refusing := map[string]bool{wstest.URL(dead): true, wstest.URL(limited): true}
	for _, ep := range pl.Snapshot(true).Endpoints {
		if refusing[ep.URL] && ep.TotalFail != 1 {
			t.Errorf("refusing endpoint %s: TotalFail = %d, want 1", ep.URL, ep.TotalFail)
		}
	}
}

func TestWSRelayAllUpstreamsDown(t *testing.T) {
	dead := wstest.NewRejectingServer(t, http.StatusBadGateway)
	hub, _ := newWSHub(t, 3, 10, wstest.URL(dead))

	ctx := context.Background()
	_, err := wsutil.Dial(ctx, "ws"+strings.TrimPrefix(hub.URL, "http")+"/1", 3*time.Second)
	if err == nil {
		t.Fatal("want handshake failure when every upstream refuses")
	}
	if !strings.Contains(err.Error(), "502") {
		t.Fatalf("err = %v, want 502 from rpchub", err)
	}
}

func TestWSChainWithoutWebSocketEndpoints(t *testing.T) {
	upstream := wstest.NewRPCServer(t, map[string]string{"eth_chainId": `"0x1"`})
	hub, _ := newWSHub(t, 3, 10, wstest.URL(upstream))

	// Solana is enabled in this registry but has no ws endpoints in the pool.
	_, err := wsutil.Dial(context.Background(), "ws"+strings.TrimPrefix(hub.URL, "http")+"/solana", 3*time.Second)
	if err == nil || !strings.Contains(err.Error(), "503") {
		t.Fatalf("err = %v, want 503", err)
	}
}

func TestWSConnectionLimit(t *testing.T) {
	upstream := wstest.NewRPCServer(t, map[string]string{"eth_chainId": `"0x1"`})
	hub, _ := newWSHub(t, 3, 1, wstest.URL(upstream)) // cap of one

	ctx := context.Background()
	first, err := wsutil.Dial(ctx, "ws"+strings.TrimPrefix(hub.URL, "http")+"/1", 3*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()

	if _, err := wsutil.Dial(ctx, "ws"+strings.TrimPrefix(hub.URL, "http")+"/1", 3*time.Second); err == nil {
		t.Fatal("second connection should be refused by the cap")
	} else if !strings.Contains(err.Error(), "503") {
		t.Fatalf("err = %v, want 503", err)
	}
}

func TestPlainGetStillGetsHint(t *testing.T) {
	upstream := wstest.NewRPCServer(t, map[string]string{"eth_chainId": `"0x1"`})
	hub, _ := newWSHub(t, 3, 10, wstest.URL(upstream))

	resp, err := http.Get(hub.URL + "/1")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("GET /1 = %d, want 405 hint (no upgrade header)", resp.StatusCode)
	}

	resp, err = http.Get(hub.URL + "/1/ws")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusUpgradeRequired {
		t.Fatalf("GET /1/ws = %d, want 426", resp.StatusCode)
	}
}

func TestWSUnknownChain(t *testing.T) {
	upstream := wstest.NewRPCServer(t, map[string]string{"eth_chainId": `"0x1"`})
	hub, _ := newWSHub(t, 3, 10, wstest.URL(upstream))

	_, err := wsutil.Dial(context.Background(), "ws"+strings.TrimPrefix(hub.URL, "http")+"/137", 3*time.Second)
	if err == nil || !strings.Contains(err.Error(), "404") {
		t.Fatalf("err = %v, want 404", err)
	}
}
