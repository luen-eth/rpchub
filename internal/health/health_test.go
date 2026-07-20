package health

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"rpchub/internal/pool"
)

// evmServer answers eth_chainId with the given id and eth_blockNumber with height.
func evmServer(t *testing.T, chainIDHex, heightHex string) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Method string `json:"method"`
		}
		json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "eth_chainId":
			w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"` + chainIDHex + `"}`))
		case "eth_blockNumber":
			w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"` + heightHex + `"}`))
		default:
			w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}`))
		}
	}))
}

func newProber(t *testing.T, urls ...string) (*Prober, *pool.Pool) {
	t.Helper()
	pl := pool.New("1", 10)
	pl.SetEndpoints(urls)
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	return NewProber(pl, EVM{ChainID: 1}, &http.Client{}, time.Minute, log), pl
}

func TestProbeHealthyEndpoint(t *testing.T) {
	srv := evmServer(t, "0x1", "0x64")
	defer srv.Close()
	p, pl := newProber(t, srv.URL)

	p.Sweep(context.Background())

	s := pl.Snapshot(true)
	ep := s.Endpoints[0]
	if ep.Status != "healthy" || ep.Height != 100 || ep.TotalOK != 1 {
		t.Fatalf("endpoint = %+v", ep)
	}
	if pl.NeedsVerify(srv.URL) {
		t.Fatal("endpoint should be verified after first sweep")
	}

	p.Sweep(context.Background()) // second sweep: no identity call needed, still healthy
	if s := pl.Snapshot(false); s.Healthy != 1 {
		t.Fatalf("healthy = %d", s.Healthy)
	}
}

func TestProbeWrongChainPermanentlyExcluded(t *testing.T) {
	srv := evmServer(t, "0x38", "0x64") // BSC answering on the "ethereum" pool
	defer srv.Close()
	p, pl := newProber(t, srv.URL)

	p.Sweep(context.Background())

	if s := pl.Snapshot(true); s.Endpoints[0].Status != "wrong_chain" {
		t.Fatalf("status = %s, want wrong_chain", s.Endpoints[0].Status)
	}
}

func TestProbeGarbageIsTransientFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte("<html>cloudflare says hi</html>"))
	}))
	defer srv.Close()
	p, pl := newProber(t, srv.URL)

	p.Sweep(context.Background())

	ep := pl.Snapshot(true).Endpoints[0]
	if ep.Status == "wrong_chain" {
		t.Fatal("garbage must not be treated as wrong chain")
	}
	if ep.TotalFail != 1 {
		t.Fatalf("endpoint = %+v, want one failure", ep)
	}
}

func TestProbeHTTPErrorAndRPCError(t *testing.T) {
	rl := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer rl.Close()
	rpcErr := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"limit"}}`))
	}))
	defer rpcErr.Close()

	p, pl := newProber(t, rl.URL, rpcErr.URL)
	p.Sweep(context.Background())

	for _, ep := range pl.Snapshot(true).Endpoints {
		if ep.TotalFail != 1 || ep.Status == "healthy" {
			t.Fatalf("endpoint = %+v, want failed", ep)
		}
	}
}

func TestSolanaAdapterParsing(t *testing.T) {
	s := Solana{}
	if h, err := s.ParseHeight([]byte(`{"jsonrpc":"2.0","id":1,"result":250000000}`)); err != nil || h != 250000000 {
		t.Errorf("ParseHeight = %d, %v", h, err)
	}
	if _, err := s.ParseHeight([]byte(`{"result":"not-a-slot"}`)); err == nil {
		t.Error("string slot must error")
	}
	if match, err := s.VerifyIdentity([]byte(`{"result":"` + solanaMainnetGenesis + `"}`)); err != nil || !match {
		t.Errorf("mainnet genesis: match=%v err=%v", match, err)
	}
	// devnet genesis hash must be a confirmed mismatch, not a transient error
	if match, err := s.VerifyIdentity([]byte(`{"result":"EtWTRABZaYq6iMfeYKouRu166VU2xqa1wcaWoxPkrZBG"}`)); err != nil || match {
		t.Errorf("devnet genesis: match=%v err=%v", match, err)
	}
	if s.LagLimit(10) != 200 {
		t.Errorf("LagLimit(10) = %d", s.LagLimit(10))
	}
}

func TestSolanaProbeEndToEnd(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Method string `json:"method"`
		}
		json.Unmarshal(body, &req)
		w.Header().Set("Content-Type", "application/json")
		switch req.Method {
		case "getGenesisHash":
			w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"` + solanaMainnetGenesis + `"}`))
		case "getSlot":
			w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":250000000}`))
		default:
			w.Write([]byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"nope"}}`))
		}
	}))
	defer srv.Close()

	pl := pool.New("solana", 200)
	pl.SetEndpoints([]string{srv.URL})
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	NewProber(pl, Solana{}, &http.Client{}, time.Minute, log).Sweep(context.Background())

	ep := pl.Snapshot(true).Endpoints[0]
	if ep.Status != "healthy" || ep.Height != 250000000 {
		t.Fatalf("endpoint = %+v", ep)
	}
}

func TestEVMAdapterParsing(t *testing.T) {
	e := EVM{ChainID: 56}
	if h, err := e.ParseHeight([]byte(`{"result":"0x1b4"}`)); err != nil || h != 436 {
		t.Errorf("ParseHeight = %d, %v", h, err)
	}
	if _, err := e.ParseHeight([]byte(`{"result":"nope"}`)); err == nil {
		t.Error("bad hex must error")
	}
	if _, err := e.ParseHeight([]byte(`not json`)); err == nil {
		t.Error("non-JSON must error")
	}
	if match, err := e.VerifyIdentity([]byte(`{"result":"0x38"}`)); err != nil || !match {
		t.Errorf("VerifyIdentity(0x38) = %v, %v; want match", match, err)
	}
	if match, err := e.VerifyIdentity([]byte(`{"result":"0x1"}`)); err != nil || match {
		t.Errorf("VerifyIdentity(0x1) = %v, %v; want mismatch without error", match, err)
	}
	if _, err := e.VerifyIdentity([]byte(`<html>`)); err == nil {
		t.Error("garbage identity must be a transient error, got nil")
	}
}
