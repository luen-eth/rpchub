package registry

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

// fixture mirrors real chainlist dirt: zero-width chars, placeholders,
// websockets, garbage strings, trailing slashes, tracking flags.
func fixtureEntries() []ChainEntry {
	return []ChainEntry{
		{Name: "Ethereum Mainnet", ChainID: 1, ChainSlug: "ethereum", ShortName: "eth", RPC: []RPCEntry{
			{URL: "https://eth-rpc.example", Tracking: "none"},
			{URL: "​https://zw.example"},               // zero-width prefix (seen live)
			{URL: "https://key.example/v2/${API_KEY}"}, // placeholder
			{URL: "wss://ws.example"},                  // websocket
			{URL: "rpcWorking"},                        // garbage (seen live)
			{URL: "http://plain.example"},              // http, off by default
			{URL: "https://eth-rpc.example/"},          // dupe after normalize
			{URL: "https://tracked.example", Tracking: "yes"},
		}},
		{Name: "BNB Smart Chain Mainnet", ChainID: 56, ChainSlug: "binance", ShortName: "bnb", RPC: []RPCEntry{
			{URL: "https://bsc.example", Tracking: "none"},
		}},
	}
}

func TestCleanURL(t *testing.T) {
	cases := []struct {
		in        string
		allowHTTP bool
		want      string
		ok        bool
	}{
		{"https://a.example/", false, "https://a.example", true},
		{"HTTPS://A.example/Path/", false, "https://a.example/Path", true},
		{"​https://a.example", false, "https://a.example", true},
		{" https://a.example ", false, "https://a.example", true},
		{"https://a.example/v2/${KEY}", false, "", false},
		{"wss://a.example", false, "", false},
		{"http://a.example", false, "", false},
		{"http://a.example", true, "http://a.example", true},
		{"rpcWorking", false, "", false},
		{"website", true, "", false},
		{"", false, "", false},
	}
	for _, c := range cases {
		got, ok := CleanURL(c.in, c.allowHTTP)
		if got != c.want || ok != c.ok {
			t.Errorf("CleanURL(%q, %v) = (%q, %v), want (%q, %v)", c.in, c.allowHTTP, got, ok, c.want, c.ok)
		}
	}
}

func TestUpdateSanitizesAndIndexes(t *testing.T) {
	r := New()
	err := r.Update(fixtureEntries(), BuildOptions{
		ChainIDs:  []int64{1, 56},
		ExtraRPCs: map[string][]string{"1": {"http://my-node.local:8545"}},
		Aliases:   map[string]string{"bsc": "56"},
	})
	if err != nil {
		t.Fatal(err)
	}

	ch, ok := r.Resolve("1")
	if !ok {
		t.Fatal("chain 1 not resolvable")
	}
	want := []string{
		"http://my-node.local:8545", // extra first, http allowed for user nodes
		"https://eth-rpc.example",
		"https://zw.example",
		"https://tracked.example",
	}
	if !reflect.DeepEqual(ch.Endpoints, want) {
		t.Errorf("endpoints = %v, want %v", ch.Endpoints, want)
	}

	for _, tok := range []string{"1", "ethereum", "eth", "ETH", " eth "} {
		if got, ok := r.Resolve(tok); !ok || got.Key != "1" {
			t.Errorf("Resolve(%q) = %v, %v; want chain 1", tok, got, ok)
		}
	}
	for _, tok := range []string{"56", "binance", "bnb", "bsc"} {
		if got, ok := r.Resolve(tok); !ok || got.Key != "56" {
			t.Errorf("Resolve(%q): want chain 56", tok)
		}
	}
	if _, ok := r.Resolve("137"); ok {
		t.Error("Resolve(137) should fail: not enabled")
	}
	toks := r.Tokens("56")
	if !reflect.DeepEqual(toks, []string{"binance", "bnb", "bsc"}) {
		t.Errorf("Tokens(56) = %v", toks)
	}
}

func TestUpdateFilterTracking(t *testing.T) {
	r := New()
	if err := r.Update(fixtureEntries(), BuildOptions{ChainIDs: []int64{1}, FilterTracking: true}); err != nil {
		t.Fatal(err)
	}
	ch, _ := r.Resolve("1")
	if !reflect.DeepEqual(ch.Endpoints, []string{"https://eth-rpc.example"}) {
		t.Errorf("endpoints = %v, want only tracking=none", ch.Endpoints)
	}
}

func TestUpdateAllowHTTP(t *testing.T) {
	r := New()
	if err := r.Update(fixtureEntries(), BuildOptions{ChainIDs: []int64{1}, AllowHTTP: true}); err != nil {
		t.Fatal(err)
	}
	ch, _ := r.Resolve("1")
	found := false
	for _, e := range ch.Endpoints {
		if e == "http://plain.example" {
			found = true
		}
	}
	if !found {
		t.Errorf("http endpoint missing with AllowHTTP: %v", ch.Endpoints)
	}
}

func TestUpdateMissingChainKeepsOldState(t *testing.T) {
	r := New()
	if err := r.Update(fixtureEntries(), BuildOptions{ChainIDs: []int64{1}}); err != nil {
		t.Fatal(err)
	}
	if err := r.Update(fixtureEntries(), BuildOptions{ChainIDs: []int64{1, 999}}); err == nil {
		t.Fatal("want error for unknown chain 999")
	}
	if _, ok := r.Resolve("ethereum"); !ok {
		t.Error("old state lost after failed update")
	}
}

func TestUpdateSolana(t *testing.T) {
	r := New()
	err := r.Update(nil, BuildOptions{
		SolanaEnabled: true,
		SolanaRPCs:    []string{"https://user-sol.example"},
		ExtraRPCs:     map[string][]string{SolanaKey: {"https://prio-sol.example"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, tok := range []string{"solana", "sol"} {
		ch, ok := r.Resolve(tok)
		if !ok || ch.Kind != KindSolana {
			t.Fatalf("Resolve(%q) failed", tok)
		}
	}
	ch, _ := r.Resolve("solana")
	if ch.Endpoints[0] != "https://prio-sol.example" || ch.Endpoints[1] != "https://user-sol.example" {
		t.Errorf("solana endpoint priority wrong: %v", ch.Endpoints)
	}
	if len(ch.Endpoints) != 2+len(DefaultSolanaRPCs) {
		t.Errorf("defaults missing: %v", ch.Endpoints)
	}
}

func TestLoadSourceFallsBackToCache(t *testing.T) {
	dir := t.TempDir()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	good := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Write([]byte(`[{"name":"Ethereum Mainnet","chainId":1,"chainSlug":"ethereum","shortName":"eth","rpc":[{"url":"https://x.example"}]}]`))
	}))
	defer good.Close()

	entries, err := LoadSource(context.Background(), good.Client(), good.URL, dir, log)
	if err != nil || len(entries) != 1 {
		t.Fatalf("LoadSource via network: %v, %d entries", err, len(entries))
	}
	if _, err := os.Stat(filepath.Join(dir, cacheFileName)); err != nil {
		t.Fatalf("cache not written: %v", err)
	}

	bad := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer bad.Close()

	entries, err = LoadSource(context.Background(), bad.Client(), bad.URL, dir, log)
	if err != nil || len(entries) != 1 {
		t.Fatalf("LoadSource via cache: %v, %d entries", err, len(entries))
	}

	if _, err := LoadSource(context.Background(), bad.Client(), bad.URL, t.TempDir(), log); err == nil {
		t.Fatal("want error when both network and cache fail")
	}
}
