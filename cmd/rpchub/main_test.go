package main

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"rpchub/internal/config"
	"rpchub/internal/registry"
)

const staticRegistryFixture = `[{"name":"Base","chainId":8453,"chainSlug":"base","shortName":"base","rpc":[]}]`
const remoteRegistryFixture = `[{"name":"Remote Base","chainId":8453,"chainSlug":"base","shortName":"base","rpc":[{"url":"https://public-provider.example"}]}]`

func TestStaticRegistryStartupNeverFetchesOrFallsBackToCache(t *testing.T) {
	var requests atomic.Int64
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, remoteRegistryFixture)
	}))
	defer remote.Close()
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	for _, tc := range []struct {
		name    string
		body    string
		missing bool
		wantErr bool
	}{
		{name: "valid", body: staticRegistryFixture},
		{name: "missing", missing: true, wantErr: true},
		{name: "empty", wantErr: true},
		{name: "malformed", body: "not-json", wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			filename := filepath.Join(dir, "explicit.json")
			if !tc.missing {
				if err := os.WriteFile(filename, []byte(tc.body), 0o600); err != nil {
					t.Fatal(err)
				}
			}
			cacheDir := filepath.Join(dir, "cache")
			if err := registry.SaveCache(cacheDir, []byte(remoteRegistryFixture)); err != nil {
				t.Fatal(err)
			}
			cfg := &config.Config{
				ChainIDs: []int64{8453}, ChainlistFile: filename,
				ChainlistURL: remote.URL, CacheDir: cacheDir,
			}
			entries, err := loadChainEntries(context.Background(), cfg, remote.Client(), log)
			if (err != nil) != tc.wantErr {
				t.Fatalf("error = %v; expected error = %v", err, tc.wantErr)
			}
			if !tc.wantErr && (len(entries) != 1 || entries[0].Name != "Base" || len(entries[0].RPC) != 0) {
				t.Fatalf("unexpected local registry result: %+v", entries)
			}
			cached, err := registry.LoadCache(cacheDir)
			if err != nil || len(cached) != 1 || cached[0].Name != "Remote Base" {
				t.Fatalf("static loading changed the remote cache: %+v, %v", cached, err)
			}
		})
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("static mode made %d remote requests", got)
	}
}

func TestStaticRegistryDisablesPeriodicRemoteRefresh(t *testing.T) {
	var requests atomic.Int64
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, remoteRegistryFixture)
	}))
	defer remote.Close()
	var updates int
	cfg := &config.Config{
		ChainIDs: []int64{8453}, ChainlistFile: filepath.Join(t.TempDir(), "pinned.json"),
		ChainlistURL: remote.URL, CacheDir: t.TempDir(), RefreshInterval: time.Millisecond,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	refreshLoop(ctx, cfg, remote.Client(), registry.New(), registry.BuildOptions{ChainIDs: []int64{8453}}, func() { updates++ }, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if requests.Load() != 0 || updates != 0 {
		t.Fatalf("static registry refreshed remotely: requests=%d updates=%d", requests.Load(), updates)
	}
}

func TestUnsetLocalRegistryRetainsRemoteSource(t *testing.T) {
	var requests atomic.Int64
	remote := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		_, _ = io.WriteString(w, remoteRegistryFixture)
	}))
	defer remote.Close()
	cfg := &config.Config{ChainIDs: []int64{8453}, ChainlistURL: remote.URL, CacheDir: t.TempDir()}
	entries, err := loadChainEntries(context.Background(), cfg, remote.Client(), slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil || requests.Load() != 1 || len(entries) != 1 || entries[0].Name != "Remote Base" {
		t.Fatalf("remote source no longer works: requests=%d entries=%+v err=%v", requests.Load(), entries, err)
	}
}
