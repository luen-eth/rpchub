// Command rpchub serves a unified JSON-RPC endpoint per chain, backed by
// public RPCs from chainlist.org (plus a static list for Solana).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"rpchub/internal/api"
	"rpchub/internal/config"
	"rpchub/internal/health"
	"rpchub/internal/pool"
	"rpchub/internal/proxy"
	"rpchub/internal/registry"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "rpchub:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(os.Environ())
	if err != nil {
		return err
	}
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: cfg.LogLevel}))
	slog.SetDefault(log)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := &http.Client{
		Transport: &http.Transport{
			MaxIdleConns:        100,
			MaxIdleConnsPerHost: 4,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
			ForceAttemptHTTP2:   true,
		},
	}

	opts := registry.BuildOptions{
		ChainIDs:       cfg.ChainIDs,
		AllowHTTP:      cfg.AllowHTTP,
		FilterTracking: cfg.FilterTracking,
		ExtraRPCs:      cfg.ExtraRPCs,
		Aliases:        cfg.Aliases,
		SolanaEnabled:  cfg.SolanaEnabled,
		SolanaRPCs:     cfg.SolanaRPCs,
		WSEnabled:      cfg.WSEnabled,
	}

	reg := registry.New()
	var entries []registry.ChainEntry
	if len(cfg.ChainIDs) > 0 {
		if entries, err = registry.LoadSource(ctx, client, cfg.ChainlistURL, cfg.CacheDir, log); err != nil {
			return err
		}
	}
	if err := reg.Update(entries, opts); err != nil {
		return err
	}

	pools := pool.NewSet()
	wsPools := pool.NewSet() // WebSocket endpoints are scored separately
	syncPools := func() {
		for _, ch := range reg.Chains() {
			lag := adapterFor(ch).LagLimit(cfg.MaxBlockLag)
			pools.Ensure(ch.Key, lag).SetEndpoints(ch.Endpoints)
			if cfg.WSEnabled {
				wsPools.Ensure(ch.Key, lag).SetEndpoints(ch.WSEndpoints)
			}
		}
	}
	syncPools()

	for _, ch := range reg.Chains() {
		pl, _ := pools.Get(ch.Key)
		log.Info("chain enabled", "chain", ch.Key, "name", ch.Name, "kind", ch.Kind.String(),
			"endpoints", len(ch.Endpoints), "ws_endpoints", len(ch.WSEndpoints))
		go health.NewProber(pl, adapterFor(ch), client, cfg.ProbeInterval, log).Run(ctx)
		if wsPl, ok := wsPools.Get(ch.Key); ok && len(ch.WSEndpoints) > 0 {
			go health.NewWSProber(wsPl, adapterFor(ch), cfg.ProbeInterval, log).Run(ctx)
		}
	}

	if len(cfg.ChainIDs) > 0 {
		go refreshLoop(ctx, cfg, client, reg, opts, syncPools, log)
	}

	proxyH := &proxy.Handler{
		Reg:        reg,
		Pools:      pools,
		Client:     client,
		MaxRetries: cfg.MaxRetries,
		Timeout:    cfg.RequestTimeout,
		Log:        log,
	}

	apiS := &api.Server{Reg: reg, Pools: pools, Started: time.Now()}

	mux := http.NewServeMux()
	mux.HandleFunc("POST /{chain}", proxyH.Proxy)
	mux.HandleFunc("OPTIONS /{chain}", proxyH.Options)
	mux.HandleFunc("GET /{chain}", proxyH.MethodHint)
	mux.HandleFunc("POST /{chain}/archive", proxyH.ProxyArchive)
	mux.HandleFunc("OPTIONS /{chain}/archive", proxyH.Options)
	mux.HandleFunc("GET /{chain}/archive", proxyH.ArchiveHint)
	mux.HandleFunc("GET /{chain}/health", apiS.ChainHealth)
	mux.HandleFunc("GET /chains", apiS.Chains)
	mux.HandleFunc("GET /health", apiS.Health)
	mux.HandleFunc("GET /", apiS.Root)

	srv := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.Port),
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	errCh := make(chan error, 1)
	go func() {
		log.Info("rpchub listening", "port", cfg.Port, "chains", len(reg.Chains()))
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
	case err := <-errCh:
		return err
	}
	log.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return srv.Shutdown(shutdownCtx)
}

// refreshLoop refetches the chainlist periodically so new public RPCs join
// the pools (and removed ones leave) without a restart.
func refreshLoop(ctx context.Context, cfg *config.Config, client *http.Client, reg *registry.Registry, opts registry.BuildOptions, syncPools func(), log *slog.Logger) {
	t := time.NewTicker(cfg.RefreshInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		entries, raw, err := registry.FetchChainlist(ctx, client, cfg.ChainlistURL)
		if err != nil {
			log.Warn("chainlist refresh failed, keeping current endpoints", "err", err)
			continue
		}
		if err := registry.SaveCache(cfg.CacheDir, raw); err != nil {
			log.Warn("chainlist cache write failed", "err", err)
		}
		if err := reg.Update(entries, opts); err != nil {
			log.Warn("chainlist refresh rejected, keeping current endpoints", "err", err)
			continue
		}
		syncPools()
		log.Info("chainlist refreshed", "chains", len(reg.Chains()))
	}
}

func adapterFor(ch *registry.Chain) health.Adapter {
	if ch.Kind == registry.KindSolana {
		return health.Solana{}
	}
	return health.EVM{ChainID: ch.ChainID}
}
