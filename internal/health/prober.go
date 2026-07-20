package health

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sync"
	"time"

	"rpchub/internal/pool"
	"rpchub/internal/registry"
)

const (
	probeTimeout     = 7 * time.Second
	probeConcurrency = 8
	maxProbeBody     = 1 << 20
)

// Prober periodically checks every endpoint of one pool: a one-time chain
// identity verification (wrong-chain endpoints are permanently excluded),
// then height/latency probes that drive the pool's scoring.
type Prober struct {
	pool     *pool.Pool
	adapter  Adapter
	client   *http.Client
	interval time.Duration
	log      *slog.Logger
}

func NewProber(pl *pool.Pool, ad Adapter, client *http.Client, interval time.Duration, log *slog.Logger) *Prober {
	return &Prober{pool: pl, adapter: ad, client: client, interval: interval, log: log}
}

// Run sweeps immediately, then on every tick until ctx is done.
func (p *Prober) Run(ctx context.Context) {
	p.Sweep(ctx)
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.Sweep(ctx)
		}
	}
}

// Sweep probes all endpoints of the pool with bounded concurrency.
func (p *Prober) Sweep(ctx context.Context) {
	sem := make(chan struct{}, probeConcurrency)
	var wg sync.WaitGroup
	for _, u := range p.pool.Endpoints() {
		if ctx.Err() != nil {
			break
		}
		sem <- struct{}{}
		wg.Add(1)
		go func(u string) {
			defer wg.Done()
			defer func() { <-sem }()
			p.probeOne(ctx, u)
		}(u)
	}
	wg.Wait()
}

func (p *Prober) probeOne(ctx context.Context, u string) {
	if p.pool.NeedsVerify(u) {
		body, _, err := p.post(ctx, u, p.adapter.IdentityRequest())
		if err != nil {
			p.pool.ReportFailure(u, "identity: "+errString(err))
			return
		}
		match, err := p.adapter.VerifyIdentity(body)
		if err != nil {
			p.pool.ReportFailure(u, "identity: "+errString(err))
			return
		}
		if !match {
			p.pool.MarkWrongChain(u, "serves a different chain")
			p.log.Warn("endpoint serves wrong chain, permanently excluded",
				"chain", p.pool.Key(), "endpoint", registry.RedactURL(u))
			return
		}
		p.pool.SetVerified(u)
	}

	body, dur, err := p.post(ctx, u, p.adapter.ProbeRequest())
	if err != nil {
		p.pool.ReportFailure(u, errString(err))
		return
	}
	height, err := p.adapter.ParseHeight(body)
	if err != nil {
		p.pool.ReportFailure(u, errString(err))
		return
	}
	p.pool.ReportSuccess(u, dur, height)
	p.log.Debug("probe ok", "chain", p.pool.Key(), "endpoint", registry.RedactURL(u),
		"latency_ms", dur.Milliseconds(), "height", height)
}

func (p *Prober) post(ctx context.Context, u string, payload []byte) ([]byte, time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(payload))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "rpchub/1.0")
	start := time.Now()
	resp, err := p.client.Do(req)
	dur := time.Since(start)
	if err != nil {
		return nil, dur, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, maxProbeBody))
	if err != nil {
		return nil, dur, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, dur, fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	return body, dur, nil
}

// errString renders an error without leaking full endpoint URLs (which can
// embed API keys) into pool state or logs.
func errString(err error) string {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Sprintf("%s %s: %v", ue.Op, registry.RedactURL(ue.URL), ue.Err)
	}
	return err.Error()
}
