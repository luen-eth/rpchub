package health

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
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
	archive  ArchiveProber // nil when the chain kind has no archive concept
	client   *http.Client
	interval time.Duration
	log      *slog.Logger
}

func NewProber(pl *pool.Pool, ad Adapter, client *http.Client, interval time.Duration, log *slog.Logger) *Prober {
	arch, _ := ad.(ArchiveProber)
	return &Prober{pool: pl, adapter: ad, archive: arch, client: client, interval: interval, log: log}
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

	if p.archive != nil && p.pool.NeedsArchiveCheck(u) {
		p.checkArchive(ctx, u)
	}
}

// checkArchive runs the archive-capability probe on a currently healthy
// endpoint. Transport or garbage failures leave the verdict undecided (the
// next sweep retries); only definitive answers are recorded.
func (p *Prober) checkArchive(ctx context.Context, u string) {
	body, _, err := p.post(ctx, u, p.archive.ArchiveRequest())
	if err != nil {
		return
	}
	isArchive, err := p.archive.InterpretArchive(body)
	if err != nil {
		return
	}
	p.pool.SetArchive(u, isArchive)
	p.log.Debug("archive check", "chain", p.pool.Key(), "endpoint", registry.RedactURL(u), "archive", isArchive)
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

func errString(err error) string { return registry.ErrString(err) }
