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
	"rpchub/internal/wsutil"
)

const (
	probeTimeout     = 7 * time.Second
	probeConcurrency = 8
	maxProbeBody     = 1 << 20
)

// roundTripper sends one JSON-RPC payload to an endpoint and returns the raw
// reply plus how long it took. It abstracts over the HTTP and WebSocket
// transports so both endpoint families are scored the same way.
type roundTripper interface {
	roundTrip(ctx context.Context, url string, payload []byte) ([]byte, time.Duration, error)
}

// Prober periodically checks every endpoint of one pool: a one-time chain
// identity verification (wrong-chain endpoints are permanently excluded),
// then height/latency probes that drive the pool's scoring.
type Prober struct {
	pool      *pool.Pool
	adapter   Adapter
	archive   ArchiveProber // nil when the chain kind has no archive concept
	transport roundTripper
	interval  time.Duration
	log       *slog.Logger
}

// NewProber builds a prober for an HTTP endpoint pool.
func NewProber(pl *pool.Pool, ad Adapter, client *http.Client, interval time.Duration, log *slog.Logger) *Prober {
	return newWithTransport(pl, ad, httpTransport{client: client}, interval, log)
}

// NewWSProber builds a prober for a WebSocket endpoint pool. Each probe opens
// a short-lived connection: rpchub does not pool upstream WebSockets, since a
// proxied client gets its own dedicated connection anyway.
//
// Archive detection is deliberately skipped here — the archive pool is served
// over HTTP, and a subscription endpoint is not asked for historical state.
func NewWSProber(pl *pool.Pool, ad Adapter, interval time.Duration, log *slog.Logger) *Prober {
	p := newWithTransport(pl, ad, wsTransport{}, interval, log)
	p.archive = nil
	return p
}

func newWithTransport(pl *pool.Pool, ad Adapter, tr roundTripper, interval time.Duration, log *slog.Logger) *Prober {
	arch, _ := ad.(ArchiveProber)
	return &Prober{pool: pl, adapter: ad, archive: arch, transport: tr, interval: interval, log: log}
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
		body, _, err := p.roundTrip(ctx, u, p.adapter.IdentityRequest())
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

	body, dur, err := p.roundTrip(ctx, u, p.adapter.ProbeRequest())
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
	body, _, err := p.roundTrip(ctx, u, p.archive.ArchiveRequest())
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

func (p *Prober) roundTrip(ctx context.Context, u string, payload []byte) ([]byte, time.Duration, error) {
	ctx, cancel := context.WithTimeout(ctx, probeTimeout)
	defer cancel()
	return p.transport.roundTrip(ctx, u, payload)
}

// httpTransport posts the payload and reads the response body.
type httpTransport struct{ client *http.Client }

func (t httpTransport) roundTrip(ctx context.Context, u string, payload []byte) ([]byte, time.Duration, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(payload))
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", "rpchub/1.0")
	start := time.Now()
	resp, err := t.client.Do(req)
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

// wsTransport opens a WebSocket, sends one request, reads one reply and
// closes. The measured duration includes the handshake, which is exactly the
// cost a connecting client pays.
type wsTransport struct{}

func (wsTransport) roundTrip(ctx context.Context, u string, payload []byte) ([]byte, time.Duration, error) {
	start := time.Now()
	conn, err := wsutil.Dial(ctx, u, probeTimeout)
	if err != nil {
		return nil, time.Since(start), err
	}
	defer conn.Close()
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	}
	if err := conn.WriteText(payload); err != nil {
		return nil, time.Since(start), err
	}
	body, err := conn.ReadMessage()
	dur := time.Since(start)
	if err != nil {
		return nil, dur, err
	}
	return body, dur, nil
}

func errString(err error) string { return registry.ErrString(err) }
