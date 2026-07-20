// Package pool tracks per-endpoint health for one chain and picks the best
// endpoint per request: P2C by EMA latency among healthy endpoints, a
// circuit breaker with exponential cooldown, and exclusion of endpoints
// lagging behind the pool's median block height.
package pool

import (
	"math/rand/v2"
	"sort"
	"sync"
	"time"
)

const (
	failThreshold = 3 // consecutive failures before cooldown starts
	baseCooldown  = time.Minute
	maxCooldown   = 10 * time.Minute
	emaAlpha      = 0.3
	neutralEMA    = 150.0 // ms, assumed for endpoints with a single sample pending

	// archiveRecheck bounds how long an archive verdict is trusted: public
	// endpoints often sit behind load balancers mixing archive and pruned
	// nodes, so the answer can change.
	archiveRecheck = time.Hour
)

// Archive detection verdict per endpoint.
const (
	archiveUnknown uint8 = iota
	archiveYes
	archiveNo
)

type endpoint struct {
	url           string
	alive         bool // has a recent successful probe/request
	verified      bool // chain identity confirmed (eth_chainId / genesis hash)
	wrongChain    bool // permanently excluded: serves a different chain
	emaMS         float64
	consecFails   int
	height        uint64
	cooldownUntil time.Time
	lastErr       string
	lastOK        time.Time
	totalOK       uint64
	totalFail     uint64
	archiveState  uint8
	archiveAt     time.Time // when archiveState was last decided
}

// Pool is the endpoint set for a single chain. All methods are safe for
// concurrent use.
type Pool struct {
	mu   sync.Mutex
	key  string
	lag  uint64
	eps  map[string]*endpoint
	list []*endpoint // stable order: registry priority (user extras first)

	now  func() time.Time
	rand func(n int) int
}

func New(key string, lag uint64) *Pool {
	return &Pool{
		key:  key,
		lag:  lag,
		eps:  map[string]*endpoint{},
		now:  time.Now,
		rand: rand.IntN,
	}
}

func (p *Pool) Key() string { return p.key }

// SetEndpoints replaces the endpoint list, preserving state for URLs that
// remain. Called at boot and after every chainlist refresh.
func (p *Pool) SetEndpoints(urls []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	eps := make(map[string]*endpoint, len(urls))
	list := make([]*endpoint, 0, len(urls))
	for _, u := range urls {
		if _, dup := eps[u]; dup {
			continue
		}
		ep, ok := p.eps[u]
		if !ok {
			ep = &endpoint{url: u}
		}
		eps[u] = ep
		list = append(list, ep)
	}
	p.eps = eps
	p.list = list
}

// Endpoints returns the URLs in priority order (for the prober).
func (p *Pool) Endpoints() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]string, len(p.list))
	for i, ep := range p.list {
		out[i] = ep.url
	}
	return out
}

// NeedsVerify reports whether the endpoint still needs a chain-identity check.
func (p *Pool) NeedsVerify(url string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	ep, ok := p.eps[url]
	return ok && !ep.verified && !ep.wrongChain
}

// SetVerified marks the endpoint's chain identity as confirmed.
func (p *Pool) SetVerified(url string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ep, ok := p.eps[url]; ok {
		ep.verified = true
	}
}

// NeedsArchiveCheck reports whether an archive-capability probe is due for
// the endpoint (never checked, or the last verdict is older than
// archiveRecheck).
func (p *Pool) NeedsArchiveCheck(url string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	ep, ok := p.eps[url]
	if !ok || ep.wrongChain {
		return false
	}
	return ep.archiveState == archiveUnknown || p.now().Sub(ep.archiveAt) >= archiveRecheck
}

// SetArchive records the archive-capability verdict for an endpoint.
func (p *Pool) SetArchive(url string, isArchive bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ep, ok := p.eps[url]
	if !ok {
		return
	}
	if isArchive {
		ep.archiveState = archiveYes
	} else {
		ep.archiveState = archiveNo
	}
	ep.archiveAt = p.now()
}

// MarkWrongChain permanently excludes an endpoint that answered for a
// different chain (wrong eth_chainId / genesis hash).
func (p *Pool) MarkWrongChain(url, detail string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ep, ok := p.eps[url]; ok {
		ep.wrongChain = true
		ep.alive = false
		ep.lastErr = detail
	}
}

// ReportSuccess records a successful probe or proxied request. height 0 means
// "unknown" (proxied requests don't parse heights) and leaves it untouched.
func (p *Pool) ReportSuccess(url string, latency time.Duration, height uint64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ep, ok := p.eps[url]
	if !ok {
		return
	}
	ms := float64(latency) / float64(time.Millisecond)
	if ep.emaMS == 0 {
		ep.emaMS = ms
	} else {
		ep.emaMS = emaAlpha*ms + (1-emaAlpha)*ep.emaMS
	}
	ep.alive = true
	ep.consecFails = 0
	ep.cooldownUntil = time.Time{}
	if height > 0 {
		ep.height = height
	}
	ep.lastOK = p.now()
	ep.totalOK++
}

// ReportFailure records a failed probe or proxied request and trips the
// breaker after failThreshold consecutive failures.
func (p *Pool) ReportFailure(url, errMsg string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ep, ok := p.eps[url]
	if !ok {
		return
	}
	ep.totalFail++
	ep.consecFails++
	ep.lastErr = errMsg
	if ep.consecFails >= failThreshold {
		ep.alive = false
		shift := min(ep.consecFails-failThreshold, 4)
		d := baseCooldown << shift
		if d > maxCooldown {
			d = maxCooldown
		}
		ep.cooldownUntil = p.now().Add(d)
	}
}

// Pick selects an endpoint, excluding the given URLs (already tried in this
// request). Preference: healthy non-lagging endpoints via P2C on EMA latency;
// if none, never-probed endpoints (cold start); known-bad ones are skipped.
// With archiveOnly, only endpoints positively verified as archive-capable
// qualify — unknowns are never served on the archive path.
func (p *Pool) Pick(exclude map[string]bool, archiveOnly bool) (string, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	ref := p.heightRefLocked(now)

	var prime, fallback []*endpoint
	for _, ep := range p.list {
		if ep.wrongChain || exclude[ep.url] || now.Before(ep.cooldownUntil) {
			continue
		}
		if archiveOnly && ep.archiveState != archiveYes {
			continue
		}
		switch {
		case ep.alive && !p.laggingLocked(ep, ref):
			prime = append(prime, ep)
		case !ep.alive && ep.height == 0:
			// Never proven yet: usable as last resort so cold boots serve
			// traffic before the first probe sweep finishes.
			fallback = append(fallback, ep)
		}
	}
	cands := prime
	if len(cands) == 0 {
		cands = fallback
	}
	switch len(cands) {
	case 0:
		return "", false
	case 1:
		return cands[0].url, true
	}
	a := p.rand(len(cands))
	b := p.rand(len(cands) - 1)
	if b >= a {
		b++
	}
	if effEMA(cands[b]) < effEMA(cands[a]) {
		return cands[b].url, true
	}
	return cands[a].url, true
}

// heightRefLocked returns the median height of live endpoints. The median
// (not max) keeps one endpoint that reports a bogus huge height from marking
// every honest endpoint as lagging.
func (p *Pool) heightRefLocked(now time.Time) uint64 {
	heights := make([]uint64, 0, len(p.list))
	for _, ep := range p.list {
		if ep.alive && !ep.wrongChain && !now.Before(ep.cooldownUntil) && ep.height > 0 {
			heights = append(heights, ep.height)
		}
	}
	if len(heights) == 0 {
		return 0
	}
	sort.Slice(heights, func(i, j int) bool { return heights[i] < heights[j] })
	return heights[len(heights)/2]
}

func (p *Pool) laggingLocked(ep *endpoint, ref uint64) bool {
	return ref > 0 && ep.height > 0 && ep.height+p.lag < ref
}

func effEMA(ep *endpoint) float64 {
	if ep.emaMS == 0 {
		return neutralEMA
	}
	return ep.emaMS
}

// EndpointSnapshot is the observable state of one endpoint.
type EndpointSnapshot struct {
	URL         string    `json:"url"`
	Status      string    `json:"status"`            // healthy|lagging|cooldown|unproven|wrong_chain
	Archive     *bool     `json:"archive,omitempty"` // nil = not determined yet
	LatencyMS   int       `json:"latency_ms"`
	Height      uint64    `json:"height,omitempty"`
	ConsecFails int       `json:"consecutive_fails,omitempty"`
	LastError   string    `json:"last_error,omitempty"`
	LastOK      time.Time `json:"last_ok,omitempty"`
	TotalOK     uint64    `json:"total_ok"`
	TotalFail   uint64    `json:"total_fail"`
}

// Snapshot is the observable state of a pool.
type Snapshot struct {
	Key            string             `json:"chain"`
	RefHeight      uint64             `json:"ref_height,omitempty"`
	Healthy        int                `json:"healthy"`
	ArchiveHealthy int                `json:"archive_healthy"`
	Total          int                `json:"total"`
	Endpoints      []EndpointSnapshot `json:"endpoints,omitempty"`
}

// Snapshot returns the pool state for the ops API.
func (p *Pool) Snapshot(withEndpoints bool) Snapshot {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	ref := p.heightRefLocked(now)
	s := Snapshot{Key: p.key, RefHeight: ref, Total: len(p.list)}
	for _, ep := range p.list {
		status := "healthy"
		switch {
		case ep.wrongChain:
			status = "wrong_chain"
		case now.Before(ep.cooldownUntil):
			status = "cooldown"
		case !ep.alive:
			status = "unproven"
		case p.laggingLocked(ep, ref):
			status = "lagging"
		}
		if status == "healthy" {
			s.Healthy++
			if ep.archiveState == archiveYes {
				s.ArchiveHealthy++
			}
		}
		var arch *bool
		if ep.archiveState != archiveUnknown {
			v := ep.archiveState == archiveYes
			arch = &v
		}
		if withEndpoints {
			s.Endpoints = append(s.Endpoints, EndpointSnapshot{
				URL:         ep.url,
				Status:      status,
				Archive:     arch,
				LatencyMS:   int(ep.emaMS),
				Height:      ep.height,
				ConsecFails: ep.consecFails,
				LastError:   ep.lastErr,
				LastOK:      ep.lastOK,
				TotalOK:     ep.totalOK,
				TotalFail:   ep.totalFail,
			})
		}
	}
	return s
}

// HealthyCount returns the number of endpoints currently serving traffic.
func (p *Pool) HealthyCount() int {
	return p.Snapshot(false).Healthy
}
