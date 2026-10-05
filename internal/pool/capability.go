package pool

import "time"

// CapabilityTTL bounds trust in public providers' changing plans/backends.
const CapabilityTTL = 10 * time.Minute

func (p *Pool) SetPriority(urls []string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, ep := range p.list {
		ep.priority = false
	}
	for _, u := range urls {
		if ep := p.eps[u]; ep != nil {
			ep.priority = true
		}
	}
}

func (p *Pool) NeedsIndexerCheck(u string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	ep := p.eps[u]
	return ep != nil && ep.verified && !ep.wrongChain && p.now().Sub(ep.indexerAt) >= CapabilityTTL && !p.now().Before(ep.quotaUntil)
}

func (p *Pool) SetIndexer(u string, ok bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ep := p.eps[u]; ep != nil {
		ep.indexerOK, ep.indexerAt = ok, p.now()
	}
}

// BlockCapability is independent of basic liveness: a successful height probe
// cannot restore a method restricted by the provider or clear a quota cooldown.
func (p *Pool) BlockCapability(u, capability string, quota bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if ep := p.eps[u]; ep != nil {
		if quota {
			ep.quotaUntil = p.now().Add(30 * time.Second)
		} else {
			if ep.blocked == nil {
				ep.blocked = map[string]time.Time{}
			}
			ep.blocked[capability] = p.now().Add(CapabilityTTL)
		}
	}
}

func (p *Pool) eligibleLocked(ep *endpoint, capability string, indexer bool) bool {
	now := p.now()
	if indexer {
		for _, shape := range []string{"eth_getBlockByNumber:full", "eth_getLogs:hash", "eth_getLogs:range", "eth_getBlockByHash"} {
			if now.Before(ep.blocked[shape]) {
				return false
			}
		}
	}
	return ep.verified && !ep.wrongChain && ep.alive && !now.Before(ep.cooldownUntil) && !now.Before(ep.quotaUntil) && !now.Before(ep.blocked[capability]) && (!indexer || ep.indexerOK && now.Sub(ep.indexerAt) < CapabilityTTL)
}

// Candidates returns eligible URLs in priority/latency order. The proxy can
// preserve its leader or validate a backup before changing the head source.
func (p *Pool) Candidates(exclude map[string]bool, capability string, indexer, archive bool) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := p.now()
	ref := p.heightRefLocked(now)
	var eps []*endpoint
	for _, ep := range p.list {
		if exclude[ep.url] || !p.eligibleLocked(ep, capability, indexer) || p.laggingLocked(ep, ref) {
			continue
		}
		if archive && (ep.archiveState != archiveYes || now.Sub(ep.archiveAt) >= archiveRecheck) {
			continue
		}
		eps = append(eps, ep)
	}
	// Small pools: insertion sorting keeps endpoint order deterministic on ties.
	for i := 1; i < len(eps); i++ {
		for j := i; j > 0; j-- {
			a, b := eps[j], eps[j-1]
			if !(a.priority && !b.priority || a.priority == b.priority && effEMA(a) < effEMA(b)) {
				break
			}
			eps[j], eps[j-1] = eps[j-1], eps[j]
		}
	}
	out := make([]string, len(eps))
	for i, ep := range eps {
		out[i] = ep.url
	}
	return out
}

func (p *Pool) IndexerStatus(u string) (bool, time.Time, time.Time) {
	p.mu.Lock()
	defer p.mu.Unlock()
	ep := p.eps[u]
	if ep == nil {
		return false, time.Time{}, time.Time{}
	}
	return p.eligibleLocked(ep, "", true), ep.indexerAt, ep.quotaUntil
}

// Choose applies P2C within the highest available priority tier.
func (p *Pool) Choose(urls []string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	var candidates []*endpoint
	preferred := false
	for _, u := range urls {
		if ep := p.eps[u]; ep != nil && ep.priority {
			preferred = true
			break
		}
	}
	for _, u := range urls {
		if ep := p.eps[u]; ep != nil && (!preferred || ep.priority) {
			candidates = append(candidates, ep)
		}
	}
	if len(candidates) == 0 {
		return ""
	}
	if len(candidates) == 1 {
		return candidates[0].url
	}
	a := p.rand(len(candidates))
	b := p.rand(len(candidates) - 1)
	if b >= a {
		b++
	}
	if effEMA(candidates[b]) < effEMA(candidates[a]) {
		return candidates[b].url
	}
	return candidates[a].url
}
