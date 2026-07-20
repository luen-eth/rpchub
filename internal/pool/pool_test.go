package pool

import (
	"testing"
	"time"
)

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newTestPool(lag uint64, urls ...string) (*Pool, *fakeClock) {
	clk := &fakeClock{t: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	p := New("test", lag)
	p.now = clk.now
	p.rand = func(int) int { return 0 } // deterministic P2C: compares cands[0] vs cands[1]
	p.SetEndpoints(urls)
	return p, clk
}

func TestColdStartFallback(t *testing.T) {
	p, _ := newTestPool(10, "https://a", "https://b")
	if _, ok := p.Pick(nil); !ok {
		t.Fatal("cold pool must still serve unproven endpoints")
	}
}

func TestBreakerTripAndRecover(t *testing.T) {
	p, clk := newTestPool(10, "https://a")
	p.ReportSuccess("https://a", 50*time.Millisecond, 100)

	for i := 0; i < failThreshold; i++ {
		p.ReportFailure("https://a", "boom")
	}
	if _, ok := p.Pick(nil); ok {
		t.Fatal("tripped endpoint must not be picked")
	}
	if s := p.Snapshot(true); s.Endpoints[0].Status != "cooldown" {
		t.Fatalf("status = %s, want cooldown", s.Endpoints[0].Status)
	}

	// Cooldown expires, but the endpoint was proven before (height>0): only a
	// probe success brings it back — no traffic fallback to known-bad.
	clk.advance(2 * baseCooldown)
	if _, ok := p.Pick(nil); ok {
		t.Fatal("expired cooldown alone must not restore traffic")
	}
	p.ReportSuccess("https://a", 50*time.Millisecond, 101)
	if url, ok := p.Pick(nil); !ok || url != "https://a" {
		t.Fatalf("Pick after recovery = %q, %v", url, ok)
	}
}

func TestCooldownGrowsExponentially(t *testing.T) {
	p, clk := newTestPool(10, "https://a")
	for i := 0; i < failThreshold; i++ {
		p.ReportFailure("https://a", "x")
	}
	first := p.eps["https://a"].cooldownUntil.Sub(clk.t)
	if first != baseCooldown {
		t.Fatalf("first cooldown = %v, want %v", first, baseCooldown)
	}
	for i := 0; i < 10; i++ {
		p.ReportFailure("https://a", "x")
	}
	last := p.eps["https://a"].cooldownUntil.Sub(clk.t)
	if last != maxCooldown {
		t.Fatalf("cooldown = %v, want capped at %v", last, maxCooldown)
	}
}

func TestP2CPrefersLowerLatency(t *testing.T) {
	p, _ := newTestPool(10, "https://slow", "https://fast")
	p.ReportSuccess("https://slow", 500*time.Millisecond, 100)
	p.ReportSuccess("https://fast", 10*time.Millisecond, 100)
	for i := 0; i < 10; i++ {
		if url, _ := p.Pick(nil); url != "https://fast" {
			t.Fatalf("Pick = %q, want https://fast", url)
		}
	}
}

func TestLaggingExcludedViaMedian(t *testing.T) {
	p, _ := newTestPool(10, "https://a", "https://b", "https://stale", "https://liar")
	p.ReportSuccess("https://a", 10*time.Millisecond, 1000)
	p.ReportSuccess("https://b", 10*time.Millisecond, 1001)
	p.ReportSuccess("https://stale", 10*time.Millisecond, 900) // 100 behind
	// One endpoint reporting a bogus huge height must not poison the median.
	p.ReportSuccess("https://liar", 10*time.Millisecond, 999999999)

	for i := 0; i < 30; i++ {
		url, ok := p.Pick(nil)
		if !ok || url == "https://stale" {
			t.Fatalf("Pick = %q, %v; stale endpoint must be excluded", url, ok)
		}
	}
	if s := p.Snapshot(true); s.Endpoints[2].Status != "lagging" {
		t.Fatalf("stale status = %s, want lagging", s.Endpoints[2].Status)
	}

	// Lagging endpoints are not even a fallback: with everything else
	// excluded, Pick must fail rather than serve stale data.
	exclude := map[string]bool{"https://a": true, "https://b": true, "https://liar": true}
	if url, ok := p.Pick(exclude); ok {
		t.Fatalf("Pick = %q; lagging endpoint served as fallback", url)
	}
}

func TestWrongChainPermanentlyExcluded(t *testing.T) {
	p, _ := newTestPool(10, "https://a", "https://b")
	p.MarkWrongChain("https://a", "chainId 56 != 1")
	for i := 0; i < 10; i++ {
		if url, _ := p.Pick(nil); url != "https://b" {
			t.Fatalf("Pick = %q, want https://b", url)
		}
	}
	p.ReportSuccess("https://a", time.Millisecond, 5) // success must not resurrect it
	if s := p.Snapshot(true); s.Endpoints[0].Status != "wrong_chain" {
		t.Fatalf("status = %s, want wrong_chain", s.Endpoints[0].Status)
	}
	if p.NeedsVerify("https://a") {
		t.Fatal("wrong-chain endpoint must not be re-verified")
	}
}

func TestSetEndpointsPreservesState(t *testing.T) {
	p, _ := newTestPool(10, "https://a")
	p.ReportSuccess("https://a", 40*time.Millisecond, 123)
	p.SetVerified("https://a")
	p.SetEndpoints([]string{"https://a", "https://new"})

	s := p.Snapshot(true)
	if s.Total != 2 || s.Healthy != 1 {
		t.Fatalf("snapshot = %+v", s)
	}
	if s.Endpoints[0].Status != "healthy" || s.Endpoints[0].Height != 123 {
		t.Fatalf("state lost: %+v", s.Endpoints[0])
	}
	if s.Endpoints[1].Status != "unproven" {
		t.Fatalf("new endpoint = %+v", s.Endpoints[1])
	}
	if !p.NeedsVerify("https://new") || p.NeedsVerify("https://a") {
		t.Fatal("verify flags wrong after SetEndpoints")
	}
}

func TestReportOnRemovedEndpointIsNoop(t *testing.T) {
	p, _ := newTestPool(10, "https://a")
	p.ReportSuccess("https://gone", time.Millisecond, 1)
	p.ReportFailure("https://gone", "x")
	if s := p.Snapshot(false); s.Total != 1 {
		t.Fatalf("snapshot = %+v", s)
	}
}

func TestSetEnsure(t *testing.T) {
	s := NewSet()
	a := s.Ensure("1", 10)
	if b := s.Ensure("1", 99); a != b {
		t.Fatal("Ensure must return the same pool")
	}
	if _, ok := s.Get("2"); ok {
		t.Fatal("Get(2) should miss")
	}
}
