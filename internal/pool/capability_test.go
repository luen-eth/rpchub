package pool

import (
	"fmt"
	"testing"
	"time"
)

func TestIndexerRefreshDoesNotDrainHealthyPool(t *testing.T) {
	var urls []string
	for i := 0; i < 48; i++ {
		urls = append(urls, fmt.Sprintf("https://rpc-%d.example", i))
	}
	p, clk := newTestPool(10, urls...)
	for _, u := range urls {
		p.SetIndexer(u, true)
	}
	// Simulate sweeps every 30 seconds and even slow checks taking two minutes.
	// The old expiry-only scheduler drops every provider at minute ten.
	pending := map[string]time.Time{}
	started := map[time.Time]bool{}
	for elapsed := time.Duration(0); elapsed < 40*time.Minute; elapsed += 30 * time.Second {
		for u, done := range pending {
			if !clk.t.Before(done) {
				p.SetIndexer(u, true)
				delete(pending, u)
			}
		}
		if got := len(p.Candidates(nil, "", true, false)); got != len(urls) {
			t.Fatalf("refresh drained healthy providers at %v: %d/%d remain", elapsed, got, len(urls))
		}
		for _, u := range urls {
			if _, running := pending[u]; !running && p.NeedsIndexerCheck(u) {
				pending[u] = clk.t.Add(2 * time.Minute)
				if elapsed < CapabilityTTL {
					started[clk.t] = true
				}
			}
		}
		clk.advance(30 * time.Second)
	}
	if len(started) < 3 {
		t.Fatal("initial refreshes were not spread across multiple sweeps")
	}
}

func TestIndexerRefreshKeepsExpiryAndFailureExclusion(t *testing.T) {
	p, clk := newTestPool(10, "a")
	p.SetIndexer("a", true)
	clk.advance(6 * time.Minute)
	if !p.NeedsIndexerCheck("a") || len(p.Candidates(nil, "", true, false)) != 1 {
		t.Fatal("renewal must start while the provider is still eligible")
	}
	p.SetIndexer("a", false)
	if len(p.Candidates(nil, "", true, false)) != 0 || p.NeedsIndexerCheck("a") {
		t.Fatal("failed check must exclude the provider and back off")
	}
	p.ReportSuccess("a", time.Millisecond, 1)
	if len(p.Candidates(nil, "", true, false)) != 0 {
		t.Fatal("basic health must not revive failed indexer capability")
	}
	clk.advance(time.Minute)
	if !p.NeedsIndexerCheck("a") {
		t.Fatal("failed capability should get a recovery check within a minute")
	}
	p.BlockCapability("a", "", true)
	if p.NeedsIndexerCheck("a") {
		t.Fatal("quota cooldown must also block capability probes")
	}
	clk.advance(30 * time.Second)
	p.SetIndexer("a", true)
	clk.advance(CapabilityTTL)
	if len(p.Candidates(nil, "", true, false)) != 0 {
		t.Fatal("missing or hung renewal must not extend the ten-minute trust bound")
	}
}

func TestCapabilityAndQuotaSurviveBasicProbe(t *testing.T) {
	p, clk := newTestPool(10, "a")
	p.SetIndexer("a", true)
	p.BlockCapability("a", "eth_getBlockByNumber:full", false)
	p.ReportSuccess("a", time.Millisecond, 100)
	if len(p.Candidates(nil, "eth_getBlockByNumber:full", true, false)) != 0 {
		t.Fatal("probe resurrected blocked capability")
	}
	if len(p.Candidates(nil, "eth_blockNumber", false, false)) != 1 {
		t.Fatal("unrelated read should remain usable")
	}
	p.BlockCapability("a", "", true)
	p.ReportSuccess("a", time.Millisecond, 100)
	if len(p.Candidates(nil, "eth_blockNumber", false, false)) != 0 {
		t.Fatal("probe cleared quota")
	}
	clk.advance(31 * time.Second)
	if len(p.Candidates(nil, "eth_blockNumber", false, false)) != 1 {
		t.Fatal("quota cooldown should expire")
	}
	clk.advance(CapabilityTTL)
	if len(p.Candidates(nil, "eth_blockNumber", true, false)) != 0 {
		t.Fatal("stale capability verdict served")
	}
}
func TestArchiveExpiryAndPrivatePriority(t *testing.T) {
	p, clk := newTestPool(10, "private", "public")
	p.SetPriority([]string{"private"})
	p.ReportSuccess("private", time.Second, 100)
	p.ReportSuccess("public", time.Millisecond, 100)
	if u, _ := p.Pick(nil, false); u != "private" {
		t.Fatal("private priority ignored")
	}
	p.SetArchive("private", true)
	clk.advance(archiveRecheck)
	if _, ok := p.Pick(nil, true); ok {
		t.Fatal("expired archive verdict served")
	}
	if p.Snapshot(false).ArchiveHealthy != 0 {
		t.Fatal("health trusts expired archive verdict")
	}
}
