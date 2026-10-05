package pool

import (
	"testing"
	"time"
)

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
