package config

import (
	"strings"
	"testing"
)

func TestProxyHardeningDefaultsPreserveLegacyBehavior(t *testing.T) {
	cfg, err := Load([]string{"CHAIN_IDS=56"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ReadOnly || cfg.MaxBatchItems != 0 || !cfg.WSEnabled {
		t.Fatalf("unexpected legacy defaults: readOnly=%v batch=%d ws=%v", cfg.ReadOnly, cfg.MaxBatchItems, cfg.WSEnabled)
	}
}

func TestReadOnlyProfileRequiresDisabledWebSockets(t *testing.T) {
	cfg, err := Load([]string{"CHAIN_IDS=56,8453", "READ_ONLY=true", "WS_ENABLED=false", "MAX_BATCH_ITEMS=10"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.ReadOnly || cfg.MaxBatchItems != 10 || cfg.WSEnabled {
		t.Fatal("read-only profile settings were not retained")
	}
	for _, websocket := range []string{"", "WS_ENABLED=true"} {
		_, err := Load([]string{"CHAIN_IDS=56", "READ_ONLY=true", websocket})
		if err == nil || !strings.Contains(err.Error(), "READ_ONLY requires WS_ENABLED=false") {
			t.Fatalf("read-only mode accepted an unfiltered WebSocket relay: %v", err)
		}
	}
}

func TestProxyHardeningRejectsMalformedSettings(t *testing.T) {
	for _, setting := range []string{"READ_ONLY=not-a-bool", "MAX_BATCH_ITEMS=-1", "MAX_BATCH_ITEMS=not-a-number"} {
		t.Run(setting, func(t *testing.T) {
			if _, err := Load([]string{"CHAIN_IDS=56", "WS_ENABLED=false", setting}); err == nil {
				t.Fatal("invalid hardening setting was accepted")
			}
		})
	}
}
