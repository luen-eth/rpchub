package config

import (
	"log/slog"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load([]string{"CHAIN_IDS=1,56,1"})
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Port != 9563 {
		t.Errorf("Port = %d, want 9563", cfg.Port)
	}
	if len(cfg.ChainIDs) != 2 || cfg.ChainIDs[0] != 1 || cfg.ChainIDs[1] != 56 {
		t.Errorf("ChainIDs = %v, want [1 56] (deduped)", cfg.ChainIDs)
	}
	if cfg.SolanaEnabled {
		t.Error("SolanaEnabled = true, want false")
	}
	if cfg.RefreshInterval != time.Hour || cfg.ProbeInterval != 30*time.Second {
		t.Errorf("intervals = %v/%v", cfg.RefreshInterval, cfg.ProbeInterval)
	}
	if cfg.MaxRetries != 3 || cfg.MaxBlockLag != 10 {
		t.Errorf("retries/lag = %d/%d", cfg.MaxRetries, cfg.MaxBlockLag)
	}
	if !cfg.WSEnabled || cfg.MaxWSConns != 256 {
		t.Errorf("ws defaults = %v/%d, want true/256", cfg.WSEnabled, cfg.MaxWSConns)
	}
	if cfg.ChainlistURL != DefaultChainlistURL {
		t.Errorf("ChainlistURL = %q", cfg.ChainlistURL)
	}
}

func TestLoadNoChains(t *testing.T) {
	if _, err := Load(nil); err == nil {
		t.Fatal("want error when nothing is configured")
	}
}

func TestSolanaRPCsImplyEnabled(t *testing.T) {
	cfg, err := Load([]string{"SOLANA_RPCS=https://a.example, https://b.example"})
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.SolanaEnabled {
		t.Error("SOLANA_RPCS should imply SolanaEnabled")
	}
	if len(cfg.SolanaRPCs) != 2 {
		t.Errorf("SolanaRPCs = %v", cfg.SolanaRPCs)
	}
}

func TestExtraRPCsAndAliases(t *testing.T) {
	cfg, err := Load([]string{
		"CHAIN_IDS=1,56",
		"SOLANA_ENABLED=true",
		"EXTRA_RPCS_1=https://my-eth-node.local, https://my-eth-2.local",
		"EXTRA_RPCS_SOLANA=https://my-sol.local",
		"ALIASES=bsc:56, sol:solana",
		"LOG_LEVEL=debug",
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.ExtraRPCs["1"]; len(got) != 2 || got[0] != "https://my-eth-node.local" {
		t.Errorf("ExtraRPCs[1] = %v", got)
	}
	if got := cfg.ExtraRPCs[SolanaKey]; len(got) != 1 {
		t.Errorf("ExtraRPCs[solana] = %v", got)
	}
	if cfg.Aliases["bsc"] != "56" || cfg.Aliases["sol"] != SolanaKey {
		t.Errorf("Aliases = %v", cfg.Aliases)
	}
	if cfg.LogLevel != slog.LevelDebug {
		t.Errorf("LogLevel = %v", cfg.LogLevel)
	}
}

func TestLoadErrors(t *testing.T) {
	cases := [][]string{
		{"CHAIN_IDS=abc"},
		{"CHAIN_IDS=0"},
		{"CHAIN_IDS=-5"},
		{"CHAIN_IDS=1", "REFRESH_INTERVAL=nope"},
		{"CHAIN_IDS=1", "MAX_RETRIES=0"},
		{"CHAIN_IDS=1", "LOG_LEVEL=loud"},
		{"CHAIN_IDS=1", "EXTRA_RPCS_FOO=https://x"},
		{"CHAIN_IDS=1", "ALIASES=bsc:56"},      // 56 not enabled
		{"CHAIN_IDS=56", "ALIASES=99:56"},      // numeric alias
		{"CHAIN_IDS=56", "ALIASES=health:56"},  // reserved
		{"CHAIN_IDS=56", "ALIASES=archive:56"}, // reserved
		{"CHAIN_IDS=56", "ALIASES=ws:56"},      // reserved
		{"CHAIN_IDS=1", "MAX_WS_CONNS=0"},
		{"CHAIN_IDS=1", "WS_ENABLED=maybe"},
		{"CHAIN_IDS=56", "ALIASES=bsc=56"}, // bad separator
		{"CHAIN_IDS=1", "PORT=99999"},
	}
	for _, environ := range cases {
		if _, err := Load(environ); err == nil {
			t.Errorf("Load(%v): want error, got nil", environ)
		}
	}
}

func TestEnabledKeys(t *testing.T) {
	cfg, err := Load([]string{"CHAIN_IDS=1,137", "SOLANA_ENABLED=1"})
	if err != nil {
		t.Fatal(err)
	}
	keys := cfg.EnabledKeys()
	for _, want := range []string{"1", "137", SolanaKey} {
		if !keys[want] {
			t.Errorf("EnabledKeys missing %q: %v", want, keys)
		}
	}
}
