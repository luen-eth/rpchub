// Package config loads rpchub configuration from environment variables.
package config

import (
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"time"
)

// DefaultChainlistURL is the canonical source of EVM chain RPC lists.
const DefaultChainlistURL = "https://chainlist.org/rpcs.json"

// SolanaKey is the canonical chain key for the Solana exception path.
const SolanaKey = "solana"

// reservedTokens can never be used as chain path tokens or aliases.
var reservedTokens = map[string]bool{"health": true, "chains": true, "metrics": true, "archive": true, "ws": true, "indexer": true}

type Config struct {
	Port            int
	ChainIDs        []int64
	SolanaEnabled   bool
	SolanaRPCs      []string
	ExtraRPCs       map[string][]string // chain key ("1", "solana") -> user RPCs, highest priority
	Aliases         map[string]string   // extra path alias -> chain key
	RefreshInterval time.Duration
	ProbeInterval   time.Duration
	RequestTimeout  time.Duration
	MaxRetries      int
	MaxBlockLag     uint64
	FilterTracking  bool
	AllowHTTP       bool
	WSEnabled       bool
	MaxWSConns      int
	CacheDir        string
	ChainlistURL    string
	LogLevel        slog.Level
}

// Load parses configuration from a list of "KEY=VALUE" strings (os.Environ format).
func Load(environ []string) (*Config, error) {
	env := make(map[string]string, len(environ))
	for _, kv := range environ {
		if k, v, ok := strings.Cut(kv, "="); ok {
			env[k] = v
		}
	}

	cfg := &Config{
		Port:            9563,
		ExtraRPCs:       map[string][]string{},
		Aliases:         map[string]string{},
		RefreshInterval: time.Hour,
		ProbeInterval:   30 * time.Second,
		RequestTimeout:  15 * time.Second,
		MaxRetries:      3,
		MaxBlockLag:     10,
		WSEnabled:       true,
		MaxWSConns:      256,
		CacheDir:        "./data",
		ChainlistURL:    DefaultChainlistURL,
		LogLevel:        slog.LevelInfo,
	}

	var err error
	if cfg.Port, err = intVal(env, "PORT", cfg.Port); err != nil {
		return nil, err
	}
	if cfg.Port < 1 || cfg.Port > 65535 {
		return nil, fmt.Errorf("PORT: %d out of range", cfg.Port)
	}

	for _, item := range splitList(env["CHAIN_IDS"]) {
		id, err := strconv.ParseInt(item, 10, 64)
		if err != nil || id <= 0 {
			return nil, fmt.Errorf("CHAIN_IDS: %q is not a positive chain id", item)
		}
		if !containsInt(cfg.ChainIDs, id) {
			cfg.ChainIDs = append(cfg.ChainIDs, id)
		}
	}

	solanaFlag, err := boolVal(env, "SOLANA_ENABLED", false)
	if err != nil {
		return nil, err
	}
	cfg.SolanaRPCs = splitList(env["SOLANA_RPCS"])
	cfg.SolanaEnabled = solanaFlag || len(cfg.SolanaRPCs) > 0

	if err := loadExtraRPCs(env, cfg); err != nil {
		return nil, err
	}
	if err := loadAliases(env, cfg); err != nil {
		return nil, err
	}

	if cfg.RefreshInterval, err = durVal(env, "REFRESH_INTERVAL", cfg.RefreshInterval); err != nil {
		return nil, err
	}
	if cfg.ProbeInterval, err = durVal(env, "PROBE_INTERVAL", cfg.ProbeInterval); err != nil {
		return nil, err
	}
	if cfg.RequestTimeout, err = durVal(env, "REQUEST_TIMEOUT", cfg.RequestTimeout); err != nil {
		return nil, err
	}
	if cfg.MaxRetries, err = intVal(env, "MAX_RETRIES", cfg.MaxRetries); err != nil {
		return nil, err
	}
	if cfg.MaxRetries < 1 {
		return nil, fmt.Errorf("MAX_RETRIES: must be >= 1")
	}
	lag, err := intVal(env, "MAX_BLOCK_LAG", int(cfg.MaxBlockLag))
	if err != nil {
		return nil, err
	}
	if lag < 0 {
		return nil, fmt.Errorf("MAX_BLOCK_LAG: must be >= 0")
	}
	cfg.MaxBlockLag = uint64(lag)
	if cfg.FilterTracking, err = boolVal(env, "FILTER_TRACKING", false); err != nil {
		return nil, err
	}
	if cfg.AllowHTTP, err = boolVal(env, "ALLOW_HTTP", false); err != nil {
		return nil, err
	}
	if cfg.WSEnabled, err = boolVal(env, "WS_ENABLED", cfg.WSEnabled); err != nil {
		return nil, err
	}
	if cfg.MaxWSConns, err = intVal(env, "MAX_WS_CONNS", cfg.MaxWSConns); err != nil {
		return nil, err
	}
	if cfg.MaxWSConns < 1 {
		return nil, fmt.Errorf("MAX_WS_CONNS: must be >= 1")
	}
	if v := env["CACHE_DIR"]; v != "" {
		cfg.CacheDir = v
	}
	if v := env["CHAINLIST_URL"]; v != "" {
		cfg.ChainlistURL = v
	}
	if v, ok := env["LOG_LEVEL"]; ok && v != "" {
		switch strings.ToLower(v) {
		case "debug":
			cfg.LogLevel = slog.LevelDebug
		case "info":
			cfg.LogLevel = slog.LevelInfo
		case "warn", "warning":
			cfg.LogLevel = slog.LevelWarn
		case "error":
			cfg.LogLevel = slog.LevelError
		default:
			return nil, fmt.Errorf("LOG_LEVEL: unknown level %q", v)
		}
	}

	if len(cfg.ChainIDs) == 0 && !cfg.SolanaEnabled {
		return nil, fmt.Errorf(`no chains configured: set CHAIN_IDS (e.g. "1,56") and/or SOLANA_ENABLED=true`)
	}
	return cfg, nil
}

// EnabledKeys returns the canonical chain keys enabled by this config.
func (c *Config) EnabledKeys() map[string]bool {
	keys := make(map[string]bool, len(c.ChainIDs)+1)
	for _, id := range c.ChainIDs {
		keys[strconv.FormatInt(id, 10)] = true
	}
	if c.SolanaEnabled {
		keys[SolanaKey] = true
	}
	return keys
}

func loadExtraRPCs(env map[string]string, cfg *Config) error {
	names := make([]string, 0, len(env))
	for k := range env {
		if strings.HasPrefix(k, "EXTRA_RPCS_") {
			names = append(names, k)
		}
	}
	sort.Strings(names)
	for _, name := range names {
		suffix := strings.TrimPrefix(name, "EXTRA_RPCS_")
		var key string
		switch {
		case strings.EqualFold(suffix, "SOLANA"):
			key = SolanaKey
		case isDigits(suffix):
			key = strings.TrimLeft(suffix, "0")
			if key == "" {
				return fmt.Errorf("%s: invalid chain id", name)
			}
		default:
			return fmt.Errorf("%s: suffix must be a numeric chain id or SOLANA", name)
		}
		if urls := splitList(env[name]); len(urls) > 0 {
			cfg.ExtraRPCs[key] = append(cfg.ExtraRPCs[key], urls...)
		}
	}
	return nil
}

func loadAliases(env map[string]string, cfg *Config) error {
	enabled := cfg.EnabledKeys()
	for _, pair := range splitList(env["ALIASES"]) {
		alias, target, ok := strings.Cut(pair, ":")
		if !ok {
			return fmt.Errorf("ALIASES: %q must be alias:chainId (e.g. bsc:56)", pair)
		}
		alias = strings.ToLower(strings.TrimSpace(alias))
		target = strings.ToLower(strings.TrimSpace(target))
		if alias == "" || reservedTokens[alias] {
			return fmt.Errorf("ALIASES: %q is reserved or empty", alias)
		}
		if isDigits(alias) {
			return fmt.Errorf("ALIASES: alias %q cannot be numeric", alias)
		}
		if target != SolanaKey && !isDigits(target) {
			return fmt.Errorf("ALIASES: target %q must be a chain id or %q", target, SolanaKey)
		}
		if !enabled[target] {
			return fmt.Errorf("ALIASES: target %q is not an enabled chain", target)
		}
		cfg.Aliases[alias] = target
	}
	return nil
}

func splitList(v string) []string {
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func intVal(env map[string]string, key string, def int) (int, error) {
	v, ok := env[key]
	if !ok || v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not an integer", key, v)
	}
	return n, nil
}

func durVal(env map[string]string, key string, def time.Duration) (time.Duration, error) {
	v, ok := env[key]
	if !ok || v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(strings.TrimSpace(v))
	if err != nil {
		return 0, fmt.Errorf("%s: %q is not a duration (e.g. 30s, 1h)", key, v)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%s: must be positive", key)
	}
	return d, nil
}

func boolVal(env map[string]string, key string, def bool) (bool, error) {
	v, ok := env[key]
	if !ok || v == "" {
		return def, nil
	}
	b, err := strconv.ParseBool(strings.TrimSpace(v))
	if err != nil {
		return false, fmt.Errorf("%s: %q is not a boolean", key, v)
	}
	return b, nil
}

func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

func containsInt(xs []int64, x int64) bool {
	for _, v := range xs {
		if v == x {
			return true
		}
	}
	return false
}
