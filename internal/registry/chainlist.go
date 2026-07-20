// Package registry loads chain and RPC endpoint definitions from
// chainlist.org (EVM) and a static list (Solana), sanitizes them, and
// resolves URL path tokens like "1", "ethereum" or "sol" to a chain.
package registry

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
)

// maxChainlistBytes caps the rpcs.json download (currently ~2 MB).
const maxChainlistBytes = 64 << 20

const cacheFileName = "rpcs.json"

// RPCEntry is one RPC endpoint in a chainlist chain record.
type RPCEntry struct {
	URL      string `json:"url"`
	Tracking string `json:"tracking"` // "none", "limited", "yes" or ""
}

// ChainEntry is one chain record from chainlist.org's rpcs.json.
type ChainEntry struct {
	Name      string     `json:"name"`
	ChainID   int64      `json:"chainId"`
	ChainSlug string     `json:"chainSlug"`
	ShortName string     `json:"shortName"`
	IsTestnet bool       `json:"isTestnet"`
	RPC       []RPCEntry `json:"rpc"`
}

// FetchChainlist downloads and decodes rpcs.json. It returns the decoded
// entries plus the raw bytes so callers can cache them.
func FetchChainlist(ctx context.Context, client *http.Client, url string) ([]ChainEntry, []byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, nil, err
	}
	req.Header.Set("User-Agent", "rpchub/1.0")
	resp, err := client.Do(req)
	if err != nil {
		return nil, nil, fmt.Errorf("fetch %s: %w", url, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("fetch %s: HTTP %d", url, resp.StatusCode)
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxChainlistBytes))
	if err != nil {
		return nil, nil, fmt.Errorf("read %s: %w", url, err)
	}
	entries, err := decodeChainlist(raw)
	if err != nil {
		return nil, nil, fmt.Errorf("decode %s: %w", url, err)
	}
	return entries, raw, nil
}

func decodeChainlist(raw []byte) ([]ChainEntry, error) {
	var entries []ChainEntry
	if err := json.Unmarshal(raw, &entries); err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("empty chain list")
	}
	return entries, nil
}

// SaveCache atomically writes raw rpcs.json bytes under dir.
func SaveCache(dir string, raw []byte) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, cacheFileName+".tmp*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), filepath.Join(dir, cacheFileName))
}

// LoadCache reads a previously saved rpcs.json from dir.
func LoadCache(dir string) ([]ChainEntry, error) {
	raw, err := os.ReadFile(filepath.Join(dir, cacheFileName))
	if err != nil {
		return nil, err
	}
	return decodeChainlist(raw)
}

// LoadSource fetches the chain list, falling back to the disk cache when the
// network fetch fails. A successful fetch refreshes the cache (best effort).
func LoadSource(ctx context.Context, client *http.Client, url, cacheDir string, log *slog.Logger) ([]ChainEntry, error) {
	entries, raw, err := FetchChainlist(ctx, client, url)
	if err == nil {
		if cerr := SaveCache(cacheDir, raw); cerr != nil {
			log.Warn("chainlist cache write failed", "dir", cacheDir, "err", cerr)
		}
		return entries, nil
	}
	log.Warn("chainlist fetch failed, trying disk cache", "err", err)
	entries, cerr := LoadCache(cacheDir)
	if cerr != nil {
		return nil, fmt.Errorf("chainlist fetch failed (%v) and no usable cache in %s (%v)", err, cacheDir, cerr)
	}
	log.Info("using cached chainlist", "dir", cacheDir, "chains", len(entries))
	return entries, nil
}
