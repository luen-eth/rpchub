package registry

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// Kind distinguishes the JSON-RPC dialect spoken by a chain.
type Kind int

const (
	KindEVM Kind = iota
	KindSolana
)

func (k Kind) String() string {
	if k == KindSolana {
		return "solana"
	}
	return "evm"
}

// SolanaKey is the canonical path token for the Solana exception path.
const SolanaKey = "solana"

// DefaultSolanaRPCs are keyless public Solana mainnet endpoints. Chainlist is
// EVM-only, so Solana gets this built-in list; SOLANA_RPCS and
// EXTRA_RPCS_SOLANA entries are placed in front of it.
var DefaultSolanaRPCs = []string{
	"https://api.mainnet-beta.solana.com",
	"https://solana-rpc.publicnode.com",
}

// reservedTokens are path segments used by rpchub's own API.
var reservedTokens = map[string]bool{"health": true, "chains": true, "metrics": true, "archive": true, "ws": true, "indexer": true}

// DefaultSolanaWSRPCs mirrors DefaultSolanaRPCs for the subscription path.
var DefaultSolanaWSRPCs = []string{
	"wss://api.mainnet-beta.solana.com",
	"wss://solana-rpc.publicnode.com",
}

// Chain is one proxied chain with its sanitized endpoint lists.
type Chain struct {
	Key       string // canonical path token: decimal chain id, or "solana"
	ChainID   int64  // 0 for Solana
	Kind      Kind
	Name      string
	Slug      string
	ShortName string
	Endpoints []string // http(s), sanitized and deduped, user extras first
	// WSEndpoints holds ws(s) endpoints for the subscription path. It may be
	// empty: plenty of chains publish no public WebSocket at all.
	WSEndpoints []string
}

// BuildOptions selects and shapes the chains exposed by the registry.
type BuildOptions struct {
	ChainIDs       []int64
	AllowHTTP      bool                // also admit plaintext http:// and ws://
	FilterTracking bool                // keep only tracking == "none" chainlist entries
	ExtraRPCs      map[string][]string // chain key -> user RPCs, highest priority
	Aliases        map[string]string   // extra token -> chain key
	SolanaEnabled  bool
	SolanaRPCs     []string
	WSEnabled      bool // collect ws(s) endpoints for the subscription path
}

// Registry resolves path tokens to chains. Update swaps the whole state
// atomically, so resolved *Chain values are immutable snapshots.
type Registry struct {
	mu     sync.RWMutex
	chains map[string]*Chain
	index  map[string]string // lowercase token -> canonical key
}

func New() *Registry {
	return &Registry{chains: map[string]*Chain{}, index: map[string]string{}}
}

// Update rebuilds the registry from a chainlist snapshot. It is
// all-or-nothing: on error the previous state is kept.
func (r *Registry) Update(entries []ChainEntry, opt BuildOptions) error {
	byID := make(map[int64]*ChainEntry, len(entries))
	for i := range entries {
		e := &entries[i]
		if _, dup := byID[e.ChainID]; !dup {
			byID[e.ChainID] = e
		}
	}

	chains := map[string]*Chain{}
	var missing []string
	for _, id := range opt.ChainIDs {
		key := strconv.FormatInt(id, 10)
		entry, ok := byID[id]
		if !ok {
			missing = append(missing, key)
			continue
		}
		ch := &Chain{
			Key:       key,
			ChainID:   id,
			Kind:      KindEVM,
			Name:      entry.Name,
			Slug:      strings.ToLower(entry.ChainSlug),
			ShortName: strings.ToLower(entry.ShortName),
		}
		seen := map[string]bool{}
		for _, raw := range opt.ExtraRPCs[key] {
			addEndpoint(ch, seen, raw, true, opt.WSEnabled) // user's own nodes: plaintext allowed
		}
		for _, rpc := range entry.RPC {
			if opt.FilterTracking && rpc.Tracking != "none" {
				continue
			}
			addEndpoint(ch, seen, rpc.URL, opt.AllowHTTP, opt.WSEnabled)
		}
		if len(ch.Endpoints) == 0 {
			return fmt.Errorf("chain %s (%s): no usable RPC endpoints after filtering", key, entry.Name)
		}
		chains[key] = ch
	}
	if len(missing) > 0 {
		return fmt.Errorf("chain ids not found in chainlist: %s", strings.Join(missing, ", "))
	}

	if opt.SolanaEnabled {
		ch := &Chain{Key: SolanaKey, Kind: KindSolana, Name: "Solana Mainnet", Slug: SolanaKey, ShortName: "sol"}
		seen := map[string]bool{}
		groups := [][]string{opt.ExtraRPCs[SolanaKey], opt.SolanaRPCs, DefaultSolanaRPCs}
		if opt.WSEnabled {
			groups = append(groups, DefaultSolanaWSRPCs)
		}
		for _, group := range groups {
			for _, raw := range group {
				addEndpoint(ch, seen, raw, true, opt.WSEnabled)
			}
		}
		if len(ch.Endpoints) == 0 {
			return fmt.Errorf("solana: no usable RPC endpoints")
		}
		chains[SolanaKey] = ch
	}

	index := map[string]string{}
	keys := sortedKeys(chains)
	for _, k := range keys { // canonical keys always win
		index[k] = k
	}
	for _, k := range keys {
		ch := chains[k]
		for _, tok := range []string{ch.Slug, ch.ShortName} {
			if tok == "" || reservedTokens[tok] {
				continue
			}
			if _, taken := index[tok]; !taken {
				index[tok] = k
			}
		}
	}
	for alias, target := range opt.Aliases {
		if _, ok := chains[target]; !ok {
			return fmt.Errorf("alias %q: target %q is not an enabled chain", alias, target)
		}
		index[strings.ToLower(alias)] = target
	}

	r.mu.Lock()
	r.chains = chains
	r.index = index
	r.mu.Unlock()
	return nil
}

// Resolve maps a path token ("1", "ethereum", "eth", "bsc", "solana") to its chain.
func (r *Registry) Resolve(token string) (*Chain, bool) {
	token = strings.ToLower(strings.TrimSpace(token))
	r.mu.RLock()
	defer r.mu.RUnlock()
	key, ok := r.index[token]
	if !ok {
		return nil, false
	}
	ch, ok := r.chains[key]
	return ch, ok
}

// Chains returns all enabled chains, EVM chains by ascending id, Solana last.
func (r *Registry) Chains() []*Chain {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*Chain, 0, len(r.chains))
	for _, ch := range r.chains {
		out = append(out, ch)
	}
	sort.Slice(out, func(i, j int) bool {
		if (out[i].Kind == KindSolana) != (out[j].Kind == KindSolana) {
			return out[j].Kind == KindSolana
		}
		return out[i].ChainID < out[j].ChainID
	})
	return out
}

// Tokens lists the non-canonical tokens (slug, short name, aliases) that
// resolve to the given chain key.
func (r *Registry) Tokens(key string) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	var out []string
	for tok, k := range r.index {
		if k == key && tok != key {
			out = append(out, tok)
		}
	}
	sort.Strings(out)
	return out
}

// addEndpoint sanitizes raw and files it under the HTTP or the WebSocket list.
// allowPlaintext is true for user-supplied nodes (their own LAN address is
// their business) and follows ALLOW_HTTP for chainlist entries.
func addEndpoint(ch *Chain, seen map[string]bool, raw string, allowPlaintext, wsEnabled bool) {
	u, transport := ClassifyURL(raw, allowPlaintext)
	if transport == TransportNone || seen[u] {
		return
	}
	if transport == TransportWS && !wsEnabled {
		return
	}
	seen[u] = true
	if transport == TransportWS {
		ch.WSEndpoints = append(ch.WSEndpoints, u)
		return
	}
	ch.Endpoints = append(ch.Endpoints, u)
}

func sortedKeys(chains map[string]*Chain) []string {
	out := make([]string, 0, len(chains))
	for k := range chains {
		out = append(out, k)
	}
	sort.Slice(out, func(i, j int) bool {
		a, aerr := strconv.ParseInt(out[i], 10, 64)
		b, berr := strconv.ParseInt(out[j], 10, 64)
		switch {
		case aerr == nil && berr == nil:
			return a < b
		case aerr == nil:
			return true
		case berr == nil:
			return false
		default:
			return out[i] < out[j]
		}
	})
	return out
}
