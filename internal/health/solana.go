package health

// solanaMainnetGenesis is the genesis hash of Solana mainnet-beta; endpoints
// answering with a different hash (devnet, testnet, forks) are excluded.
const solanaMainnetGenesis = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d"

// Solana is the adapter for the Solana exception path (chainlist is
// EVM-only, so these endpoints come from a static list + env).
type Solana struct{}

func (Solana) ProbeRequest() []byte {
	return []byte(`{"jsonrpc":"2.0","id":1,"method":"getSlot","params":[{"commitment":"confirmed"}]}`)
}

func (Solana) ParseHeight(body []byte) (uint64, error) {
	var slot uint64
	if err := parseResult(body, &slot); err != nil {
		return 0, err
	}
	return slot, nil
}

func (Solana) IdentityRequest() []byte {
	return []byte(`{"jsonrpc":"2.0","id":1,"method":"getGenesisHash","params":[]}`)
}

func (Solana) VerifyIdentity(body []byte) (bool, error) {
	var hash string
	if err := parseResult(body, &hash); err != nil {
		return false, err
	}
	return hash == solanaMainnetGenesis, nil
}

// LagLimit widens the configured lag for Solana: slots advance ~30x faster
// than typical EVM blocks (default 10 blocks -> 200 slots ≈ 80s).
func (Solana) LagLimit(base uint64) uint64 { return base * 20 }
