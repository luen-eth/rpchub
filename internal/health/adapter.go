// Package health probes RPC endpoints and feeds the results into their pool.
package health

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// Adapter abstracts the JSON-RPC dialect differences between chain kinds
// (EVM vs Solana): how to measure height and how to verify chain identity.
type Adapter interface {
	// ProbeRequest is the JSON-RPC body used to measure liveness, latency and height.
	ProbeRequest() []byte
	// ParseHeight extracts the chain height (block number / slot) from a probe response.
	ParseHeight(body []byte) (uint64, error)
	// IdentityRequest is the JSON-RPC body used to check which chain an endpoint serves.
	IdentityRequest() []byte
	// VerifyIdentity inspects the identity response. match=false with nil err
	// means the endpoint verifiably serves a DIFFERENT chain (permanent
	// exclusion); a non-nil err means the check itself failed (transient).
	VerifyIdentity(body []byte) (match bool, err error)
	// LagLimit scales the configured MAX_BLOCK_LAG to this chain's height unit.
	LagLimit(base uint64) uint64
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func parseResult(body []byte, out any) error {
	var r rpcResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return fmt.Errorf("non-JSON response")
	}
	if r.Error != nil {
		return fmt.Errorf("rpc error %d: %s", r.Error.Code, r.Error.Message)
	}
	if err := json.Unmarshal(r.Result, out); err != nil {
		return fmt.Errorf("unexpected result type")
	}
	return nil
}

// EVM is the adapter for all chainlist chains.
type EVM struct {
	ChainID int64
}

func (EVM) ProbeRequest() []byte {
	return []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)
}

func (EVM) ParseHeight(body []byte) (uint64, error) {
	var hexStr string
	if err := parseResult(body, &hexStr); err != nil {
		return 0, err
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(hexStr, "0x"), 16, 64)
	if err != nil {
		return 0, fmt.Errorf("bad block number %q", hexStr)
	}
	return n, nil
}

func (EVM) IdentityRequest() []byte {
	return []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`)
}

func (e EVM) VerifyIdentity(body []byte) (bool, error) {
	var hexStr string
	if err := parseResult(body, &hexStr); err != nil {
		return false, err
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(hexStr, "0x"), 16, 63)
	if err != nil {
		return false, fmt.Errorf("bad chainId %q", hexStr)
	}
	if int64(n) != e.ChainID {
		return false, nil // verifiably a different chain
	}
	return true, nil
}

func (EVM) LagLimit(base uint64) uint64 { return base }
