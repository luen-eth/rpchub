package health

import (
	"context"
	"encoding/json"
	"fmt"
	"rpchub/internal/rpcutil"
)

// Check actual Ponder request shapes; height alone cannot establish support.
func (p *Prober) checkIndexer(ctx context.Context, u string, height uint64) {
	call := func(method string, params any, out any) bool {
		payload, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": 1, "method": method, "params": params})
		body, _, err := p.roundTrip(ctx, u, payload)
		if err != nil {
			return false
		}
		res, err := rpcutil.ParseResponse(body)
		if err != nil || len(res.Error) > 0 && string(res.Error) != "null" {
			return false
		}
		return string(res.Result) != "null" && json.Unmarshal(res.Result, out) == nil
	}
	// Keep probe responses small, but require full transactions to be accepted.
	var block struct {
		Hash         string            `json:"hash"`
		Number       string            `json:"number"`
		Transactions []json.RawMessage `json:"transactions"`
	}
	ok := call("eth_getBlockByNumber", []any{fmt.Sprintf("0x%x", height), true}, &block) && len(block.Hash) == 66 && block.Number == fmt.Sprintf("0x%x", height) && block.Transactions != nil
	for _, tx := range block.Transactions {
		if len(tx) == 0 || tx[0] != '{' {
			ok = false
		}
	}
	if ok {
		var logs []json.RawMessage
		ok = call("eth_getLogs", []any{map[string]any{"blockHash": block.Hash}}, &logs)
		if ok {
			ok = call("eth_getLogs", []any{map[string]any{"fromBlock": block.Number, "toBlock": block.Number, "address": "0x0000000000000000000000000000000000000000"}}, &logs)
		}
	}
	if ok {
		var byHash struct {
			Hash string `json:"hash"`
		}
		ok = call("eth_getBlockByHash", []any{block.Hash, false}, &byHash) && byHash.Hash == block.Hash
	}
	p.pool.SetIndexer(u, ok)
	p.log.Debug("indexer capability", "chain", p.pool.Key(), "supported", ok)
}
