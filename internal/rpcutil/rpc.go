// Package rpcutil validates JSON-RPC envelopes and classifies provider failures.
package rpcutil

import (
	"encoding/json"
	"fmt"
	"strings"
)

type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   json.RawMessage `json:"error,omitempty"`
}

func ReadOnly(method string) bool {
	switch method {
	case "eth_blockNumber", "eth_chainId", "eth_getBlockByNumber", "eth_getBlockByHash", "eth_getLogs", "eth_getTransactionReceipt", "eth_getTransactionByHash", "eth_getBlockReceipts", "eth_getBalance", "eth_getCode", "eth_getStorageAt", "eth_call", "eth_getTransactionCount", "eth_feeHistory", "eth_gasPrice", "eth_maxPriorityFeePerGas", "eth_estimateGas", "net_version", "web3_clientVersion", "getSlot", "getBlock", "getBlockHeight", "getBalance", "getTransaction", "getSignatureStatuses", "getSignaturesForAddress", "getLatestBlockhash", "getGenesisHash", "getAccountInfo", "getProgramAccounts", "getMultipleAccounts", "getHealth":
		return true
	}
	return false
}

func (r Request) Capability() string {
	var params []json.RawMessage
	_ = json.Unmarshal(r.Params, &params)
	if (r.Method == "eth_getBlockByNumber" || r.Method == "eth_getBlockByHash") && len(params) > 1 && string(params[1]) == "true" {
		return r.Method + ":full"
	}
	if r.Method == "eth_getLogs" && len(params) > 0 {
		var filter map[string]json.RawMessage
		_ = json.Unmarshal(params[0], &filter)
		if filter["blockHash"] != nil {
			return "eth_getLogs:hash"
		}
		return "eth_getLogs:range"
	}
	return r.Method
}

// ProviderError deliberately uses known error messages, not all -32000 errors:
// execution reverts, invalid parameters and unknown client methods must survive.
func ProviderError(raw json.RawMessage, method string) (retry, quota bool) {
	if len(raw) == 0 || string(raw) == "null" {
		return false, false
	}
	var e struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}
	if json.Unmarshal(raw, &e) != nil {
		return true, false
	}
	m := strings.ToLower(e.Message)
	for _, s := range []string{"execution reverted", "invalid argument", "invalid param", "insufficient funds", "nonce too", "out of gas"} {
		if strings.Contains(m, s) {
			return false, false
		}
	}
	for _, s := range []string{"rate limit", "limit exceeded", "too many requests", "quota", "capacity", "compute units", "requests per", "request limit"} {
		if strings.Contains(m, s) {
			return true, true
		}
	}
	if e.Code == 429 {
		return true, true
	}
	for _, s := range []string{"method not available", "method not supported", "unsupported method", "not available on this plan", "upgrade your", "not supported", "temporarily unavailable", "timeout", "timed out", "header not found", "block not found", "missing trie node", "historical state", "pruned", "try again"} {
		if strings.Contains(m, s) {
			return true, false
		}
	}
	if e.Code == -32601 && ReadOnly(method) {
		return true, false
	}
	return false, false
}

func ParseResponse(raw []byte) (Response, error) {
	var r Response
	if err := json.Unmarshal(raw, &r); err != nil {
		return r, err
	}
	if r.JSONRPC != "2.0" || len(r.ID) == 0 || (len(r.Result) == 0) == (len(r.Error) == 0 || string(r.Error) == "null") {
		return r, fmt.Errorf("invalid JSON-RPC response envelope")
	}
	return r, nil
}

func ErrorResponse(id json.RawMessage, message string) json.RawMessage {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	b, _ := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": id, "error": map[string]any{"code": -32000, "message": message}})
	return b
}
