package rpcutil

import (
	"encoding/json"
	"testing"
)

func TestProviderClassification(t *testing.T) {
	for _, tc := range []struct {
		method, err  string
		retry, quota bool
	}{
		{"eth_call", `{"code":-32000,"message":"execution reverted: quota exceeded"}`, false, false},
		{"eth_getLogs", `{"code":-32005,"message":"limit exceeded"}`, true, true},
		{"eth_getBlockByNumber", `{"code":-32601,"message":"method not found"}`, true, false},
		{"unknown_method", `{"code":-32601,"message":"method not found"}`, false, false},
		{"eth_call", `{"code":-32602,"message":"invalid params"}`, false, false},
		{"eth_blockNumber", `"method not supported"`, true, false},
	} {
		r, q := ProviderError(json.RawMessage(tc.err), tc.method)
		if r != tc.retry || q != tc.quota {
			t.Errorf("%s => %t/%t", tc.err, r, q)
		}
	}
}
func TestResponseEnvelope(t *testing.T) {
	for _, bad := range []string{`{broken`, `{"result":1}`, `{"jsonrpc":"2.0","id":1}`, `{"jsonrpc":"2.0","id":1,"result":1,"error":{"code":-1}}`} {
		if _, err := ParseResponse([]byte(bad)); err == nil {
			t.Errorf("accepted %s", bad)
		}
	}
	for _, good := range []string{`{"jsonrpc":"2.0","id":1,"result":null}`, `{"jsonrpc":"2.0","id":1,"result":[],"error":null}`, `{"jsonrpc":"2.0","id":1,"error":{"code":-1}}`} {
		if _, err := ParseResponse([]byte(good)); err != nil {
			t.Errorf("rejected %s: %v", good, err)
		}
	}
}
