package proxy

import (
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestStrictEnvelopesRejectAmbiguousMembersBeforeForwarding(t *testing.T) {
	requests := map[string]string{
		"lowercase write then uppercase read": `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","METHOD":"eth_chainId","params":["0x00"]}`,
		"uppercase read then lowercase write": `{"jsonrpc":"2.0","id":1,"METHOD":"eth_chainId","method":"eth_sendRawTransaction","params":["0x00"]}`,
		"lowercase read then uppercase write": `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","METHOD":"eth_sendRawTransaction","params":[]}`,
		"uppercase write then lowercase read": `{"jsonrpc":"2.0","id":1,"METHOD":"eth_sendRawTransaction","method":"eth_chainId","params":[]}`,
		"uppercase method only":               `{"jsonrpc":"2.0","id":1,"METHOD":"eth_chainId"}`,
		"mixed-case method":                   `{"jsonrpc":"2.0","id":1,"Method":"eth_chainId"}`,
		"uppercase jsonrpc":                   `{"JSONRPC":"2.0","id":1,"method":"eth_chainId"}`,
		"uppercase id":                        `{"jsonrpc":"2.0","ID":1,"method":"eth_chainId"}`,
		"uppercase params":                    `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","PARAMS":[]}`,
		"jsonrpc alias shadows version":       `{"jsonrpc":"1.0","JSONRPC":"2.0","id":1,"method":"eth_chainId"}`,
		"duplicate method write then read":    `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","method":"eth_chainId","params":["0x00"]}`,
		"duplicate method read then write":    `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","method":"eth_sendRawTransaction","params":["0x00"]}`,
		"duplicate id":                        `{"jsonrpc":"2.0","id":1,"id":2,"method":"eth_chainId"}`,
		"duplicate jsonrpc":                   `{"jsonrpc":"2.0","jsonrpc":"2.0","id":1,"method":"eth_chainId"}`,
		"duplicate params":                    `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[],"params":{}}`,
		"escaped duplicate method":            `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","\u006dethod":"eth_chainId","params":["0x00"]}`,
		"escaped duplicate method first":      `{"jsonrpc":"2.0","id":1,"\u006dethod":"eth_sendRawTransaction","method":"eth_chainId","params":["0x00"]}`,
		"escaped duplicate id":                `{"jsonrpc":"2.0","id":1,"i\u0064":2,"method":"eth_chainId"}`,
		"escaped duplicate jsonrpc":           `{"jsonrpc":"2.0","\u006asonrpc":"2.0","id":1,"method":"eth_chainId"}`,
		"escaped duplicate params":            `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[],"param\u0073":{}}`,
		"escaped uppercase alias":             `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","\u004dETHOD":"eth_chainId","params":["0x00"]}`,
		"unknown member":                      `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","extra":true}`,
		"nonobject null":                      `null`,
		"nonobject string":                    `"eth_chainId"`,
		"nonobject number":                    `42`,
		"nonobject batch member":              `[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},null]`,
		"trailing object":                     `{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}{"jsonrpc":"2.0","id":2,"method":"eth_chainId"}`,
		"method wrong type":                   `{"jsonrpc":"2.0","id":1,"method":[]}`,
		"jsonrpc wrong type":                  `{"jsonrpc":2.0,"id":1,"method":"eth_chainId"}`,
	}
	for _, path := range []string{"/1", "/1/archive", "/1/indexer"} {
		for name, body := range requests {
			t.Run(path+"/"+name, func(t *testing.T) {
				up, count := upstream(t, echoPolicyReads)
				result := policyRequest(policyHub(t, up.URL, true, 20), path, body)
				if result.Code != http.StatusBadRequest || !json.Valid(result.Body.Bytes()) {
					t.Fatalf("ambiguous envelope response = %d %s", result.Code, result.Body.String())
				}
				if count.hits.Load() != 0 {
					t.Fatalf("ambiguous envelope reached upstream %d times", count.hits.Load())
				}
			})
		}
	}
}

func TestStrictEnvelopeRejectsEntireBatchBeforeAnyReadOrWrite(t *testing.T) {
	body := `[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},{"jsonrpc":"2.0","id":2,"method":"eth_sendRawTransaction","METHOD":"eth_chainId","params":["0x00"]}]`
	for _, path := range []string{"/1", "/1/archive", "/1/indexer"} {
		for _, readOnly := range []bool{false, true} {
			up, count := upstream(t, echoPolicyReads)
			result := policyRequest(policyHub(t, up.URL, readOnly, 20), path, body)
			if result.Code != http.StatusBadRequest || count.hits.Load() != 0 {
				t.Fatalf("invalid batch forwarded on %s readOnly=%v: status=%d hits=%d", path, readOnly, result.Code, count.hits.Load())
			}
		}
	}
}

func TestStrictEnvelopePreservesValidReadsAndParameterPayload(t *testing.T) {
	for _, path := range []string{"/1", "/1/archive", "/1/indexer"} {
		for _, body := range []string{
			`{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`,
			`{"jso\u006erpc":"2.0","i\u0064":0,"metho\u0064":"eth_chainId","param\u0073":[]}`,
			`{"jsonrpc":"2.0","id":"call","method":"eth_call","params":[ {"data":"0x00","method":"nested parameter data"}, "0x1" ]}`,
		} {
			t.Run(path+body, func(t *testing.T) {
				var received string
				up, count := upstream(t, func(w http.ResponseWriter, r *http.Request) {
					raw, _ := io.ReadAll(r.Body)
					received = string(raw)
					// Read only exact canonical object members, as a case-sensitive
					// upstream would; escaped names decode to those same members.
					var envelope map[string]json.RawMessage
					if err := json.Unmarshal(raw, &envelope); err != nil {
						t.Error(err)
						w.WriteHeader(http.StatusBadRequest)
						return
					}
					var method string
					_ = json.Unmarshal(envelope["method"], &method)
					if method != "eth_chainId" && method != "eth_call" {
						t.Errorf("unexpected upstream method %q", method)
					}
					_ = json.NewEncoder(w).Encode(map[string]any{"jsonrpc": "2.0", "id": envelope["id"], "result": "0x1"})
				})
				result := policyRequest(policyHub(t, up.URL, true, 20), path, body)
				if result.Code != http.StatusOK || count.hits.Load() != 1 || received != body {
					t.Fatalf("valid read changed: status=%d hits=%d received=%q response=%s", result.Code, count.hits.Load(), received, result.Body.String())
				}
			})
		}
		body := `[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},{"jsonrpc":"2.0","id":2,"method":"eth_chainId","params":[]}]`
		up, count := upstream(t, echoPolicyReads)
		result := policyRequest(policyHub(t, up.URL, true, 20), path, body)
		if result.Code != http.StatusOK || count.hits.Load() != 1 || !strings.Contains(result.Body.String(), `"id":2`) {
			t.Fatalf("valid read batch rejected: %d %s hits=%d", result.Code, result.Body.String(), count.hits.Load())
		}
	}
}

func TestStrictEnvelopePreservesCanonicalLegacyWrites(t *testing.T) {
	body := `{"jsonrpc":"2.0","id":1,"method":"eth_sendRawTransaction","params":["0x00"]}`
	for _, path := range []string{"/1", "/1/archive"} {
		up, count := upstream(t, echoPolicyReads)
		result := policyRequest(policyHub(t, up.URL, false, 20), path, body)
		if result.Code != http.StatusOK || count.hits.Load() != 1 {
			t.Fatalf("legacy canonical write rejected on %s: %d %s", path, result.Code, result.Body.String())
		}
	}
}
