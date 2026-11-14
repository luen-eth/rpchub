package registry

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

const localChainlistFixture = `[
  {"name":"Base Mainnet","chainId":8453,"chainSlug":"base","shortName":"base","rpc":[]},
  {"name":"BNB Smart Chain Mainnet","chainId":56,"chainSlug":"bnb","shortName":"bnb","rpc":[]}
]`

func TestLoadLocalChainlistWithExplicitProvidersOnly(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "chains.json")
	if err := os.WriteFile(filename, []byte(localChainlistFixture), 0o600); err != nil {
		t.Fatal(err)
	}
	entries, err := LoadFile(filename)
	if err != nil {
		t.Fatal(err)
	}
	reg := New()
	opts := BuildOptions{
		ChainIDs: []int64{8453, 56},
		ExtraRPCs: map[string][]string{
			"8453": {"https://base-provider.example"},
			"56":   {"https://bnb-provider.example"},
		},
	}
	if err := reg.Update(entries, opts); err != nil {
		t.Fatal(err)
	}
	for key, urls := range opts.ExtraRPCs {
		ch, ok := reg.Resolve(key)
		if !ok || !reflect.DeepEqual(ch.Endpoints, urls) || len(ch.WSEndpoints) != 0 {
			t.Fatalf("chain %s: expected only configured provider, got %+v", key, ch)
		}
	}
	opts.ExtraRPCs = nil
	if err := New().Update(entries, opts); err == nil {
		t.Fatal("empty provider metadata must not silently acquire public providers")
	}
}

func TestLoadLocalChainlistRejectsMissingInvalidAndOversizedFiles(t *testing.T) {
	for _, tc := range []struct {
		name string
		body string
	}{
		{"empty", ""},
		{"empty-list", "[]"},
		{"null", "null"},
		{"object", "{}"},
		{"malformed", `[{"chainId":56`},
		{"trailing-data", localChainlistFixture + "true"},
		{"wrong-field-type", `[{"chainId":"56"}]`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "chains.json")
			if err := os.WriteFile(filename, []byte(tc.body), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := LoadFile(filename); err == nil {
				t.Fatal("invalid local registry must fail closed")
			}
		})
	}
	if _, err := LoadFile(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("missing local registry must fail closed")
	}
	if _, err := LoadFile(t.TempDir()); err == nil {
		t.Fatal("a directory is not a registry file")
	}
	f, err := os.Create(filepath.Join(t.TempDir(), "oversized.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := f.Truncate(maxChainlistBytes + 1); err != nil {
		f.Close()
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadFile(f.Name()); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized local registry: expected size error, got %v", err)
	}
}
