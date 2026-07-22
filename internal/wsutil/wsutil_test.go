package wsutil_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"rpchub/internal/wstest"
	"rpchub/internal/wsutil"
)

func TestIsUpgrade(t *testing.T) {
	cases := []struct {
		upgrade, connection string
		want                bool
	}{
		{"websocket", "Upgrade", true},
		{"WebSocket", "keep-alive, Upgrade", true},
		{"websocket", "upgrade", true},
		{"websocket", "keep-alive", false},
		{"h2c", "Upgrade", false},
		{"", "", false},
	}
	for _, c := range cases {
		r := httptest.NewRequest(http.MethodGet, "/1", nil)
		if c.upgrade != "" {
			r.Header.Set("Upgrade", c.upgrade)
		}
		if c.connection != "" {
			r.Header.Set("Connection", c.connection)
		}
		if got := wsutil.IsUpgrade(r); got != c.want {
			t.Errorf("IsUpgrade(Upgrade=%q, Connection=%q) = %v, want %v", c.upgrade, c.connection, got, c.want)
		}
	}
}

func TestIsWebSocketURL(t *testing.T) {
	for raw, want := range map[string]bool{
		"wss://a.example":   true,
		"ws://a.example":    true,
		"WSS://a.example":   true,
		"https://a.example": false,
		"http://a.example":  false,
		"garbage":           false,
	} {
		if got := wsutil.IsWebSocketURL(raw); got != want {
			t.Errorf("IsWebSocketURL(%q) = %v, want %v", raw, got, want)
		}
	}
}

func TestAcceptKey(t *testing.T) {
	// The example pair from RFC 6455 section 1.3.
	if got := wsutil.AcceptKey("dGhlIHNhbXBsZSBub25jZQ=="); got != "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=" {
		t.Errorf("AcceptKey = %q", got)
	}
}

func TestDialAndRoundTrip(t *testing.T) {
	srv := wstest.NewRPCServer(t, map[string]string{
		"eth_blockNumber": `"0x64"`,
		"eth_chainId":     `"0x1"`,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	c, err := wsutil.Dial(ctx, wstest.URL(srv), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	if err := c.WriteText([]byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber"}`)); err != nil {
		t.Fatal(err)
	}
	msg, err := c.ReadMessage()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(msg), `"0x64"`) {
		t.Fatalf("reply = %s", msg)
	}

	// A second round trip on the same connection must work too.
	if err := c.WriteText([]byte(`{"jsonrpc":"2.0","id":2,"method":"eth_chainId"}`)); err != nil {
		t.Fatal(err)
	}
	if msg, err = c.ReadMessage(); err != nil || !strings.Contains(string(msg), `"0x1"`) {
		t.Fatalf("second reply = %s, %v", msg, err)
	}
}

// TestLargePayloadFraming exercises the 16-bit and 64-bit length paths in
// both directions.
func TestLargePayloadFraming(t *testing.T) {
	srv := wstest.NewServer(t, func(msg []byte) []byte { return msg }) // echo

	ctx := context.Background()
	c, err := wsutil.Dial(ctx, wstest.URL(srv), 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	for _, size := range []int{125, 126, 4096, 70000} {
		payload := []byte(strings.Repeat("x", size))
		if err := c.WriteText(payload); err != nil {
			t.Fatalf("write %d: %v", size, err)
		}
		got, err := c.ReadMessage()
		if err != nil {
			t.Fatalf("read %d: %v", size, err)
		}
		if len(got) != size {
			t.Fatalf("echo size = %d, want %d", len(got), size)
		}
	}
}

func TestDialRejectsNonUpgrade(t *testing.T) {
	srv := wstest.NewRejectingServer(t, http.StatusTooManyRequests)
	if _, err := wsutil.Dial(context.Background(), wstest.URL(srv), 3*time.Second); err == nil {
		t.Fatal("want handshake error for non-101 response")
	} else if !strings.Contains(err.Error(), "429") {
		t.Fatalf("err = %v, want the upstream status", err)
	}
}

func TestDialRejectsBadAcceptKey(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hj := w.(http.Hijacker)
		conn, _, err := hj.Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		conn.Write([]byte("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n" +
			"Connection: Upgrade\r\nSec-WebSocket-Accept: wrong-value\r\n\r\n"))
	}))
	defer srv.Close()

	if _, err := wsutil.Dial(context.Background(), wstest.URL(srv), 3*time.Second); err == nil {
		t.Fatal("want error for wrong Sec-WebSocket-Accept")
	}
}

func TestDialBadURLs(t *testing.T) {
	for _, raw := range []string{"https://a.example", "://", "ws://"} {
		if _, err := wsutil.Dial(context.Background(), raw, time.Second); err == nil {
			t.Errorf("Dial(%q): want error", raw)
		}
	}
}

func TestRequestURI(t *testing.T) {
	for raw, want := range map[string]string{
		"wss://a.example":           "/",
		"wss://a.example/":          "/",
		"wss://a.example/v2/key":    "/v2/key",
		"wss://a.example/p?x=1&y=2": "/p?x=1&y=2",
	} {
		u, err := url.Parse(raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := wsutil.RequestURI(u); got != want {
			t.Errorf("RequestURI(%q) = %q, want %q", raw, got, want)
		}
	}
}
