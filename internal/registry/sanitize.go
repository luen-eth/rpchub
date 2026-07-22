package registry

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
	"unicode"
)

// ErrString renders an error without leaking full endpoint URLs (which can
// embed API keys) into pool state, logs or client-facing messages.
func ErrString(err error) string {
	var ue *url.Error
	if errors.As(err, &ue) {
		return fmt.Sprintf("%s %s: %v", ue.Op, RedactURL(ue.URL), ue.Err)
	}
	return err.Error()
}

// RedactURL hides the path of an RPC URL for logs and the ops API: some
// endpoints (including user EXTRA_RPCS) embed API keys in the path.
func RedactURL(raw string) string {
	u, err := url.Parse(raw)
	if err != nil {
		return "invalid-url"
	}
	if u.Path == "" || u.Path == "/" {
		return u.Scheme + "://" + u.Host
	}
	return u.Scheme + "://" + u.Host + "/…"
}

// Transport tells the two endpoint families apart: request/response JSON-RPC
// over HTTP, and long-lived WebSocket connections carrying subscriptions.
type Transport int

const (
	TransportNone Transport = iota // rejected
	TransportHTTP
	TransportWS
)

func (t Transport) String() string {
	switch t {
	case TransportHTTP:
		return "http"
	case TransportWS:
		return "ws"
	default:
		return "none"
	}
}

// ClassifyURL sanitizes a raw RPC URL from chainlist and reports which pool
// it belongs to. The live data contains zero-width characters, ${API_KEY}
// placeholders and plain garbage ("rpcWorking", bare hostnames), all of which
// are rejected. allowPlaintext admits the unencrypted http:// and ws://
// schemes, which are off by default.
func ClassifyURL(raw string, allowPlaintext bool) (string, Transport) {
	s := strings.Map(func(r rune) rune {
		if !unicode.IsGraphic(r) || unicode.Is(unicode.Cf, r) || unicode.IsSpace(r) {
			return -1 // strips ​ and friends, seen in live chainlist data
		}
		return r
	}, raw)
	if s == "" || strings.Contains(s, "${") {
		return "", TransportNone
	}
	u, err := url.Parse(s)
	if err != nil {
		return "", TransportNone
	}
	var transport Transport
	switch strings.ToLower(u.Scheme) {
	case "https":
		transport = TransportHTTP
	case "wss":
		transport = TransportWS
	case "http":
		if !allowPlaintext {
			return "", TransportNone
		}
		transport = TransportHTTP
	case "ws":
		if !allowPlaintext {
			return "", TransportNone
		}
		transport = TransportWS
	default:
		return "", TransportNone
	}
	if u.Host == "" {
		return "", TransportNone
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	u.Path = strings.TrimRight(u.Path, "/")
	u.Fragment = ""
	return u.String(), transport
}

// CleanURL keeps the HTTP-only view of ClassifyURL.
func CleanURL(raw string, allowHTTP bool) (string, bool) {
	u, transport := ClassifyURL(raw, allowHTTP)
	if transport != TransportHTTP {
		return "", false
	}
	return u, true
}
