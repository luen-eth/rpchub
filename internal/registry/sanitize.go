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

// CleanURL normalizes a raw RPC URL from chainlist. The live data contains
// zero-width characters, ${API_KEY} placeholders, wss:// endpoints and plain
// garbage ("rpcWorking", bare hostnames), all of which are rejected here.
// ok=false means the URL must be dropped.
func CleanURL(raw string, allowHTTP bool) (string, bool) {
	s := strings.Map(func(r rune) rune {
		if !unicode.IsGraphic(r) || unicode.Is(unicode.Cf, r) || unicode.IsSpace(r) {
			return -1 // strips ​ and friends, seen in live chainlist data
		}
		return r
	}, raw)
	if s == "" || strings.Contains(s, "${") {
		return "", false
	}
	u, err := url.Parse(s)
	if err != nil {
		return "", false
	}
	switch strings.ToLower(u.Scheme) {
	case "https":
	case "http":
		if !allowHTTP {
			return "", false
		}
	default:
		return "", false
	}
	if u.Host == "" {
		return "", false
	}
	u.Scheme = strings.ToLower(u.Scheme)
	u.Host = strings.ToLower(u.Host)
	u.Path = strings.TrimRight(u.Path, "/")
	u.Fragment = ""
	return u.String(), true
}
