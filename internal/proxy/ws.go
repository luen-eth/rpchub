package proxy

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"rpchub/internal/pool"
	"rpchub/internal/registry"
	"rpchub/internal/wsutil"
)

// hopByHopHeaders must not be forwarded to the upstream: rpchub writes its own
// (relayed) versions of the upgrade headers, and the rest are per-connection.
var hopByHopHeaders = map[string]bool{
	"Connection":          true,
	"Upgrade":             true,
	"Keep-Alive":          true,
	"Proxy-Connection":    true,
	"Proxy-Authenticate":  true,
	"Proxy-Authorization": true,
	"Te":                  true,
	"Trailer":             true,
	"Transfer-Encoding":   true,
	"Host":                true,
}

// WSHandler relays WebSocket connections to a chain's ws endpoint pool.
//
// It never parses WebSocket frames. The client's own handshake — including
// its Sec-WebSocket-Key and any requested extensions — is replayed to the
// upstream, and the upstream's 101 response is written back verbatim. Since
// Sec-WebSocket-Accept is derived from the key we forwarded, the client sees
// a valid handshake and extensions such as permessage-deflate are negotiated
// end to end. After that it is a byte pipe in both directions.
type WSHandler struct {
	Reg     *registry.Registry
	Pools   *pool.Set
	Retries int
	Timeout time.Duration // handshake timeout; the relay itself has no deadline
	Max     int           // max concurrent relayed connections
	Log     *slog.Logger

	open atomic.Int64
}

// GetHandler dispatches "GET /{chain}": a WebSocket upgrade is relayed so one
// URL serves both transports, anything else gets the usage hint. ws may be nil
// when WS_ENABLED is off.
func GetHandler(h *Handler, ws *WSHandler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if ws != nil && wsutil.IsUpgrade(r) {
			ws.Handle(w, r)
			return
		}
		h.MethodHint(w, r)
	}
}

// HandleExplicit serves "GET /{chain}/ws" for clients that prefer a dedicated
// path. A plain GET there is answered with 426 rather than a relay.
func (h *WSHandler) HandleExplicit(w http.ResponseWriter, r *http.Request) {
	if !wsutil.IsUpgrade(r) {
		setCORS(w)
		w.Header().Set("Upgrade", "websocket")
		writeRPCError(w, http.StatusUpgradeRequired,
			fmt.Sprintf("rpchub: connect with a WebSocket client, e.g. wscat -c ws://<host>/%s", r.PathValue("chain")))
		return
	}
	h.Handle(w, r)
}

// Disabled answers the ws routes when WS_ENABLED is off.
func Disabled(w http.ResponseWriter, _ *http.Request) {
	writeRPCError(w, http.StatusNotFound, "rpchub: WebSocket support is disabled (WS_ENABLED=false)")
}

// Handle serves a WebSocket upgrade for "GET /{chain}" or "GET /{chain}/ws".
func (h *WSHandler) Handle(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("chain")
	ch, ok := h.Reg.Resolve(token)
	if !ok {
		writeRPCError(w, http.StatusNotFound, fmt.Sprintf("rpchub: unknown chain %q; see GET /chains", token))
		return
	}
	pl, ok := h.Pools.Get(ch.Key)
	if !ok || len(pl.Endpoints()) == 0 {
		writeRPCError(w, http.StatusServiceUnavailable,
			fmt.Sprintf("rpchub: no WebSocket endpoint known for chain %s", ch.Key))
		return
	}

	if n := h.open.Add(1); int(n) > h.Max {
		h.open.Add(-1)
		w.Header().Set("Retry-After", "5")
		writeRPCError(w, http.StatusServiceUnavailable,
			fmt.Sprintf("rpchub: WebSocket connection limit reached (%d)", h.Max))
		return
	}
	defer h.open.Add(-1)

	upstream, upstreamURL, head, err := h.dialUpstream(r, pl)
	if err != nil {
		h.Log.Warn("websocket upstream unavailable", "chain", ch.Key, "err", err)
		writeRPCError(w, http.StatusBadGateway,
			fmt.Sprintf("rpchub: no WebSocket upstream accepted the connection for chain %s", ch.Key))
		return
	}
	defer upstream.Close()

	// Only hijack once an upstream is committed: before this point the normal
	// ResponseWriter can still deliver a clean JSON error to the client.
	hj, ok := w.(http.Hijacker)
	if !ok {
		writeRPCError(w, http.StatusInternalServerError, "rpchub: server does not support connection hijacking")
		return
	}
	clientConn, clientBuf, err := hj.Hijack()
	if err != nil {
		h.Log.Warn("websocket hijack failed", "chain", ch.Key, "err", err)
		return
	}
	defer clientConn.Close()
	clientConn.SetDeadline(time.Time{}) // subscriptions are long-lived

	if _, err := clientConn.Write(head); err != nil {
		return
	}
	start := time.Now()
	h.Log.Info("websocket relay open", "chain", ch.Key, "upstream", registry.RedactURL(upstreamURL),
		"open_conns", h.open.Load())

	sent, received := relay(clientConn, clientBuf.Reader, upstream)
	pl.ReportSuccess(upstreamURL, time.Since(start), 0)
	h.Log.Info("websocket relay closed", "chain", ch.Key, "upstream", registry.RedactURL(upstreamURL),
		"seconds", int(time.Since(start).Seconds()), "bytes_up", sent, "bytes_down", received)
}

// dialUpstream tries endpoints until one completes the WebSocket handshake.
// Failover is only possible here: once frames flow, subscription ids belong to
// that specific node, so a mid-session switch would silently lose state.
func (h *WSHandler) dialUpstream(r *http.Request, pl *pool.Pool) (net.Conn, string, []byte, error) {
	tried := make(map[string]bool, h.Retries)
	var lastErr error
	for attempt := 0; attempt < h.Retries; attempt++ {
		target, ok := pl.Pick(tried, false)
		if !ok {
			break
		}
		tried[target] = true

		conn, head, err := h.handshake(r.Context(), r, target)
		if err != nil {
			pl.ReportFailure(target, registry.ErrString(err))
			lastErr = err
			if r.Context().Err() != nil {
				return nil, "", nil, r.Context().Err()
			}
			continue
		}
		return conn, target, head, nil
	}
	if lastErr == nil {
		lastErr = fmt.Errorf("no healthy WebSocket upstream")
	}
	return nil, "", nil, lastErr
}

// handshake dials target and replays the client's upgrade request. It returns
// the raw bytes of the upstream's 101 response head, to be forwarded to the
// client untouched.
func (h *WSHandler) handshake(ctx context.Context, r *http.Request, target string) (net.Conn, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, h.Timeout)
	defer cancel()

	conn, u, err := wsutil.DialRaw(ctx, target, h.Timeout)
	if err != nil {
		return nil, nil, err
	}
	conn.SetDeadline(time.Now().Add(h.Timeout))

	var req strings.Builder
	req.WriteString("GET " + wsutil.RequestURI(u) + " HTTP/1.1\r\n")
	req.WriteString("Host: " + u.Host + "\r\n")
	req.WriteString("Upgrade: websocket\r\nConnection: Upgrade\r\n")
	for name, values := range r.Header {
		if hopByHopHeaders[http.CanonicalHeaderKey(name)] {
			continue
		}
		for _, v := range values {
			req.WriteString(name + ": " + v + "\r\n")
		}
	}
	req.WriteString("\r\n")
	if _, err := io.WriteString(conn, req.String()); err != nil {
		conn.Close()
		return nil, nil, err
	}

	br := bufio.NewReader(conn)
	head, err := readResponseHead(br)
	if err != nil {
		conn.Close()
		return nil, nil, err
	}
	if !strings.Contains(strings.SplitN(string(head), "\r\n", 2)[0], " 101 ") {
		status := strings.SplitN(string(head), "\r\n", 2)[0]
		conn.Close()
		return nil, nil, fmt.Errorf("upstream refused upgrade: %s", status)
	}
	conn.SetDeadline(time.Time{})

	// The upstream may already have pushed frames into br; hand those over so
	// the relay does not lose them.
	if n := br.Buffered(); n > 0 {
		buffered, _ := br.Peek(n)
		head = append(head, buffered...)
	}
	return conn, head, nil
}

// readResponseHead reads an HTTP response head verbatim, up to and including
// the blank line, so it can be forwarded byte for byte.
func readResponseHead(br *bufio.Reader) ([]byte, error) {
	const maxHeadBytes = 64 << 10
	var head []byte
	for {
		line, err := br.ReadBytes('\n')
		if err != nil {
			return nil, err
		}
		head = append(head, line...)
		if len(head) > maxHeadBytes {
			return nil, fmt.Errorf("upstream response head too large")
		}
		if len(line) == 2 && line[0] == '\r' { // "\r\n" terminates the head
			return head, nil
		}
		if len(line) == 1 && line[0] == '\n' { // tolerate bare LF
			return head, nil
		}
	}
}

// relay copies bytes both ways until either side closes, returning the byte
// counts client→upstream and upstream→client.
func relay(client net.Conn, clientBuf *bufio.Reader, upstream net.Conn) (sent, received int64) {
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		sent, _ = io.Copy(upstream, clientBuf) // clientBuf may hold early frames
		closeWrite(upstream)
	}()
	go func() {
		defer wg.Done()
		received, _ = io.Copy(client, upstream)
		closeWrite(client)
	}()
	wg.Wait()
	return sent, received
}

// closeWrite signals EOF to the peer without tearing down the other direction.
func closeWrite(c net.Conn) {
	type closeWriter interface{ CloseWrite() error }
	if cw, ok := c.(closeWriter); ok {
		cw.CloseWrite()
		return
	}
	c.Close()
}
