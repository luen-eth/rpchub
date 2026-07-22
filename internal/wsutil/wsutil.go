// Package wsutil implements the small slice of RFC 6455 that rpchub needs,
// so the project keeps its zero-dependency promise.
//
// Two very different jobs live here:
//
//   - The health prober speaks WebSocket for real: it needs the client
//     handshake plus masked text frames to run eth_blockNumber/getSlot over
//     wss and score endpoints exactly like the HTTP pool does.
//   - The proxy does NOT parse frames at all. It relays the client's own
//     handshake to the upstream and then copies bytes both ways, so
//     extensions such as permessage-deflate are negotiated end to end.
//     DialRaw and RequestURI exist for that path.
package wsutil

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// handshakeGUID is the RFC 6455 magic value used to derive Sec-WebSocket-Accept.
const handshakeGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

const (
	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
)

// maxMessageBytes caps a single application message; RPC replies are small,
// so this only exists to stop a hostile endpoint from exhausting memory.
const maxMessageBytes = 8 << 20

// ErrClosed is returned when the peer sent a close frame.
var ErrClosed = errors.New("websocket: closed by peer")

// IsUpgrade reports whether r is a WebSocket upgrade request. Connection is a
// comma separated list and both header values are case-insensitive.
func IsUpgrade(r *http.Request) bool {
	if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
		return false
	}
	for _, tok := range strings.Split(r.Header.Get("Connection"), ",") {
		if strings.EqualFold(strings.TrimSpace(tok), "upgrade") {
			return true
		}
	}
	return false
}

// IsWebSocketURL reports whether raw is a ws:// or wss:// URL.
func IsWebSocketURL(raw string) bool {
	u, err := url.Parse(raw)
	if err != nil {
		return false
	}
	s := strings.ToLower(u.Scheme)
	return s == "ws" || s == "wss"
}

// DialRaw opens the transport connection for a ws:// or wss:// endpoint
// WITHOUT performing the WebSocket handshake, so the caller can send its own
// (or a relayed) upgrade request.
func DialRaw(ctx context.Context, rawURL string, timeout time.Duration) (net.Conn, *url.URL, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, nil, fmt.Errorf("websocket: bad url: %w", err)
	}
	secure := strings.EqualFold(u.Scheme, "wss")
	if !secure && !strings.EqualFold(u.Scheme, "ws") {
		return nil, nil, fmt.Errorf("websocket: unsupported scheme %q", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return nil, nil, errors.New("websocket: missing host")
	}
	port := u.Port()
	if port == "" {
		port = "80"
		if secure {
			port = "443"
		}
	}
	dialer := &net.Dialer{Timeout: timeout}
	addr := net.JoinHostPort(host, port)
	if !secure {
		conn, err := dialer.DialContext(ctx, "tcp", addr)
		return conn, u, err
	}
	td := &tls.Dialer{NetDialer: dialer, Config: &tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}}
	conn, err := td.DialContext(ctx, "tcp", addr)
	return conn, u, err
}

// RequestURI renders the origin-form request target for an endpoint URL.
func RequestURI(u *url.URL) string {
	uri := u.EscapedPath()
	if uri == "" {
		uri = "/"
	}
	if u.RawQuery != "" {
		uri += "?" + u.RawQuery
	}
	return uri
}

// AcceptKey derives the Sec-WebSocket-Accept value for a client key.
func AcceptKey(clientKey string) string {
	sum := sha1.Sum([]byte(clientKey + handshakeGUID))
	return base64.StdEncoding.EncodeToString(sum[:])
}

// Conn is a minimal client-side WebSocket connection used by the prober.
type Conn struct {
	conn net.Conn
	br   *bufio.Reader
}

// Dial performs a full client handshake against a ws:// or wss:// endpoint.
func Dial(ctx context.Context, rawURL string, timeout time.Duration) (*Conn, error) {
	conn, u, err := DialRaw(ctx, rawURL, timeout)
	if err != nil {
		return nil, err
	}
	if deadline, ok := ctx.Deadline(); ok {
		conn.SetDeadline(deadline)
	} else {
		conn.SetDeadline(time.Now().Add(timeout))
	}

	key, err := nonce()
	if err != nil {
		conn.Close()
		return nil, err
	}
	req := "GET " + RequestURI(u) + " HTTP/1.1\r\n" +
		"Host: " + u.Host + "\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Key: " + key + "\r\n" +
		"Sec-WebSocket-Version: 13\r\n" +
		"User-Agent: rpchub/1.0\r\n\r\n"
	if _, err := io.WriteString(conn, req); err != nil {
		conn.Close()
		return nil, err
	}

	br := bufio.NewReader(conn)
	// NOTE: resp.Body must never be read — for a 101 the remaining bytes on
	// br are WebSocket frames, and the prober keeps reading from br.
	resp, err := http.ReadResponse(br, nil)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("websocket: handshake: %w", err)
	}
	if resp.StatusCode != http.StatusSwitchingProtocols {
		conn.Close()
		return nil, fmt.Errorf("websocket: handshake: HTTP %d", resp.StatusCode)
	}
	if !strings.EqualFold(resp.Header.Get("Upgrade"), "websocket") {
		conn.Close()
		return nil, errors.New("websocket: handshake: missing upgrade header")
	}
	if resp.Header.Get("Sec-WebSocket-Accept") != AcceptKey(key) {
		conn.Close()
		return nil, errors.New("websocket: handshake: bad accept key")
	}
	return &Conn{conn: conn, br: br}, nil
}

// SetDeadline bounds subsequent reads and writes.
func (c *Conn) SetDeadline(t time.Time) error { return c.conn.SetDeadline(t) }

// WriteText sends one unfragmented, masked text message.
func (c *Conn) WriteText(payload []byte) error { return c.writeFrame(opText, payload) }

// ReadMessage returns the next application message, transparently answering
// pings and skipping pongs.
func (c *Conn) ReadMessage() ([]byte, error) {
	var msg []byte
	for {
		fin, op, payload, err := c.readFrame()
		if err != nil {
			return nil, err
		}
		switch op {
		case opPing:
			if err := c.writeFrame(opPong, payload); err != nil {
				return nil, err
			}
			continue
		case opPong:
			continue
		case opClose:
			return nil, ErrClosed
		case opText, opBinary:
			msg = payload
		case opContinuation:
			msg = append(msg, payload...)
		default:
			return nil, fmt.Errorf("websocket: unknown opcode 0x%x", op)
		}
		if len(msg) > maxMessageBytes {
			return nil, errors.New("websocket: message too large")
		}
		if fin {
			return msg, nil
		}
	}
}

// Close sends a close frame (best effort) and drops the connection.
func (c *Conn) Close() error {
	c.conn.SetDeadline(time.Now().Add(time.Second))
	c.writeFrame(opClose, []byte{0x03, 0xe8}) // 1000 normal closure
	return c.conn.Close()
}

func (c *Conn) writeFrame(op byte, payload []byte) error {
	n := len(payload)
	if op >= opClose && n > 125 {
		return errors.New("websocket: control frame too large")
	}
	buf := make([]byte, 0, n+14)
	buf = append(buf, 0x80|op) // FIN set: rpchub never fragments what it sends
	switch {
	case n <= 125:
		buf = append(buf, byte(0x80|n)) // mask bit: client frames must be masked
	case n <= 0xFFFF:
		buf = append(buf, 0x80|126, byte(n>>8), byte(n))
	default:
		buf = append(buf, 0x80|127,
			byte(n>>56), byte(n>>48), byte(n>>40), byte(n>>32),
			byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
	var mask [4]byte
	if _, err := rand.Read(mask[:]); err != nil {
		return err
	}
	buf = append(buf, mask[:]...)
	for i := 0; i < n; i++ {
		buf = append(buf, payload[i]^mask[i%4])
	}
	_, err := c.conn.Write(buf)
	return err
}

func (c *Conn) readFrame() (fin bool, op byte, payload []byte, err error) {
	var head [2]byte
	if _, err = io.ReadFull(c.br, head[:]); err != nil {
		return false, 0, nil, err
	}
	fin = head[0]&0x80 != 0
	op = head[0] & 0x0F
	masked := head[1]&0x80 != 0
	length := uint64(head[1] & 0x7F)

	switch length {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		length = uint64(ext[0])<<8 | uint64(ext[1])
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(c.br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		for _, b := range ext {
			length = length<<8 | uint64(b)
		}
	}
	if length > maxMessageBytes {
		return false, 0, nil, fmt.Errorf("websocket: frame too large (%d bytes)", length)
	}

	var mask [4]byte
	if masked { // servers must not mask, but tolerate it rather than desync
		if _, err = io.ReadFull(c.br, mask[:]); err != nil {
			return false, 0, nil, err
		}
	}
	payload = make([]byte, length)
	if _, err = io.ReadFull(c.br, payload); err != nil {
		return false, 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return fin, op, payload, nil
}

func nonce() (string, error) {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b[:]), nil
}
