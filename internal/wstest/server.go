// Package wstest provides a tiny WebSocket server used only by rpchub's
// tests: the prober needs something to talk to, and the proxy relay needs a
// fake upstream. It implements the server half of RFC 6455 (unmasked frames
// out, masked frames in) with no dependencies.
package wstest

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"rpchub/internal/wsutil"
)

const (
	opContinuation = 0x0
	opText         = 0x1
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA
)

// NewServer starts a WebSocket server that answers every text message with
// handle(msg). Returning nil from handle closes the connection.
func NewServer(t *testing.T, handle func(msg []byte) []byte) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !wsutil.IsUpgrade(r) {
			http.Error(w, "expected websocket upgrade", http.StatusBadRequest)
			return
		}
		conn, br, err := upgrade(w, r)
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			fin, op, payload, err := readFrame(br)
			if err != nil {
				return
			}
			switch op {
			case opClose:
				return
			case opPing:
				if err := writeFrame(conn, opPong, payload); err != nil {
					return
				}
			case opText, opContinuation:
				if !fin { // tests never fragment their requests
					return
				}
				reply := handle(payload)
				if reply == nil {
					return
				}
				if err := writeFrame(conn, opText, reply); err != nil {
					return
				}
			}
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// NewRPCServer answers JSON-RPC requests by method name. Unknown methods get
// a -32601 error, mirroring a real node.
func NewRPCServer(t *testing.T, results map[string]string) *httptest.Server {
	t.Helper()
	return NewServer(t, func(msg []byte) []byte {
		var req struct {
			Method string `json:"method"`
		}
		json.Unmarshal(msg, &req)
		if res, ok := results[req.Method]; ok {
			return []byte(`{"jsonrpc":"2.0","id":1,"result":` + res + `}`)
		}
		return []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32601,"message":"method not found"}}`)
	})
}

// NewRejectingServer never upgrades; it answers the handshake with status.
func NewRejectingServer(t *testing.T, status int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "no websocket here", status)
	}))
	t.Cleanup(srv.Close)
	return srv
}

// URL converts an httptest http:// URL into its ws:// form.
func URL(srv *httptest.Server) string {
	return "ws" + strings.TrimPrefix(srv.URL, "http")
}

func upgrade(w http.ResponseWriter, r *http.Request) (net.Conn, *bufio.Reader, error) {
	hj, ok := w.(http.Hijacker)
	if !ok {
		http.Error(w, "no hijack", http.StatusInternalServerError)
		return nil, nil, http.ErrNotSupported
	}
	conn, rw, err := hj.Hijack()
	if err != nil {
		return nil, nil, err
	}
	accept := wsutil.AcceptKey(r.Header.Get("Sec-WebSocket-Key"))
	resp := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\nConnection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + accept + "\r\n\r\n"
	if _, err := io.WriteString(conn, resp); err != nil {
		conn.Close()
		return nil, nil, err
	}
	return conn, rw.Reader, nil
}

// writeFrame sends an unmasked server frame.
func writeFrame(w io.Writer, op byte, payload []byte) error {
	n := len(payload)
	buf := make([]byte, 0, n+10)
	buf = append(buf, 0x80|op)
	switch {
	case n <= 125:
		buf = append(buf, byte(n))
	case n <= 0xFFFF:
		buf = append(buf, 126, byte(n>>8), byte(n))
	default:
		buf = append(buf, 127, 0, 0, 0, 0, byte(n>>24), byte(n>>16), byte(n>>8), byte(n))
	}
	buf = append(buf, payload...)
	_, err := w.Write(buf)
	return err
}

func readFrame(br *bufio.Reader) (fin bool, op byte, payload []byte, err error) {
	var head [2]byte
	if _, err = io.ReadFull(br, head[:]); err != nil {
		return false, 0, nil, err
	}
	fin = head[0]&0x80 != 0
	op = head[0] & 0x0F
	masked := head[1]&0x80 != 0
	length := uint64(head[1] & 0x7F)
	switch length {
	case 126:
		var ext [2]byte
		if _, err = io.ReadFull(br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		length = uint64(ext[0])<<8 | uint64(ext[1])
	case 127:
		var ext [8]byte
		if _, err = io.ReadFull(br, ext[:]); err != nil {
			return false, 0, nil, err
		}
		for _, b := range ext {
			length = length<<8 | uint64(b)
		}
	}
	var mask [4]byte
	if masked {
		if _, err = io.ReadFull(br, mask[:]); err != nil {
			return false, 0, nil, err
		}
	}
	payload = make([]byte, length)
	if _, err = io.ReadFull(br, payload); err != nil {
		return false, 0, nil, err
	}
	if masked {
		for i := range payload {
			payload[i] ^= mask[i%4]
		}
	}
	return fin, op, payload, nil
}
