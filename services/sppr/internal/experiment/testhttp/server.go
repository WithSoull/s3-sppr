// Package testhttp runs a real httptest HTTP server over net.Pipe. Experiment
// tests need streaming responses even in sandboxes which forbid binding TCP.
package testhttp

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
)

type listener struct {
	conns chan net.Conn
	done  chan struct{}
	once  sync.Once
}

func (l *listener) Accept() (net.Conn, error) {
	select {
	case c := <-l.conns:
		return c, nil
	case <-l.done:
		return nil, net.ErrClosed
	}
}
func (l *listener) Close() error   { l.once.Do(func() { close(l.done) }); return nil }
func (l *listener) Addr() net.Addr { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 80} }
func NewServer(h http.Handler) *httptest.Server {
	l := &listener{conns: make(chan net.Conn), done: make(chan struct{})}
	s := &httptest.Server{Listener: l, Config: &http.Server{Handler: h}}
	s.Start()
	s.Client().Transport = &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		client, server := net.Pipe()
		select {
		case l.conns <- server:
			return client, nil
		case <-ctx.Done():
			client.Close()
			server.Close()
			return nil, ctx.Err()
		case <-l.done:
			client.Close()
			server.Close()
			return nil, net.ErrClosed
		}
	}}
	return s
}
