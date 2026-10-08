package raft

import (
	"errors"
	"net"
	"net/rpc"
	"sync"
	"time"
)

// TCPTransport is a Transport over net/rpc (gob), one cached connection per peer.
type TCPTransport struct {
	addrs   map[string]string
	timeout time.Duration

	mu      sync.Mutex
	clients map[string]*rpc.Client
}

// NewTCPTransport maps node IDs to Raft RPC addresses. timeout bounds each
// call (snapshot chunks get at least 10s).
func NewTCPTransport(addrs map[string]string, timeout time.Duration) *TCPTransport {
	if timeout <= 0 {
		timeout = time.Second
	}
	return &TCPTransport{addrs: addrs, timeout: timeout, clients: map[string]*rpc.Client{}}
}

func (t *TCPTransport) client(to string) (*rpc.Client, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if c := t.clients[to]; c != nil {
		return c, nil
	}
	addr, ok := t.addrs[to]
	if !ok {
		return nil, errors.New("raft: unknown peer " + to)
	}
	conn, err := net.DialTimeout("tcp", addr, t.timeout)
	if err != nil {
		return nil, err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	c := rpc.NewClient(conn)
	t.clients[to] = c
	return c, nil
}

func (t *TCPTransport) call(to, method string, timeout time.Duration, args, reply any) error {
	c, err := t.client(to)
	if err != nil {
		return err
	}
	done := c.Go("Raft."+method, args, reply, make(chan *rpc.Call, 1)).Done
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case call := <-done:
		if call.Error != nil {
			t.drop(to, c)
		}
		return call.Error
	case <-timer.C:
		t.drop(to, c)
		return ErrTimeout
	}
}

func (t *TCPTransport) drop(to string, c *rpc.Client) {
	t.mu.Lock()
	if t.clients[to] == c {
		delete(t.clients, to)
	}
	t.mu.Unlock()
	c.Close()
}

func (t *TCPTransport) RequestVote(to string, r *VoteReq) (*VoteResp, error) {
	var resp VoteResp
	if err := t.call(to, "RequestVote", t.timeout, r, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (t *TCPTransport) AppendEntries(to string, r *AppendReq) (*AppendResp, error) {
	var resp AppendResp
	if err := t.call(to, "AppendEntries", 2*t.timeout, r, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (t *TCPTransport) InstallSnapshot(to string, r *SnapReq) (*SnapResp, error) {
	timeout := t.timeout
	if timeout < 10*time.Second {
		timeout = 10 * time.Second
	}
	var resp SnapResp
	if err := t.call(to, "InstallSnapshot", timeout, r, &resp); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (t *TCPTransport) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()
	for id, c := range t.clients {
		c.Close()
		delete(t.clients, id)
	}
}

// RPCServer serves a Node's handlers to its peers.
type RPCServer struct {
	ln net.Listener
	wg sync.WaitGroup

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
}

type rpcService struct{ n *Node }

func (s *rpcService) RequestVote(req *VoteReq, resp *VoteResp) error {
	r, err := s.n.HandleRequestVote(req)
	if err != nil {
		return err
	}
	*resp = *r
	return nil
}

func (s *rpcService) AppendEntries(req *AppendReq, resp *AppendResp) error {
	r, err := s.n.HandleAppendEntries(req)
	if err != nil {
		return err
	}
	*resp = *r
	return nil
}

func (s *rpcService) InstallSnapshot(req *SnapReq, resp *SnapResp) error {
	r, err := s.n.HandleInstallSnapshot(req)
	if err != nil {
		return err
	}
	*resp = *r
	return nil
}

// ServeRPC accepts peer connections on ln until Close.
func ServeRPC(ln net.Listener, n *Node) (*RPCServer, error) {
	srv := rpc.NewServer()
	if err := srv.RegisterName("Raft", &rpcService{n}); err != nil {
		return nil, err
	}
	s := &RPCServer{ln: ln, conns: map[net.Conn]struct{}{}}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			if tc, ok := conn.(*net.TCPConn); ok {
				tc.SetNoDelay(true)
			}
			s.mu.Lock()
			if s.closed {
				s.mu.Unlock()
				conn.Close()
				return
			}
			s.conns[conn] = struct{}{}
			s.mu.Unlock()
			go func() {
				srv.ServeConn(conn) // returns when the connection closes
				s.mu.Lock()
				delete(s.conns, conn)
				s.mu.Unlock()
			}()
		}
	}()
	return s, nil
}

func (s *RPCServer) Addr() string { return s.ln.Addr().String() }

// Close stops accepting and drops all peer connections.
func (s *RPCServer) Close() {
	s.mu.Lock()
	s.closed = true
	s.ln.Close()
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}
