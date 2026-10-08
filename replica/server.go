package replica

import (
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"time"

	"kvstore/engine"
	"kvstore/kvnet"
	"kvstore/raft"
)

// Peer describes one cluster member.
type Peer struct {
	ID         string
	RaftAddr   string // node-to-node RPC
	ClientAddr string // clients (kvnet)
}

// ParsePeers parses "id=raftHost:port/clientHost:port,id2=...".
func ParsePeers(s string) ([]Peer, error) {
	var out []Peer
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		id, addrs, ok1 := strings.Cut(part, "=")
		r, c, ok2 := strings.Cut(addrs, "/")
		if !ok1 || !ok2 || id == "" || r == "" || c == "" {
			return nil, fmt.Errorf("bad peer %q, want id=raftAddr/clientAddr", part)
		}
		out = append(out, Peer{ID: id, RaftAddr: r, ClientAddr: c})
	}
	return out, nil
}

type NodeConfig struct {
	ID    string
	Dir   string // node data dir: holds sm/ (engine data) and raft/ (log, state)
	Peers []Peer // whole cluster, including this node

	Engine engine.Options
	Tune   func(*raft.Config) // optional tweaks (timeouts, retention, ...)
	Logf   func(format string, args ...any)
}

// Server is one running cluster member.
type Server struct {
	Machine *KVMachine
	Node    *raft.Node
	Store   *Store

	rpc       *raft.RPCServer
	transport *raft.TCPTransport
	clients   *kvnet.Server
}

func StartServer(cfg NodeConfig) (*Server, error) {
	var self *Peer
	ids := make([]string, 0, len(cfg.Peers))
	raftAddrs := map[string]string{}
	clientAddrs := map[string]string{}
	for i := range cfg.Peers {
		p := &cfg.Peers[i]
		ids = append(ids, p.ID)
		raftAddrs[p.ID], clientAddrs[p.ID] = p.RaftAddr, p.ClientAddr
		if p.ID == cfg.ID {
			self = p
		}
	}
	if self == nil {
		return nil, fmt.Errorf("replica: node %q is not in the peer list", cfg.ID)
	}
	sm, err := OpenKVMachine(filepath.Join(cfg.Dir, "sm"), &cfg.Engine)
	if err != nil {
		return nil, err
	}
	tr := raft.NewTCPTransport(raftAddrs, time.Second)
	rc := raft.Config{
		ID: cfg.ID, Peers: ids, Dir: filepath.Join(cfg.Dir, "raft"),
		Transport: tr, SM: sm, Logf: cfg.Logf,
	}
	if cfg.Tune != nil {
		cfg.Tune(&rc)
	}
	node, err := raft.NewNode(rc)
	if err != nil {
		sm.Close()
		return nil, err
	}
	s := &Server{Machine: sm, Node: node, transport: tr}
	fail := func(err error) (*Server, error) { s.Close(); return nil, err }

	ln, err := net.Listen("tcp", self.RaftAddr)
	if err != nil {
		return fail(err)
	}
	if s.rpc, err = raft.ServeRPC(ln, node); err != nil {
		ln.Close()
		return fail(err)
	}
	s.Store = NewStore(node, sm, 0)
	s.Store.SetClientAddrs(clientAddrs)
	if s.clients, err = kvnet.Listen(self.ClientAddr, s.Store); err != nil {
		return fail(err)
	}
	node.Start()
	return s, nil
}

func (s *Server) ClientAddr() string { return s.clients.Addr() }

func (s *Server) Close() {
	if s.clients != nil {
		s.clients.Close()
	}
	s.Node.Stop()
	if s.rpc != nil {
		s.rpc.Close()
	}
	s.transport.Close()
	s.Machine.Close()
}
