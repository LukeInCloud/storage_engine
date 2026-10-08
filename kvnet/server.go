package kvnet

import (
	"bufio"
	"errors"
	"io"
	"log"
	"net"
	"sync"

	"kvstore/engine"
)

type Server struct {
	store Store
	ln    net.Listener

	mu     sync.Mutex
	conns  map[net.Conn]struct{}
	closed bool
	wg     sync.WaitGroup
}

// Listen starts serving store on addr (e.g. "127.0.0.1:7000", or ":0" for any port).
func Listen(addr string, store Store) (*Server, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := &Server{store: store, ln: ln, conns: map[net.Conn]struct{}{}}
	s.wg.Add(1)
	go s.acceptLoop()
	return s, nil
}

func (s *Server) Addr() string { return s.ln.Addr().String() }

// Close stops accepting, drops all connections and waits for handlers to end.
func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	err := s.ln.Close()
	for c := range s.conns {
		c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return err
}

func (s *Server) acceptLoop() {
	defer s.wg.Done()
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			c.Close()
			return
		}
		s.conns[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		go s.serveConn(c)
	}
}

func (s *Server) serveConn(c net.Conn) {
	defer func() {
		c.Close()
		s.mu.Lock()
		delete(s.conns, c)
		s.mu.Unlock()
		s.wg.Done()
	}()
	if tc, ok := c.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	r := bufio.NewReaderSize(c, 64<<10)
	w := bufio.NewWriterSize(c, 64<<10)
	for {
		req, err := readFrame(r)
		if err != nil {
			if err != io.EOF && !errors.Is(err, net.ErrClosed) && !errors.Is(err, io.ErrUnexpectedEOF) {
				log.Printf("kvnet: %v: %v", c.RemoteAddr(), err)
			}
			return
		}
		if err := writeFrame(w, s.handle(req)); err != nil {
			return
		}
		// Flush only when no further pipelined request is already waiting.
		if r.Buffered() == 0 {
			if err := w.Flush(); err != nil {
				return
			}
		}
	}
}

func errResponse(err error) []byte {
	var nl *NotLeaderError
	switch {
	case errors.Is(err, engine.ErrNotFound):
		return []byte{stNotFound}
	case errors.As(err, &nl):
		return appendBytes([]byte{stNotLeader}, []byte(nl.Leader))
	default:
		return appendBytes([]byte{stError}, []byte(err.Error()))
	}
}

func okResponse() []byte { return []byte{stOK} }

func (s *Server) handle(req []byte) []byte {
	d := &dec{b: req}
	op := d.byte()
	switch op {
	case opPut:
		k, v := d.bytes(), d.bytes()
		if d.err != nil {
			return errResponse(d.err)
		}
		if err := s.store.Put(k, v); err != nil {
			return errResponse(err)
		}
		return okResponse()
	case opRead:
		k := d.bytes()
		if d.err != nil {
			return errResponse(d.err)
		}
		v, err := s.store.Read(k)
		if err != nil {
			return errResponse(err)
		}
		return appendBytes(okResponse(), v)
	case opDelete:
		k := d.bytes()
		if d.err != nil {
			return errResponse(d.err)
		}
		if err := s.store.Delete(k); err != nil {
			return errResponse(err)
		}
		return okResponse()
	case opBatchPut:
		n := d.uvarint()
		if d.err != nil || n > uint64(len(d.b)) { // each pair takes >= 2 bytes
			return errResponse(errMalformed)
		}
		keys, vals := make([][]byte, 0, n), make([][]byte, 0, n)
		for i := uint64(0); i < n; i++ {
			keys = append(keys, d.bytes())
			vals = append(vals, d.bytes())
		}
		if d.err != nil {
			return errResponse(d.err)
		}
		if err := s.store.BatchPut(keys, vals); err != nil {
			return errResponse(err)
		}
		return okResponse()
	case opRange:
		flags := d.byte()
		var start, end []byte
		if flags&1 != 0 {
			start = d.bytes()
			if start == nil && d.err == nil {
				start = []byte{}
			}
		}
		if flags&2 != 0 {
			end = d.bytes()
			if end == nil && d.err == nil {
				end = []byte{}
			}
		}
		if d.err != nil {
			return errResponse(d.err)
		}
		kvs, err := s.store.ReadKeyRange(start, end)
		if err != nil {
			return errResponse(err)
		}
		out := appendUvarint(okResponse(), uint64(len(kvs)))
		for _, kv := range kvs {
			out = appendBytes(out, kv.Key)
			out = appendBytes(out, kv.Value)
			if len(out) > MaxFrame {
				return errResponse(errors.New("kvnet: range result too large; narrow the range"))
			}
		}
		return out
	default:
		return errResponse(errors.New("kvnet: unknown operation"))
	}
}
