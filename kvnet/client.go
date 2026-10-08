package kvnet

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"kvstore/engine"
)

// Client is safe for concurrent use; calls on one Client are serialised over a
// single connection (use several Clients for parallelism).
//
// A Client made by Dial talks to one server: a broken connection is
// re-dialled on the next call and the failed call is not retried. A Client
// made by DialCluster knows all nodes of a replicated cluster: it follows
// not-leader redirects, rotates to the next node when one is unreachable and
// retries with backoff for up to retryFor. Retrying can apply a write twice
// if the first attempt's outcome was unknown; all operations here are
// idempotent (last write wins), so this is safe unless clients race on a key.
type Client struct {
	timeout  time.Duration
	retryFor time.Duration

	mu    sync.Mutex
	addrs []string
	cur   int
	conn  net.Conn
	r     *bufio.Reader
	w     *bufio.Writer
}

// Dial connects to a single server. timeout bounds every request (0 = 10s).
func Dial(addr string, timeout time.Duration) (*Client, error) {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	c := &Client{addrs: []string{addr}, timeout: timeout}
	if err := c.connect(); err != nil {
		return nil, err
	}
	return c, nil
}

// DialCluster creates a leader-following client for a replicated cluster.
// Connections are made lazily, so some nodes may be down.
func DialCluster(addrs []string, timeout, retryFor time.Duration) (*Client, error) {
	if len(addrs) == 0 {
		return nil, errors.New("kvnet: no addresses")
	}
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	return &Client{addrs: append([]string(nil), addrs...), timeout: timeout, retryFor: retryFor}, nil
}

func (c *Client) connect() error {
	conn, err := net.DialTimeout("tcp", c.addrs[c.cur], c.timeout)
	if err != nil {
		return err
	}
	if tc, ok := conn.(*net.TCPConn); ok {
		tc.SetNoDelay(true)
	}
	c.conn = conn
	c.r = bufio.NewReaderSize(conn, 64<<10)
	c.w = bufio.NewWriterSize(conn, 64<<10)
	return nil
}

func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}

func (c *Client) dropConn() {
	if c.conn != nil {
		c.conn.Close()
		c.conn = nil
	}
}

// rotate moves on to the next known node.
func (c *Client) rotate() {
	c.dropConn()
	c.cur = (c.cur + 1) % len(c.addrs)
}

// follow switches to the leader a node pointed us at (or rotates if unknown).
func (c *Client) follow(addr string) {
	if addr == "" {
		c.rotate()
		return
	}
	for i, a := range c.addrs {
		if a == addr {
			if i != c.cur {
				c.dropConn()
				c.cur = i
			}
			return
		}
	}
	c.dropConn()
	c.addrs = append(c.addrs, addr)
	c.cur = len(c.addrs) - 1
}

// do sends one request and returns the response status and body.
func (c *Client) do(req []byte) (byte, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	deadline := time.Now().Add(c.retryFor)
	backoff := 20 * time.Millisecond
	for {
		st, body, err := c.attempt(req)
		retry := false
		switch {
		case err != nil:
			c.rotate()
			retry = true
		case st == stNotLeader:
			d := &dec{b: body}
			c.follow(string(d.bytes()))
			retry = true
		}
		if !retry || c.retryFor == 0 || !time.Now().Add(backoff).Before(deadline) {
			return st, body, err
		}
		time.Sleep(backoff)
		if backoff *= 2; backoff > 500*time.Millisecond {
			backoff = 500 * time.Millisecond
		}
	}
}

func (c *Client) attempt(req []byte) (byte, []byte, error) {
	if c.conn == nil {
		if err := c.connect(); err != nil {
			return 0, nil, err
		}
	}
	c.conn.SetDeadline(time.Now().Add(c.timeout))
	resp, err := c.roundTrip(req)
	if err != nil {
		c.dropConn()
		return 0, nil, err
	}
	if len(resp) == 0 {
		c.dropConn()
		return 0, nil, errMalformed
	}
	return resp[0], resp[1:], nil
}

func (c *Client) roundTrip(req []byte) ([]byte, error) {
	if err := writeFrame(c.w, req); err != nil {
		return nil, err
	}
	if err := c.w.Flush(); err != nil {
		return nil, err
	}
	return readFrame(c.r)
}

// check converts a non-OK status into an error.
func check(status byte, body []byte) error {
	switch status {
	case stOK:
		return nil
	case stNotFound:
		return engine.ErrNotFound
	case stNotLeader:
		d := &dec{b: body}
		return &NotLeaderError{Leader: string(d.bytes())}
	default:
		d := &dec{b: body}
		msg := d.bytes()
		if d.err != nil {
			return errMalformed
		}
		return errors.New(string(msg))
	}
}

func (c *Client) simple(req []byte) error {
	st, body, err := c.do(req)
	if err != nil {
		return err
	}
	return check(st, body)
}

func (c *Client) Put(key, value []byte) error {
	return c.simple(appendBytes(appendBytes([]byte{opPut}, key), value))
}

func (c *Client) Delete(key []byte) error {
	return c.simple(appendBytes([]byte{opDelete}, key))
}

func (c *Client) Read(key []byte) ([]byte, error) {
	st, body, err := c.do(appendBytes([]byte{opRead}, key))
	if err != nil {
		return nil, err
	}
	if err := check(st, body); err != nil {
		return nil, err
	}
	d := &dec{b: body}
	v := d.bytes()
	if d.err != nil {
		return nil, d.err
	}
	return append([]byte{}, v...), nil
}

func (c *Client) BatchPut(keys, values [][]byte) error {
	if len(keys) != len(values) {
		return fmt.Errorf("kvnet: %d keys but %d values", len(keys), len(values))
	}
	req := appendUvarint([]byte{opBatchPut}, uint64(len(keys)))
	for i := range keys {
		req = appendBytes(appendBytes(req, keys[i]), values[i])
	}
	return c.simple(req)
}

// ReadKeyRange returns pairs with start <= key <= end; nil bounds are open.
func (c *Client) ReadKeyRange(start, end []byte) ([]engine.KV, error) {
	var flags byte
	if start != nil {
		flags |= 1
	}
	if end != nil {
		flags |= 2
	}
	req := []byte{opRange, flags}
	if start != nil {
		req = appendBytes(req, start)
	}
	if end != nil {
		req = appendBytes(req, end)
	}
	st, body, err := c.do(req)
	if err != nil {
		return nil, err
	}
	if err := check(st, body); err != nil {
		return nil, err
	}
	d := &dec{b: body}
	n := d.uvarint()
	if d.err != nil || n > uint64(len(body)) {
		return nil, errMalformed
	}
	out := make([]engine.KV, 0, n)
	for i := uint64(0); i < n; i++ {
		k, v := d.bytes(), d.bytes()
		if d.err != nil {
			return nil, d.err
		}
		out = append(out, engine.KV{Key: append([]byte{}, k...), Value: append([]byte{}, v...)})
	}
	return out, nil
}
