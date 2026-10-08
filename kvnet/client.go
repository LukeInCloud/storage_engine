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
// single connection (use several Clients for parallelism). A broken connection
// is re-dialled on the next call; the failed call itself is not retried.
type Client struct {
	addr    string
	timeout time.Duration

	mu   sync.Mutex
	conn net.Conn
	r    *bufio.Reader
	w    *bufio.Writer
}

// Dial connects to addr. timeout bounds every request (0 = 10s).
func Dial(addr string, timeout time.Duration) (*Client, error) {
	if timeout <= 0 {
		timeout = 10 * time.Second
	}
	c := &Client{addr: addr, timeout: timeout}
	if err := c.connect(); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) connect() error {
	conn, err := net.DialTimeout("tcp", c.addr, c.timeout)
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

// do sends one request and returns the response status and body.
func (c *Client) do(req []byte) (byte, []byte, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		if err := c.connect(); err != nil {
			return 0, nil, err
		}
	}
	c.conn.SetDeadline(time.Now().Add(c.timeout))
	resp, err := c.roundTrip(req)
	if err != nil {
		c.conn.Close()
		c.conn = nil
		return 0, nil, err
	}
	if len(resp) == 0 {
		c.conn.Close()
		c.conn = nil
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
