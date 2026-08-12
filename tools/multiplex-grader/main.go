// Multiplex grader (DESIGN §12.3).
// Stateful HTTP/2 frame shim: client-preface pass-through + incremental framing
// on both directions. Drops client→server WINDOW_UPDATE for one stream while
// recording the peer's advertised stream window and DATA received during morphs.
package main

import (
	"context"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
)

const clientPreface = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"

type dropWindowUpdateConn struct {
	net.Conn
	streamID uint32

	wmu     sync.Mutex
	wbuf    []byte
	preface int // bytes of client preface still to pass through unchanged
	// Bytes of the first buffered frame already written to Conn (partial-write resume).
	partialSent int
	failed      bool

	rmu  sync.Mutex
	rbuf []byte

	dropped       atomic.Int64
	peerWin       atomic.Int32 // remaining send window we believe the server has for streamID
	initWin       atomic.Int32 // SETTINGS_INITIAL_WINDOW_SIZE observed (or 65535)
	dataDuring    atomic.Int64 // DATA bytes for stalled stream observed during morphs
	morphing      atomic.Bool
	windowHitZero atomic.Bool

	// Exact bytes forwarded to the underlying Conn (excluding dropped WU).
	forwarded []byte
}

func newDropConn(c net.Conn, streamID uint32) *dropWindowUpdateConn {
	d := &dropWindowUpdateConn{Conn: c, streamID: streamID, preface: len(clientPreface)}
	d.initWin.Store(65535)
	d.peerWin.Store(65535)
	return d
}

func (c *dropWindowUpdateConn) Write(p []byte) (int, error) {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.failed {
		return 0, errors.New("shim write wrapper failed")
	}

	// Buffering contract: every accepted byte of p is counted. Unsent socket
	// remnants stay in wbuf; partialSent resumes the first frame without loss.
	accepted := 0
	for len(p) > 0 {
		if c.preface > 0 {
			n := c.preface
			if n > len(p) {
				n = len(p)
			}
			off := 0
			for off < n {
				wn, err := c.Conn.Write(p[off:n])
				if wn > 0 {
					c.forwarded = append(c.forwarded, p[off:off+wn]...)
					accepted += wn
					c.preface -= wn
					off += wn
				}
				if err != nil {
					c.failed = true
					return accepted, err
				}
				if wn == 0 {
					c.failed = true
					return accepted, io.ErrShortWrite
				}
			}
			p = p[n:]
			continue
		}
		c.wbuf = append(c.wbuf, p...)
		accepted += len(p)
		p = nil
	}
	if err := c.flushWbuf(); err != nil {
		return accepted, err
	}
	return accepted, nil
}

func (c *dropWindowUpdateConn) flushWbuf() error {
	for {
		if c.failed {
			return errors.New("shim write wrapper failed")
		}
		if len(c.wbuf) < 9 {
			return nil
		}
		length := int(c.wbuf[0])<<16 | int(c.wbuf[1])<<8 | int(c.wbuf[2])
		frameLen := 9 + length
		if len(c.wbuf) < frameLen {
			return nil
		}
		frame := c.wbuf[:frameLen]
		ftype := frame[3]
		sid := binary.BigEndian.Uint32(frame[5:9]) & 0x7fffffff
		if ftype == 0x4 && sid == 0 && length%6 == 0 && frame[4]&0x1 == 0 {
			payload := frame[9:frameLen]
			for i := 0; i+6 <= len(payload); i += 6 {
				id := binary.BigEndian.Uint16(payload[i : i+2])
				if id == 0x4 {
					const forced uint32 = 64 * 1024
					binary.BigEndian.PutUint32(payload[i+2:i+6], forced)
					c.initWin.Store(int32(forced))
					c.peerWin.Store(int32(forced))
				}
			}
		}
		if ftype == 0x8 && sid == c.streamID {
			c.dropped.Add(1)
			c.wbuf = c.wbuf[frameLen:]
			c.partialSent = 0
			continue
		}
		off := c.partialSent
		for off < len(frame) {
			wn, err := c.Conn.Write(frame[off:])
			if wn > 0 {
				c.forwarded = append(c.forwarded, frame[off:off+wn]...)
				off += wn
			}
			if err != nil {
				// (n>0, err!=nil): retain unsent, mark failed — caller must not retry.
				c.partialSent = off
				c.failed = true
				return err
			}
			if wn == 0 {
				c.partialSent = off
				c.failed = true
				return io.ErrShortWrite
			}
			// Ordinary short write with nil error: loop internally.
		}
		c.partialSent = 0
		c.wbuf = c.wbuf[frameLen:]
	}
}

func (c *dropWindowUpdateConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if n > 0 {
		c.rmu.Lock()
		c.rbuf = append(c.rbuf, p[:n]...)
		c.consumeReadFrames()
		c.rmu.Unlock()
	}
	return n, err
}

func (c *dropWindowUpdateConn) consumeReadFrames() {
	for {
		if len(c.rbuf) < 9 {
			return
		}
		length := int(c.rbuf[0])<<16 | int(c.rbuf[1])<<8 | int(c.rbuf[2])
		frameLen := 9 + length
		if len(c.rbuf) < frameLen {
			return
		}
		frame := c.rbuf[:frameLen]
		ftype := frame[3]
		sid := binary.BigEndian.Uint32(frame[5:9]) & 0x7fffffff
		payload := frame[9:frameLen]

		switch ftype {
		case 0x0: // DATA
			if sid == c.streamID {
				pad := 0
				dataLen := length
				if frame[4]&0x8 != 0 && length > 0 {
					pad = int(payload[0])
					dataLen = length - 1 - pad
					if dataLen < 0 {
						dataLen = 0
					}
				}
				c.peerWin.Add(-int32(dataLen))
				if c.morphing.Load() {
					c.dataDuring.Add(int64(dataLen))
					if c.peerWin.Load() <= 0 {
						c.windowHitZero.Store(true)
					}
				} else if c.peerWin.Load() <= 0 {
					c.windowHitZero.Store(true)
				}
			}
		case 0x4: // SETTINGS from server — do not overwrite client window tracking.
		case 0x8: // WINDOW_UPDATE from server — ignore for stall proof.
		}
		c.rbuf = c.rbuf[frameLen:]
	}
}

func main() {
	mode := flag.String("mode", "h2c", "h2c|tls")
	addr := flag.String("addr", "127.0.0.1:8080", "host:port")
	sseN := flag.Int("sse", 100, "SSE streams")
	morphN := flag.Int("morph", 200, "morph POSTs while SSE open")
	flag.Parse()

	start := time.Now()
	var dials atomic.Int64
	var wrap *dropWindowUpdateConn
	var wrapOnce sync.Once

	transport := &http2.Transport{
		AllowHTTP: *mode == "h2c",
		DialTLSContext: func(ctx context.Context, network, address string, cfg *tls.Config) (net.Conn, error) {
			dials.Add(1)
			d := net.Dialer{Timeout: 5 * time.Second}
			c, err := d.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			var base net.Conn = c
			if *mode == "tls" {
				tc := tls.Client(c, cfg)
				if err := tc.HandshakeContext(ctx); err != nil {
					_ = c.Close()
					return nil, err
				}
				base = tc
			}
			wrapOnce.Do(func() {
				wrap = newDropConn(base, 1) // first client stream
			})
			// Single TCP connection: always return the wrapping conn.
			if wrap != nil {
				return wrap, nil
			}
			return base, nil
		},
		StrictMaxConcurrentStreams: true,
	}
	if *mode == "tls" {
		transport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h2"}}
	}
	client := &http.Client{Transport: transport}
	scheme := "http"
	if *mode == "tls" {
		scheme = "https"
	}

	type sseState struct {
		nonce  string
		before atomic.Bool
		after  atomic.Bool
		body   io.ReadCloser
		cancel context.CancelFunc
	}
	states := make([]*sseState, *sseN)
	var wg sync.WaitGroup
	var okSSE atomic.Int64
	var fail atomic.Int64
	stalled := 0

	// Open the stalled/exhaust stream first so it is HTTP/2 stream 1 (wrap target).
	openOne := func(i int) {
		nonce := fmt.Sprintf("n%d", i)
		ctx, cancel := context.WithCancel(context.Background())
		url := fmt.Sprintf("%s://%s/sse?datastar={\"nonce\":\"%s\",\"sequence\":0}", scheme, *addr, nonce)
		if i == stalled {
			url = fmt.Sprintf("%s://%s/sse?exhaust=1&datastar={\"nonce\":\"%s\",\"sequence\":0}", scheme, *addr, nonce)
		}
		req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
		reqStart := time.Now()
		resp, err := client.Do(req)
		if err != nil {
			fail.Add(1)
			cancel()
			return
		}
		if time.Since(reqStart) > 2*time.Second {
			fail.Add(1)
			cancel()
			_ = resp.Body.Close()
			return
		}
		st := &sseState{nonce: nonce, body: resp.Body, cancel: cancel}
		states[i] = st
		if i == stalled {
			buf := make([]byte, 64*1024)
			deadline := time.Now().Add(8 * time.Second)
			for time.Now().Before(deadline) {
				if wrap != nil && wrap.peerWin.Load() <= 0 {
					wrap.windowHitZero.Store(true)
					break
				}
				n, rerr := resp.Body.Read(buf)
				if n == 0 {
					if rerr != nil {
						time.Sleep(20 * time.Millisecond)
					}
					continue
				}
			}
			okSSE.Add(1)
			st.before.Store(true)
			return
		}
		buf := make([]byte, 4096)
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			n, rerr := resp.Body.Read(buf)
			if n > 0 && strings.Contains(string(buf[:n]), nonce) {
				st.before.Store(true)
				okSSE.Add(1)
				return
			}
			if rerr == io.EOF {
				break
			}
		}
		fail.Add(1)
	}

	openOne(stalled)
	for i := 0; i < *sseN; i++ {
		if i == stalled {
			continue
		}
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			openOne(i)
		}(i)
	}
	wg.Wait()

	// Wait until stalled stream window is exhausted (using observed init window).
	if wrap != nil {
		deadlineFill := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadlineFill) {
			if wrap.peerWin.Load() <= 0 {
				wrap.windowHitZero.Store(true)
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		// Quiesce so in-flight DATA for the stalled stream drains before morph.
		if wrap.peerWin.Load() <= 0 {
			time.Sleep(200 * time.Millisecond)
			wrap.dataDuring.Store(0)
		}
	}

	if wrap != nil {
		wrap.morphing.Store(true)
	}
	var morphOK atomic.Int64
	var morphWG sync.WaitGroup
	sem := make(chan struct{}, 156)
	for i := 0; i < *morphN; i++ {
		morphWG.Add(1)
		go func(i int) {
			defer morphWG.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			nonce := fmt.Sprintf("m%d", i)
			body := fmt.Sprintf("{\"nonce\":%q,\"sequence\":1}", nonce)
			url := fmt.Sprintf("%s://%s/morph", scheme, *addr)
			req, _ := http.NewRequest(http.MethodPost, url, strings.NewReader(body))
			req.Header.Set("content-type", "application/json")
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			req = req.WithContext(ctx)
			reqStart := time.Now()
			resp, err := client.Do(req)
			if err != nil {
				fail.Add(1)
				return
			}
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			if time.Since(reqStart) > 2*time.Second {
				fail.Add(1)
				return
			}
			if resp.StatusCode == 200 && strings.Contains(string(b), nonce) {
				morphOK.Add(1)
			} else {
				fail.Add(1)
			}
		}(i)
	}
	morphWG.Wait()
	heldDuringMorph := false
	if wrap != nil {
		wrap.morphing.Store(false)
		// Held-without-WINDOW_UPDATE: window exhausted, WUs dropped or withheld,
		// and no further DATA for the stalled stream during the morph burst.
		heldDuringMorph = wrap.windowHitZero.Load() &&
			wrap.peerWin.Load() <= 0 &&
			int64(wrap.initWin.Load()) > 0 &&
			wrap.dropped.Load() > 0 &&
			wrap.dataDuring.Load() == 0
	}

	var afterOK atomic.Int64
	for i, st := range states {
		if st == nil || i == stalled {
			continue
		}
		buf := make([]byte, 4096)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		done := make(chan struct{})
		go func() {
			n, _ := st.body.Read(buf)
			if n > 0 && strings.Contains(string(buf[:n]), st.nonce) {
				st.after.Store(true)
				afterOK.Add(1)
			}
			close(done)
		}()
		select {
		case <-done:
		case <-ctx.Done():
			fail.Add(1)
		}
		cancel()
	}

	for i := 0; i < 5 && i < len(states); i++ {
		if states[i] != nil {
			states[i].cancel()
			_ = states[i].body.Close()
		}
	}
	probeOK := false
	{
		url := fmt.Sprintf("%s://%s/hello", scheme, *addr)
		req, _ := http.NewRequest(http.MethodGet, url, nil)
		req.Header.Set("x-grader-nonce", "probe")
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		req = req.WithContext(ctx)
		resp, err := client.Do(req)
		if err == nil {
			b, _ := io.ReadAll(resp.Body)
			resp.Body.Close()
			probeOK = resp.StatusCode == 200 && strings.Contains(string(b), "probe")
		}
	}

	for _, st := range states {
		if st != nil {
			st.cancel()
			_ = st.body.Close()
		}
	}

	elapsed := time.Since(start)
	wantAfter := int64(*sseN - 1)
	if wantAfter < 0 {
		wantAfter = 0
	}
	pass := dials.Load() == 1 &&
		okSSE.Load() == int64(*sseN) &&
		morphOK.Load() == int64(*morphN) &&
		afterOK.Load() == wantAfter &&
		fail.Load() == 0 &&
		probeOK &&
		heldDuringMorph &&
		elapsed <= 15*time.Second

	initW, peerW, dropped, dataD := int32(0), int32(0), int64(0), int64(0)
	if wrap != nil {
		initW = wrap.initWin.Load()
		peerW = wrap.peerWin.Load()
		dropped = wrap.dropped.Load()
		dataD = wrap.dataDuring.Load()
	}
	fmt.Printf("{\"dials\":%d,\"sse_ok\":%d,\"morph_ok\":%d,\"after_ok\":%d,\"fail\":%d,\"probe\":%v,\"held_during_morph\":%v,\"init_win\":%d,\"peer_win\":%d,\"wu_dropped\":%d,\"data_during_morph\":%d,\"elapsed_ms\":%d,\"sse\":%d,\"morph\":%d,\"pass\":%v}\n",
		dials.Load(), okSSE.Load(), morphOK.Load(), afterOK.Load(), fail.Load(), probeOK, heldDuringMorph,
		initW, peerW, dropped, dataD, elapsed.Milliseconds(), *sseN, *morphN, pass)
	if !pass {
		os.Exit(1)
	}
}
