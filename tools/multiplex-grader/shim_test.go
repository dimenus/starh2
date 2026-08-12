package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

type memConn struct {
	r *bytes.Reader
	w bytes.Buffer
	// partial write simulation: return at most maxWrite bytes with nil error
	maxWrite  int
	failAfter int
	writes    int
	hardErr   error
}

func (m *memConn) Read(p []byte) (int, error) { return m.r.Read(p) }
func (m *memConn) Write(p []byte) (int, error) {
	m.writes++
	if m.failAfter > 0 && m.writes > m.failAfter {
		if m.hardErr != nil {
			n := 0
			if len(p) > 0 {
				n = 1
				_, _ = m.w.Write(p[:1])
			}
			return n, m.hardErr
		}
		return 0, errors.New("write failed")
	}
	n := len(p)
	if m.maxWrite > 0 && n > m.maxWrite {
		n = m.maxWrite
	}
	wn, err := m.w.Write(p[:n])
	if err != nil {
		return wn, err
	}
	// Ordinary short write: n < len(p), err == nil — shim must loop.
	return wn, nil
}
func (m *memConn) Close() error                       { return nil }
func (m *memConn) LocalAddr() net.Addr                { return dummyAddr{} }
func (m *memConn) RemoteAddr() net.Addr               { return dummyAddr{} }
func (m *memConn) SetDeadline(t time.Time) error      { return nil }
func (m *memConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *memConn) SetWriteDeadline(t time.Time) error { return nil }

type dummyAddr struct{}

func (dummyAddr) Network() string { return "tcp" }
func (dummyAddr) String() string  { return "127.0.0.1:0" }

func frame(ftype byte, flags byte, sid uint32, payload []byte) []byte {
	var b [9]byte
	binary.BigEndian.PutUint32(b[5:9], sid&0x7fffffff)
	b[0] = byte(len(payload) >> 16)
	b[1] = byte(len(payload) >> 8)
	b[2] = byte(len(payload))
	b[3] = ftype
	b[4] = flags
	return append(b[:], payload...)
}

func TestShimSplitPrefaceAndFrames(t *testing.T) {
	mc := &memConn{r: bytes.NewReader(nil)}
	c := newDropConn(mc, 1)

	preface := []byte(clientPreface)
	settings := frame(0x4, 0, 0, []byte{
		0x00, 0x04, 0x00, 0x01, 0x00, 0x00, // INITIAL_WINDOW_SIZE=65536
	})
	wu := frame(0x8, 0, 1, []byte{0x00, 0x00, 0x00, 0x01}) // drop

	n, err := c.Write(preface[:10])
	if err != nil || n != 10 {
		t.Fatalf("preface1: n=%d err=%v", n, err)
	}
	n, err = c.Write(append(preface[10:], append(settings, wu...)...))
	if err != nil {
		t.Fatalf("rest: %v", err)
	}
	_ = n
	if c.dropped.Load() != 1 {
		t.Fatalf("expected 1 dropped WU, got %d", c.dropped.Load())
	}
	if c.initWin.Load() != 64*1024 {
		t.Fatalf("initWin=%d want 64KiB forced", c.initWin.Load())
	}
	if !bytes.Contains(mc.w.Bytes(), settings) {
		t.Fatal("settings not forwarded")
	}
	if bytes.Contains(mc.w.Bytes(), wu) {
		t.Fatal("WINDOW_UPDATE should be dropped")
	}
	if !bytes.Equal(c.forwarded, mc.w.Bytes()) {
		t.Fatalf("forwarded tracking mismatch: %d vs %d", len(c.forwarded), mc.w.Len())
	}
}

func TestShimCoalescedPrefacePlusFrames(t *testing.T) {
	mc := &memConn{r: bytes.NewReader(nil)}
	c := newDropConn(mc, 7)
	settings := frame(0x4, 0, 0, []byte{0x00, 0x04, 0x00, 0x00, 0x10, 0x00})
	blob := append([]byte(clientPreface), settings...)
	if _, err := c.Write(blob); err != nil {
		t.Fatal(err)
	}
	if c.initWin.Load() != 64*1024 {
		t.Fatalf("initWin=%d want 64KiB forced", c.initWin.Load())
	}
}

func TestShimOrdinaryShortWritesLoopToCompletion(t *testing.T) {
	mc := &memConn{r: bytes.NewReader(nil), maxWrite: 3}
	c := newDropConn(mc, 1)
	preface := []byte(clientPreface)
	settings := frame(0x4, 0, 0, []byte{0x00, 0x04, 0x00, 0x00, 0x00, 0x64})
	blob := append(preface, settings...)
	n, err := c.Write(blob)
	if err != nil {
		t.Fatalf("short-write loop must complete with nil err: %v", err)
	}
	if n != len(blob) {
		t.Fatalf("accepted=%d want %d", n, len(blob))
	}
	if len(c.wbuf) != 0 {
		t.Fatalf("wbuf remnant %d — internal loop should have flushed", len(c.wbuf))
	}
	if c.failed {
		t.Fatal("wrapper must not be marked failed on ordinary short writes")
	}
	if c.initWin.Load() != 64*1024 {
		t.Fatalf("initWin=%d", c.initWin.Load())
	}
	if !bytes.Equal(c.forwarded, mc.w.Bytes()) {
		t.Fatal("forwarded bytes must equal underlying write buffer")
	}
}

func TestShimHardErrorMarksFailedNoNilRetry(t *testing.T) {
	mc := &memConn{r: bytes.NewReader(nil), failAfter: 1, hardErr: errors.New("boom")}
	c := newDropConn(mc, 1)
	settings := frame(0x4, 0, 0, []byte{0x00, 0x04, 0x00, 0x00, 0x00, 0x64})
	_, err := c.Write(append([]byte(clientPreface), settings...))
	if err == nil {
		t.Fatal("expected hard write error")
	}
	if !c.failed {
		t.Fatal("wrapper must be marked failed")
	}
	// Retry must not succeed via Write(nil) — wrapper stays failed.
	n2, err2 := c.Write(nil)
	if err2 == nil || n2 != 0 {
		t.Fatalf("retry after fail: n=%d err=%v (must fail closed)", n2, err2)
	}
	if len(c.wbuf) == 0 && c.partialSent == 0 {
		// May have failed during preface; remnant optional.
	}
}

func TestShimForwardedEqualityExact(t *testing.T) {
	mc := &memConn{r: bytes.NewReader(nil), maxWrite: 7}
	c := newDropConn(mc, 1)
	settings := frame(0x4, 0, 0, []byte{0x00, 0x04, 0x00, 0x01, 0x00, 0x00})
	wu := frame(0x8, 0, 1, []byte{0x00, 0x00, 0x00, 0x01})
	blob := append([]byte(clientPreface), append(settings, wu...)...)
	if _, err := c.Write(blob); err != nil {
		t.Fatal(err)
	}
	want := append([]byte(clientPreface), settings...)
	if !bytes.Equal(c.forwarded, want) {
		t.Fatalf("forwarded mismatch len=%d want=%d", len(c.forwarded), len(want))
	}
	if !bytes.Equal(mc.w.Bytes(), want) {
		t.Fatal("underlying conn bytes mismatch")
	}
}

func TestShimSplitFrameAcrossWrites(t *testing.T) {
	mc := &memConn{r: bytes.NewReader(nil)}
	c := newDropConn(mc, 1)
	if _, err := c.Write([]byte(clientPreface)); err != nil {
		t.Fatal(err)
	}
	settings := frame(0x4, 0, 0, []byte{0x00, 0x04, 0x00, 0x00, 0x10, 0x00})
	if _, err := c.Write(settings[:5]); err != nil {
		t.Fatal(err)
	}
	if len(c.wbuf) != 5 {
		t.Fatalf("partial header buffered = %d", len(c.wbuf))
	}
	if _, err := c.Write(settings[5:]); err != nil {
		t.Fatal(err)
	}
	if c.initWin.Load() != 64*1024 {
		t.Fatalf("initWin=%d", c.initWin.Load())
	}
}

func TestShimServerSettingsDoNotOverwriteClientWindow(t *testing.T) {
	serverSettings := frame(0x4, 0, 0, []byte{0x00, 0x04, 0x00, 0x00, 0x00, 0x01}) // IWS=1
	data := frame(0x0, 0, 1, bytes.Repeat([]byte{'x'}, 10))
	mc := &memConn{r: bytes.NewReader(append(serverSettings, data...))}
	c := newDropConn(mc, 1)
	c.initWin.Store(100)
	c.peerWin.Store(100)

	buf := make([]byte, 256)
	_, err := c.Read(buf)
	if err != nil && !errors.Is(err, io.EOF) {
		t.Fatal(err)
	}
	if c.initWin.Load() != 100 || c.peerWin.Load() != 90 {
		t.Fatalf("init=%d peer=%d (server SETTINGS must not reset)", c.initWin.Load(), c.peerWin.Load())
	}
}

func TestShimDataDuringMorphCounted(t *testing.T) {
	data := frame(0x0, 0, 1, []byte("abc"))
	mc := &memConn{r: bytes.NewReader(data)}
	c := newDropConn(mc, 1)
	c.peerWin.Store(3)
	c.morphing.Store(true)
	buf := make([]byte, 64)
	_, _ = c.Read(buf)
	if c.dataDuring.Load() != 3 {
		t.Fatalf("dataDuring=%d", c.dataDuring.Load())
	}
	if !c.windowHitZero.Load() {
		t.Fatal("expected windowHitZero")
	}
}
