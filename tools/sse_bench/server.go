// The Go arm of the SSE benchmark.
//
// It is the opponent because no other Zig HTTP/2 server can stream at all:
// the only one that builds and serves h2 (hendriknielaender/http2.zig) has a
// one-shot handler API, fn(ctx) !Response with setBody, and no flush.
//
// The handler matches the starh2 arm byte for byte: one `data: <nanos>\n\n`
// event per interval, carrying the wall-clock nanosecond count at the moment
// the server wrote it. Same event shape, same interval, same TLS certificate.
//
// /sse-broadcast is the same stream fed by ONE ticker for every stream, the
// shape of an app that pushes one update to many viewers. It stamps the tick
// time, so its latency includes the fan-out to every stream.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"
)

// tickHub is the one ticker behind /sse-broadcast. A stream that missed ticks
// sends the latest stamp once, as the starh2 arm's condition variable does.
type tickHub struct {
	mu   sync.Mutex
	seq  uint64
	ts   int64
	tick chan struct{} // closed and replaced on every tick
}

func (h *tickHub) publish(ts int64) {
	h.mu.Lock()
	h.seq++
	h.ts = ts
	close(h.tick)
	h.tick = make(chan struct{})
	h.mu.Unlock()
}

func (h *tickHub) current() uint64 {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.seq
}

// after returns the newest tick past seen, waiting for one if there is none.
func (h *tickHub) after(seen uint64, done <-chan struct{}) (uint64, int64, bool) {
	for {
		h.mu.Lock()
		seq, ts, tick := h.seq, h.ts, h.tick
		h.mu.Unlock()
		if seq != seen {
			return seq, ts, true
		}
		select {
		case <-done:
			return 0, 0, false
		case <-tick:
		}
	}
}

func main() {
	port := flag.Int("port", 8444, "listen port")
	intervalMs := flag.Int("sse-interval-ms", 100, "event interval")
	cert := flag.String("cert", "testdata/cert.pem", "certificate chain")
	key := flag.String("key", "testdata/key.pem", "private key")
	flag.Parse()

	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "text/plain")
		fmt.Fprint(w, "Hello, World!")
	})

	mux.HandleFunc("/sse", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "no flusher", http.StatusInternalServerError)
			return
		}
		flusher.Flush()
		ticker := time.NewTicker(time.Duration(*intervalMs) * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-r.Context().Done():
				return
			case <-ticker.C:
				// UnixNano, the same clock the starh2 arm reads.
				fmt.Fprintf(w, "data: %d\n\n", time.Now().UnixNano())
				flusher.Flush()
			}
		}
	})

	hub := &tickHub{tick: make(chan struct{})}
	go func() {
		ticker := time.NewTicker(time.Duration(*intervalMs) * time.Millisecond)
		for range ticker.C {
			hub.publish(time.Now().UnixNano())
		}
	}()

	mux.HandleFunc("/sse-broadcast", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("content-type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "no flusher", http.StatusInternalServerError)
			return
		}
		flusher.Flush()
		seen := hub.current()
		for {
			seq, ts, ok := hub.after(seen, r.Context().Done())
			if !ok {
				return
			}
			seen = seq
			fmt.Fprintf(w, "data: %d\n\n", ts)
			flusher.Flush()
		}
	})

	addr := fmt.Sprintf("127.0.0.1:%d", *port)
	srv := &http.Server{Addr: addr, Handler: mux}
	// ListenAndServeTLS negotiates h2 through ALPN automatically.
	// width is the scheduler width this arm actually runs with, so the
	// harness can check that every arm is pinned the same (GOMAXPROCS).
	fmt.Printf("{\"ready\":true,\"port\":%d,\"width\":%d}\n", *port, runtime.GOMAXPROCS(0))
	log.Fatal(srv.ListenAndServeTLS(*cert, *key))
}
