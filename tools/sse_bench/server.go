// The Go arm of the SSE benchmark.
//
// It is the opponent because no other Zig HTTP/2 server can stream at all:
// the only one that builds and serves h2 (hendriknielaender/http2.zig) has a
// one-shot handler API, fn(ctx) !Response with setBody, and no flush.
//
// The handler matches the starh2 arm byte for byte: one `data: <nanos>\n\n`
// event per interval, carrying the wall-clock nanosecond count at the moment
// the server wrote it. Same event shape, same interval, same TLS certificate.
package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"time"
)

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

	addr := fmt.Sprintf("127.0.0.1:%d", *port)
	srv := &http.Server{Addr: addr, Handler: mux}
	// ListenAndServeTLS negotiates h2 through ALPN automatically.
	// width is the scheduler width this arm actually runs with, so the
	// harness can check that every arm is pinned the same (GOMAXPROCS).
	fmt.Printf("{\"ready\":true,\"port\":%d,\"width\":%d}\n", *port, runtime.GOMAXPROCS(0))
	log.Fatal(srv.ListenAndServeTLS(*cert, *key))
}
