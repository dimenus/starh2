// Single-dial 100 concurrent SSE open → cancel/close stress.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/net/http2"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:8080", "host:port")
	n := flag.Int("n", 100, "concurrent SSE streams")
	flag.Parse()

	var dials atomic.Int64
	tr := &http2.Transport{
		AllowHTTP: true,
		DialTLSContext: func(ctx context.Context, network, address string, _ *tls.Config) (net.Conn, error) {
			dials.Add(1)
			d := net.Dialer{Timeout: 5 * time.Second}
			return d.DialContext(ctx, network, address)
		},
		StrictMaxConcurrentStreams: true,
	}
	client := &http.Client{Transport: tr}

	type body struct {
		cancel context.CancelFunc
		rc     interface{ Close() error }
	}
	bodies := make([]body, *n)
	var wg sync.WaitGroup
	var opened atomic.Int64
	var fail atomic.Int64

	for i := 0; i < *n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ctx, cancel := context.WithCancel(context.Background())
			url := fmt.Sprintf("http://%s/sse?datastar={\"nonce\":\"c%d\",\"sequence\":0}", *addr, i)
			req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
			resp, err := client.Do(req)
			if err != nil {
				fail.Add(1)
				cancel()
				return
			}
			bodies[i] = body{cancel: cancel, rc: resp.Body}
			opened.Add(1)
		}(i)
	}
	wg.Wait()

	if dials.Load() != 1 {
		fmt.Printf("{\"pass\":false,\"reason\":\"dials\",\"dials\":%d,\"opened\":%d}\n", dials.Load(), opened.Load())
		os.Exit(1)
	}
	if opened.Load() != int64(*n) || fail.Load() != 0 {
		fmt.Printf("{\"pass\":false,\"reason\":\"open\",\"opened\":%d,\"fail\":%d}\n", opened.Load(), fail.Load())
		os.Exit(1)
	}

	// Abrupt cancel/close all streams, then one more hello on same connection.
	for i := range bodies {
		if bodies[i].cancel != nil {
			bodies[i].cancel()
		}
		if bodies[i].rc != nil {
			_ = bodies[i].rc.Close()
		}
	}
	time.Sleep(200 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, fmt.Sprintf("http://%s/hello", *addr), nil)
	resp, err := client.Do(req)
	ok := err == nil && resp != nil && resp.StatusCode == 200
	if resp != nil {
		_ = resp.Body.Close()
	}
	pass := ok && dials.Load() == 1
	fmt.Printf("{\"pass\":%v,\"dials\":%d,\"opened\":%d,\"hello\":%v}\n", pass, dials.Load(), opened.Load(), ok)
	if !pass {
		os.Exit(1)
	}
}
