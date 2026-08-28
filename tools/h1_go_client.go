// Go net/http oracle for starh2 HTTP/1.1 keep-alive and 100-continue.
// Stdlib only. Shares no parser with starh2 or curl.
package main

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"net/http/httptrace"
	"os"
	"sync/atomic"
	"time"
)

func abort(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "h1-go-smoke ABORT: "+format+"\n", args...)
	os.Exit(1)
}

func main() {
	base := ""
	args := os.Args[1:]
	for i := 0; i < len(args); i++ {
		if args[i] == "--base" {
			i++
			if i >= len(args) {
				abort("--base needs a value")
			}
			base = args[i]
		}
	}
	if base == "" {
		abort("--base http://127.0.0.1:PORT is required")
	}

	transport := &http.Transport{
		DisableKeepAlives:     false,
		MaxIdleConns:          1,
		MaxIdleConnsPerHost:   1,
		IdleConnTimeout:       10 * time.Second,
		ExpectContinueTimeout: 2 * time.Second,
	}
	client := &http.Client{Transport: transport, Timeout: 5 * time.Second}

	proveKeepAlive(client, base)
	proveExpectContinue(client, base)
	fmt.Println("h1-go-smoke PASS keep-alive=1-conn expect-100=ok")
}

func proveKeepAlive(client *http.Client, base string) {
	var connects atomic.Int32
	var reused atomic.Int32
	trace := &httptrace.ClientTrace{
		ConnectDone: func(_, _ string, err error) {
			if err == nil {
				connects.Add(1)
			}
		},
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Reused {
				reused.Add(1)
			}
		},
	}

	url := base + "/hello"
	for i := 0; i < 2; i++ {
		req, err := http.NewRequest(http.MethodGet, url, nil)
		if err != nil {
			abort("keep-alive request: %v", err)
		}
		req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))
		resp, err := client.Do(req)
		if err != nil {
			abort("keep-alive GET %d: %v", i+1, err)
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		if resp.StatusCode != 200 {
			abort("keep-alive GET %d status %d", i+1, resp.StatusCode)
		}
	}

	n := connects.Load()
	if n != 1 {
		abort("keep-alive opened %d TCP connections; want 1", n)
	}
	if reused.Load() != 1 {
		abort("keep-alive second GET did not reuse the connection")
	}
}

func proveExpectContinue(client *http.Client, base string) {
	var got100 atomic.Bool
	trace := &httptrace.ClientTrace{
		Got100Continue: func() {
			got100.Store(true)
		},
	}

	payload := []byte("ping")
	req, err := http.NewRequest(http.MethodPost, base+"/echo", bytes.NewReader(payload))
	if err != nil {
		abort("100-continue request: %v", err)
	}
	req.Header.Set("Expect", "100-continue")
	req.ContentLength = int64(len(payload))
	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	resp, err := client.Do(req)
	if err != nil {
		abort("100-continue POST: %v", err)
	}
	defer resp.Body.Close()
	got, err := io.ReadAll(resp.Body)
	if err != nil {
		abort("100-continue read body: %v", err)
	}
	if !got100.Load() {
		abort("100 Continue was skipped; httptrace.Got100Continue never fired")
	}
	if resp.StatusCode != 200 {
		abort("100-continue final status %d", resp.StatusCode)
	}
	if !bytes.Equal(got, payload) {
		abort("echoed body %q does not match %q — body was not read", got, payload)
	}
}
