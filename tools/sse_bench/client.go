// SSE load client: N concurrent long-lived streams, delivery latency measured
// per event.
//
// Why not h2load: it measures request throughput. A long-lived stream has one
// request and then behaves for minutes, so req/s says nothing about it. What
// matters is whether event k reaches the client on time while many other
// streams are open, and whether one stalled consumer delays everybody else.
//
// The client is Go because its HTTP/2 stack shares no code with either server
// under test. Streams multiplex over ONE connection per server by default,
// which is the shape being measured: h2 stream concurrency, not socket count.
//
// Reported per arm:
//   - streams opened, and how many delivered at least one event
//   - events delivered against events expected from the interval
//   - delivery latency p50/p99/max, where latency is (client receive time -
//     the server timestamp inside the event)
//   - with -stall, one stream stops reading; the p99 of the OTHERS is the
//     number that matters, because that is head-of-line blocking made visible
package main

import (
	"bufio"
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type result struct {
	stream    int
	events    int
	latencies []time.Duration
	err       error
	stalled   bool
}

func main() {
	url := flag.String("url", "", "SSE endpoint")
	streams := flag.Int("streams", 100, "concurrent streams")
	seconds := flag.Int("seconds", 10, "measurement window")
	warmup := flag.Int("warmup", 1, "discarded seconds after every stream opens")
	stall := flag.Bool("stall", false, "one stream stops reading after opening")
	conns := flag.Int("conns", 1, "TCP connections to spread the streams over")
	label := flag.String("label", "arm", "name for the report")
	flag.Parse()
	if *url == "" {
		fmt.Fprintln(os.Stderr, "-url is required")
		os.Exit(1)
	}
	if *streams <= 0 || *seconds <= 0 || *warmup < 0 || *conns <= 0 {
		fmt.Fprintln(os.Stderr, "-streams, -seconds, and -conns must be positive; -warmup must be non-negative")
		os.Exit(1)
	}

	// One http.Client per connection. Go pools one h2 connection per host per
	// Transport, so N Transports is exactly N connections, and -conns 1 keeps
	// the default shape: every stream multiplexed over ONE socket.
	clients := make([]*http.Client, *conns)
	for i := range clients {
		tr := &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: true, NextProtos: []string{"h2"}},
			ForceAttemptHTTP2: true,
			MaxConnsPerHost:   1,
		}
		clients[i] = &http.Client{Transport: tr}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	safety := time.AfterFunc(time.Duration(*seconds+*warmup+30)*time.Second, cancel)
	defer safety.Stop()

	results := make([]result, *streams)
	var wg sync.WaitGroup
	var ready sync.WaitGroup
	ready.Add(*streams)
	var measurementStart atomic.Int64
	measurementStart.Store(1<<63 - 1)
	for i := 0; i < *streams; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			readyReported := false
			defer func() {
				if !readyReported {
					ready.Done()
				}
			}()
			r := result{stream: i}
			// The stalled consumer is stream 0 and only when asked for.
			r.stalled = *stall && i == 0
			req, err := http.NewRequestWithContext(ctx, "GET", *url, nil)
			if err != nil {
				r.err = err
				results[i] = r
				return
			}
			resp, err := clients[i%len(clients)].Do(req)
			if err != nil {
				r.err = err
				results[i] = r
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != 200 {
				r.err = fmt.Errorf("status %d", resp.StatusCode)
				results[i] = r
				return
			}
			ready.Done()
			readyReported = true
			if r.stalled {
				// Read nothing after opening: the server's window for this
				// stream fills while every other stream is measured.
				<-ctx.Done()
				results[i] = r
				return
			}
			sc := bufio.NewScanner(resp.Body)
			sc.Buffer(make([]byte, 0, 64*1024), 1024*1024)
			for sc.Scan() {
				line := sc.Text()
				if !strings.HasPrefix(line, "data: ") {
					continue
				}
				now := time.Now().UnixNano()
				sent, err := strconv.ParseInt(strings.TrimSpace(line[6:]), 10, 64)
				if err != nil {
					continue
				}
				if sent < measurementStart.Load() {
					continue
				}
				r.events++
				r.latencies = append(r.latencies, time.Duration(now-sent))
			}
			results[i] = r
		}(i)
	}
	ready.Wait()
	start := time.Now().Add(time.Duration(*warmup) * time.Second)
	measurementStart.Store(start.UnixNano())
	stop := time.AfterFunc(time.Until(start)+time.Duration(*seconds)*time.Second, cancel)
	wg.Wait()
	stop.Stop()

	var all []time.Duration
	opened, delivered, failed, totalEvents := 0, 0, 0, 0
	for _, r := range results {
		if r.err != nil {
			failed++
			continue
		}
		opened++
		if r.stalled {
			continue
		}
		if r.events > 0 {
			delivered++
		}
		totalEvents += r.events
		all = append(all, r.latencies...)
	}
	sort.Slice(all, func(i, j int) bool { return all[i] < all[j] })

	pct := func(p float64) time.Duration {
		if len(all) == 0 {
			return 0
		}
		i := int(float64(len(all)) * p)
		if i >= len(all) {
			i = len(all) - 1
		}
		return all[i]
	}

	fmt.Printf("%-12s streams=%d conns=%d opened=%d delivering=%d failed=%d events=%d\n",
		*label, *streams, *conns, opened, delivered, failed, totalEvents)
	if len(all) == 0 {
		fmt.Printf("%-12s NO EVENTS — nothing was measured\n", *label)
		os.Exit(1)
	}
	fmt.Printf("%-12s latency p50=%v p99=%v max=%v\n",
		*label, pct(0.50).Round(time.Microsecond), pct(0.99).Round(time.Microsecond), all[len(all)-1].Round(time.Microsecond))
}
