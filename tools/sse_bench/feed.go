// The feeder for the /sse-fed arms: events that come from OUTSIDE the server.
//
// It sends UDP datagrams at random (Poisson) times to a server's -fed-port,
// each naming a stream slot and carrying the send time. The server's one
// reader task wakes only that slot's stream, so no server-side timer decides
// when an event fires. That is the shape of a message bus or a database
// notification feeding many viewers, and it takes the runtimes' timer
// resolution out of the comparison.
//
// Datagram: u64 slot, then i64 unix nanoseconds at send, both little-endian.
//
// It spins on the clock between sends rather than sleeping, so the arrival
// times are what the distribution says, not what this process's timer can
// do. That costs one core on the client side, which is not the server under
// test. A send that runs late keeps the schedule, so the mean rate holds.
package main

import (
	"encoding/binary"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"net"
	"time"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:19490", "server's fed UDP address")
	rate := flag.Float64("rate", 20000, "events per second over all slots")
	slots := flag.Int("slots", 200, "stream slots; must match the server's -fed-slots")
	seconds := flag.Float64("seconds", 12, "how long to send")
	flag.Parse()
	if *rate <= 0 || *slots <= 0 {
		log.Fatal("rate and slots must be positive")
	}

	conn, err := net.Dial("udp", *addr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()

	meanNs := 1e9 / *rate
	var dgram [16]byte
	start := time.Now()
	end := start.Add(time.Duration(*seconds * 1e9))
	next := start
	sent, late := 0, 0
	for {
		next = next.Add(time.Duration(rand.ExpFloat64() * meanNs))
		if next.After(end) {
			break
		}
		now := time.Now()
		if now.After(next) {
			late++
		}
		for now.Before(next) {
			now = time.Now()
		}
		binary.LittleEndian.PutUint64(dgram[0:8], uint64(rand.IntN(*slots)))
		binary.LittleEndian.PutUint64(dgram[8:16], uint64(time.Now().UnixNano()))
		if _, err := conn.Write(dgram[:]); err != nil {
			log.Fatal(err)
		}
		sent++
	}
	elapsed := time.Since(start).Seconds()
	fmt.Printf("feed sent=%d rate=%.0f/s late=%d slots=%d\n", sent, float64(sent)/elapsed, late, *slots)
}
