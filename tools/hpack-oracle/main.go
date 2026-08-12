package main

import (
	"fmt"

	"golang.org/x/net/http2/hpack"
)

func main() {
	fmt.Println("257")
	for i := 0; i < 256; i++ {
		enc := hpack.AppendHuffmanString(nil, string([]byte{byte(i)}))
		fmt.Printf("%x\n", enc)
	}
	fmt.Println("3fffffff1e") // EOS code=0x3fffffff bits=30 — marker only
}
