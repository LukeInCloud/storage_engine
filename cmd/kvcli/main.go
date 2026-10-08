// kvcli is a tiny command line client.
//
//	kvcli [-addr host:port] put KEY VALUE | get KEY | del KEY | range START END | batch K1 V1 K2 V2 ...
//	(use "" for an open range bound)
package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"kvstore/engine"
	"kvstore/kvnet"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7000", "server address")
	flag.Parse()
	a := flag.Args()
	fail := func(err error) {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
	if len(a) == 0 {
		fmt.Fprintln(os.Stderr, "usage: kvcli put|get|del|range|batch ...")
		os.Exit(2)
	}
	c, err := kvnet.Dial(*addr, 10*time.Second)
	if err != nil {
		fail(err)
	}
	defer c.Close()
	bound := func(s string) []byte {
		if s == "" {
			return nil
		}
		return []byte(s)
	}
	switch {
	case a[0] == "put" && len(a) == 3:
		err = c.Put([]byte(a[1]), []byte(a[2]))
	case a[0] == "del" && len(a) == 2:
		err = c.Delete([]byte(a[1]))
	case a[0] == "get" && len(a) == 2:
		var v []byte
		if v, err = c.Read([]byte(a[1])); err == nil {
			fmt.Println(string(v))
		}
	case a[0] == "range" && len(a) == 3:
		var kvs []engine.KV
		kvs, err = c.ReadKeyRange(bound(a[1]), bound(a[2]))
		for _, kv := range kvs {
			fmt.Printf("%s\t%s\n", kv.Key, kv.Value)
		}
	case a[0] == "batch" && len(a) >= 3 && len(a)%2 == 1:
		var ks, vs [][]byte
		for i := 1; i < len(a); i += 2 {
			ks, vs = append(ks, []byte(a[i])), append(vs, []byte(a[i+1]))
		}
		err = c.BatchPut(ks, vs)
	default:
		fmt.Fprintln(os.Stderr, "bad arguments")
		os.Exit(2)
	}
	if err != nil {
		fail(err)
	}
}
