// crashtest: "writer" appends numbered keys and prints each acknowledged
// number; the test harness kill -9s it, then "verify" checks that every
// acknowledged key survived.
package main

import (
	"fmt"
	"os"
	"strconv"

	"kvstore/engine"
)

func main() {
	dir := os.Args[2]
	db, err := engine.Open(dir, &engine.Options{MemtableSize: 64 << 10}) // fsync on
	if err != nil {
		panic(err)
	}
	switch os.Args[1] {
	case "writer":
		for i := 0; ; i++ {
			k := []byte(fmt.Sprintf("key-%09d", i))
			if err := db.Put(k, k); err != nil {
				panic(err)
			}
			fmt.Println(i) // acknowledged
		}
	case "verify":
		last, _ := strconv.Atoi(os.Args[3])
		for i := 0; i <= last; i++ {
			k := []byte(fmt.Sprintf("key-%09d", i))
			if v, err := db.Read(k); err != nil || string(v) != string(k) {
				fmt.Printf("LOST key %d: %v\n", i, err)
				os.Exit(1)
			}
		}
		fmt.Printf("OK: all %d acknowledged keys present\n", last+1)
	}
}
