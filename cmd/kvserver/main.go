// kvserver serves one engine over TCP.
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"kvstore/engine"
	"kvstore/kvnet"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7000", "listen address")
	dir := flag.String("dir", "data", "data directory")
	noSync := flag.Bool("nosync", false, "disable fsync (faster, not crash-safe)")
	flag.Parse()

	db, err := engine.Open(*dir, &engine.Options{NoSync: *noSync})
	if err != nil {
		log.Fatal(err)
	}
	srv, err := kvnet.Listen(*addr, db)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("serving %s on %s", *dir, srv.Addr())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	srv.Close()
	db.Close()
}
