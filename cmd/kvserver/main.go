// kvserver serves the storage engine over TCP, either standalone or as a
// member of a replicated (Raft) cluster.
//
//	standalone: kvserver -addr 127.0.0.1:7000 -dir data
//	cluster:    kvserver -id n1 -dir data1 \
//	              -peers n1=127.0.0.1:9001/127.0.0.1:7001,n2=127.0.0.1:9002/127.0.0.1:7002,n3=127.0.0.1:9003/127.0.0.1:7003
//	            (each peer is id=raftAddr/clientAddr; run one process per peer)
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"kvstore/engine"
	"kvstore/kvnet"
	"kvstore/replica"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7000", "client listen address (standalone mode)")
	dir := flag.String("dir", "data", "data directory")
	noSync := flag.Bool("nosync", false, "disable engine fsync (faster, not crash-safe)")
	id := flag.String("id", "", "this node's ID (cluster mode)")
	peers := flag.String("peers", "", "cluster members: id=raftAddr/clientAddr,... (enables cluster mode)")
	flag.Parse()

	if *peers == "" {
		db, err := engine.Open(*dir, &engine.Options{NoSync: *noSync})
		if err != nil {
			log.Fatal(err)
		}
		srv, err := kvnet.Listen(*addr, db)
		if err != nil {
			log.Fatal(err)
		}
		log.Printf("standalone: serving %s on %s", *dir, srv.Addr())
		waitForSignal()
		srv.Close()
		db.Close()
		return
	}

	ps, err := replica.ParsePeers(*peers)
	if err != nil {
		log.Fatal(err)
	}
	s, err := replica.StartServer(replica.NodeConfig{
		ID: *id, Dir: *dir, Peers: ps,
		Engine: engine.Options{NoSync: *noSync},
		Logf:   log.Printf,
	})
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("cluster node %s: clients on %s", *id, s.ClientAddr())
	waitForSignal()
	s.Close()
}

func waitForSignal() {
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
}
