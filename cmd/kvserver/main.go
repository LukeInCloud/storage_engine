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
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"kvstore/engine"
	"kvstore/kvnet"
	"kvstore/raft"
	"kvstore/replica"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:7000", "client listen address (standalone mode)")
	dir := flag.String("dir", "data", "data directory")
	noSync := flag.Bool("nosync", false, "disable engine fsync (faster, not crash-safe)")
	id := flag.String("id", "", "this node's ID (cluster mode)")
	peers := flag.String("peers", "", "cluster members: id=raftAddr/clientAddr,... (enables cluster mode)")
	statsEvery := flag.Duration("stats", 0, "log engine statistics at this interval (standalone mode), e.g. 5s")
	pprofAddr := flag.String("pprof", "", "serve net/http/pprof on this address (e.g. 127.0.0.1:6060)")
	flag.Parse()
	if *pprofAddr != "" {
		go func() { log.Println(http.ListenAndServe(*pprofAddr, nil)) }()
	}

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
		if *statsEvery > 0 {
			go func() {
				for range time.Tick(*statsEvery) {
					log.Printf("stats: %v", db.Stats())
				}
			}()
		}
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
		Tune:   func(rc *raft.Config) { rc.NoSync = *noSync }, // -nosync also skips the Raft log fsync
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
