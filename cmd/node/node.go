package main

import (
	"flag"
	"log"
	"net"
	"net/http"
	"net/rpc"

	"github.com/despreston/go-craq/node"
	"github.com/despreston/go-craq/store/boltdb"
	"github.com/despreston/go-craq/transport/netrpc"
)

func main() {
	var addr, pub, cdr, dbFile string

	flag.StringVar(&addr, "a", ":1235", "Local address to listen on")
	flag.StringVar(&pub, "p", ":1235", "Public address reachable by coordinator and other nodes")
	flag.StringVar(&cdr, "c", ":1234", "Coordinator address")
	flag.StringVar(&dbFile, "f", "craq.db", "Bolt DB database file")
	flag.Parse()
	//打开本地存储
	db := boltdb.New(dbFile, "yessir")
	if err := db.Connect(); err != nil {
		log.Fatal(err)
	}

	defer db.DB.Close()

	//构造Node实例
	n := node.New(node.Opts{
		Address:           addr,
		CdrAddress:        cdr,
		PubAddress:        pub,
		Store:             db,
		Transport:         netrpc.NewNodeClient,
		CoordinatorClient: netrpc.NewCoordinatorClient(),
		Log:               log.Default(),
	})

	// 注册 RPC
	rpc.RegisterName("RPC", &netrpc.NodeBinding{Svc: n})
	rpc.HandleHTTP()

	// 1) 先同步打开监听器，确保下面的 Serve 一定能立即接受连接
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}

	// 2) 启动 HTTP+RPC Server 到后台
	go func() {
		log.Printf("Listening at %s\n", addr)
		if err := http.Serve(ln, nil); err != nil {
			log.Fatalf("http.Serve error: %v", err)
		}
	}()

	// 3) 然后开始连 Coordinator & 报名
	if err := n.Start(); err != nil {
		log.Fatalf("Failed to connect to the chain.\nError: %v", err)
	}

	// 4) 阻塞，让主 goroutine 不退出
	select {}
}
