package main

import (
	"flag" //flag包解析地址参数 -a ，默认是:1234，作为其监听地址
	"log"
	"net/http"
	"net/rpc"

	"github.com/despreston/go-craq/coordinator"
	"github.com/despreston/go-craq/transport/netrpc"
)

func main() {
	addr := *flag.String("a", ":1234", "Local address to listen on")
	flag.Parse()

	c := coordinator.New(netrpc.NewNodeClient) //创建coordinator实例

	binding := netrpc.CoordinatorBinding{Svc: c} //注册RPC服务，外部可通过RPC调用coordinator的接口
	if err := rpc.RegisterName("RPC", &binding); err != nil {
		log.Fatal(err)
	}
	rpc.HandleHTTP()

	// Start the Coordinator
	go c.Start()

	// Start the rpc server
	log.Println("Listening at " + addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}
