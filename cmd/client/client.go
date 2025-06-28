package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"strings"
	"time"

	"github.com/despreston/go-craq/transport/netrpc"
)

var (
	coordAddr    string    // -c 参数
	replicaCache string    // 缓存的最近副本地址
	cacheTime    time.Time // 缓存时间
	cacheTTL     = 30 * time.Second
)

func init() {
	flag.StringVar(&coordAddr, "c", ":1234", "coordinator address")
}

func main() {

	flag.Parse()
	args := flag.Args()
	if len(args) < 1 {
		log.Fatal("No command given.")
	}
	cmd := args[0]

	// 启动时预热一次缓存
	refreshReplicaCache()

	switch cmd {
	case "write":
		if len(args) < 3 {
			log.Fatal("Usage: write <key> <value>")
		}
		key := args[1]
		val := strings.Join(args[2:], " ")

		// 1) 先从 Coordinator 拿版本号
		version := requestVersion(key, []byte(val))

		// 2) 写到缓存的最近副本
		target := getReplica()
		writeToReplica(target, key, []byte(val), version)
		fmt.Printf("Wrote key=%s version=%d to replica %s\n", key, version, target)

	case "read":
		if len(args) < 2 {
			log.Fatal("Usage: read <key>")
		}
		key := args[1]
		target := getReplica()
		k, v := readFromReplica(target, key)
		fmt.Printf("Read from %s → key: %s, value: %s\n", target, k, string(v))

	case "readall":
		target := getReplica()
		n := netrpc.NewNodeClient()
		if err := n.Connect(target); err != nil {
			log.Fatalf("Failed to connect to node %s: %v", target, err)
		}
		items, err := n.ReadAll()
		n.Close()
		if err != nil {
			log.Fatalf("ReadAll failed: %v", err)
		}
		for _, item := range *items {
			fmt.Printf("key: %s, value: %s\n", item.Key, string(item.Value))
		}

	default:
		log.Fatalf("Unknown command: %s", cmd)

	}
}

// requestVersion 从 Coordinator 拿版本号
func requestVersion(key string, val []byte) uint64 {
	c := netrpc.NewCoordinatorClient()
	if err := c.Connect(coordAddr); err != nil {
		log.Fatalf("Failed to connect to coordinator %s: %v", coordAddr, err)
	}
	wr, err := c.Write(key, val)
	c.Close()
	if err != nil {
		log.Fatalf("Coordinator.Write failed: %v", err)
	}
	return wr.Version
}

// refreshReplicaCache 向 Coordinator 请求所有副本列表并选出 RTT 最低的缓存
func refreshReplicaCache() {
	c := netrpc.NewCoordinatorClient()
	if err := c.Connect(coordAddr); err != nil {
		log.Fatalf("connect to coordinator %s failed: %v", coordAddr, err)
	}
	addrs, err := c.ListReplicas()
	c.Close()
	if err != nil {
		log.Fatalf("Coordinator.ListReplicas failed: %v", err)
	}
	if len(addrs) == 0 {
		log.Fatal("no replicas available")
	}
	// 并行测 RTT
	type r struct {
		addr string
		rtt  time.Duration
	}
	ch := make(chan r, len(addrs))
	for _, a := range addrs {
		go func(addr string) {
			start := time.Now()
			conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
			if err != nil {
				ch <- r{addr, time.Hour}
				return
			}
			conn.Close()
			ch <- r{addr, time.Since(start)}
		}(a)
	}
	bestRTT := time.Hour
	bestAddr := ""
	for i := 0; i < len(addrs); i++ {
		res := <-ch
		if res.rtt < bestRTT {
			bestRTT, bestAddr = res.rtt, res.addr
		}
	}
	replicaCache = bestAddr
	cacheTime = time.Now()
	log.Printf("Cached nearest replica: %s (rtt=%v)", replicaCache, bestRTT)
}

// getReplica 返回缓存的副本地址，过期则刷新
func getReplica() string {
	if replicaCache == "" || time.Since(cacheTime) > cacheTTL {
		refreshReplicaCache()
	}
	return replicaCache
}

// writeToReplica 向指定副本发写请求
func writeToReplica(addr, key string, val []byte, version uint64) {
	n := netrpc.NewNodeClient()
	if err := n.Connect(addr); err != nil {
		log.Fatalf("Failed to connect to replica %s: %v", addr, err)
	}
	defer n.Close()
	if err := n.ClientWrite(key, val, version); err != nil {
		log.Fatalf("Replica.ClientWrite failed: %v", err)
	}
}

// readFromReplica 从指定副本读
func readFromReplica(addr, key string) (string, []byte) {
	n := netrpc.NewNodeClient()
	if err := n.Connect(addr); err != nil {
		log.Fatalf("Failed to connect to replica %s: %v", addr, err)
	}
	defer n.Close()
	k, v, err := n.Read(key)
	if err != nil {
		log.Fatalf("Replica.Read failed: %v", err)
	}
	return k, v
}
