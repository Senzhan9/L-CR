package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"time"

	"github.com/despreston/go-craq/transport/netrpc"
)

var (
	coordAddr    string
	total        int
	concurrency  int
	replicaCache string
	cacheTime    time.Time
	cacheTTL     = 30 * time.Second
)

func init() {
	flag.StringVar(&coordAddr, "c", ":1234", "Coordinator address")
	flag.IntVar(&total, "n", 1000, "Total writes")
	flag.IntVar(&concurrency, "cc", 50, "Concurrency")
}

func main() {
	flag.Parse()
	if total <= 0 || concurrency <= 0 {
		fmt.Println("Invalid -n or -cc")
		os.Exit(1)
	}

	// 预热：缓存一次“最近副本”
	refreshReplicaCache()

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)

	// 记录开始时间
	start := time.Now()

	// 并发发起 total 次写请求
	for i := 0; i < total; i++ {
		wg.Add(1)
		sem <- struct{}{}

		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()

			key := fmt.Sprintf("key%04d", i)
			val := []byte(fmt.Sprintf("value-for-%04d", i))

			// 1) 从 Coordinator 拿版本号（不分配副本）
			version := requestVersion(key, val)

			// 2) 写入到缓存的“最近副本”
			target := getReplica()
			if err := writeToReplica(target, key, val, version); err != nil {
				log.Printf("[#%d] write error: %v\n", i, err)
			}
		}(i)
	}

	wg.Wait()

	// 统计总耗时、平均延迟、吞吐量
	elapsed := time.Since(start)
	totalMs := float64(elapsed.Milliseconds())
	avgLatencyMs := totalMs / float64(total)
	throughput := float64(total) / elapsed.Seconds()

	fmt.Printf("\n========== bench_write2 结果 ==========\n")
	fmt.Printf("总写入次数: %d\n", total)
	fmt.Printf("并发度(concurrency): %d\n", concurrency)
	fmt.Printf("总耗时      : %v\n", elapsed)
	fmt.Printf("平均延迟    : %.3f ms\n", avgLatencyMs)
	fmt.Printf("吞吐量      : %.2f ops/sec\n", throughput)
	fmt.Printf("====================================\n")
}

// requestVersion 从 Coordinator 拿版本号
func requestVersion(key string, val []byte) uint64 {
	c := netrpc.NewCoordinatorClient()
	if err := c.Connect(coordAddr); err != nil {
		log.Fatalf("connect coord failed: %v", err)
	}
	wr, err := c.Write(key, val)
	c.Close()
	if err != nil {
		log.Fatalf("Coordinator.Write failed: %v", err)
	}
	return wr.Version
}

// refreshReplicaCache 获取并缓存“最近副本”
func refreshReplicaCache() {
	c := netrpc.NewCoordinatorClient()
	if err := c.Connect(coordAddr); err != nil {
		log.Fatalf("connect coord failed: %v", err)
	}
	addrs, err := c.ListReplicas()
	c.Close()
	if err != nil {
		log.Fatalf("ListReplicas failed: %v", err)
	}
	if len(addrs) == 0 {
		log.Fatal("no replicas")
	}

	type res struct {
		addr string
		rtt  time.Duration
	}
	ch := make(chan res, len(addrs))

	// 并行测 RTT，挑最小
	for _, a := range addrs {
		go func(addr string) {
			st := time.Now()
			conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
			if err != nil {
				ch <- res{addr, time.Hour}
				return
			}
			conn.Close()
			ch <- res{addr, time.Since(st)}
		}(a)
	}

	best, bestRTT := "", time.Hour
	for i := 0; i < len(addrs); i++ {
		r := <-ch
		if r.rtt < bestRTT {
			bestRTT, best = r.rtt, r.addr
		}
	}

	replicaCache, cacheTime = best, time.Now()
	log.Printf("bench_write2: cached nearest replica %s (rtt=%v)\n", best, bestRTT)
}

// getReplica 返回缓存的副本地址，过期则刷新
func getReplica() string {
	if replicaCache == "" || time.Since(cacheTime) > cacheTTL {
		refreshReplicaCache()
	}
	return replicaCache
}

// writeToReplica 发写请求到指定副本
func writeToReplica(addr, key string, val []byte, version uint64) error {
	n := netrpc.NewNodeClient()
	if err := n.Connect(addr); err != nil {
		return err
	}
	defer n.Close()
	return n.ClientWrite(key, val, version)
}
