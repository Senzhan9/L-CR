package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
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
	maxKeyIndex  int
	replicaCache string
	cacheTime    time.Time
	cacheTTL     = 30 * time.Second
)

func init() {
	flag.StringVar(&coordAddr, "c", ":1234", "Coordinator address")
	flag.IntVar(&total, "n", 1000, "Total reads")
	flag.IntVar(&concurrency, "cc", 50, "Concurrency")
	flag.IntVar(&maxKeyIndex, "max", 999, "Max key index for reads")
}

func main() {
	flag.Parse()
	if total <= 0 || concurrency <= 0 || maxKeyIndex < 0 {
		fmt.Println("Invalid flags")
		os.Exit(1)
	}

	// 预热：缓存一次“最近副本”
	refreshReplicaCache()

	var wg sync.WaitGroup
	sem := make(chan struct{}, concurrency)

	// 记录开始时间
	start := time.Now()

	// 并发发起 total 次读请求
	for i := 0; i < total; i++ {
		wg.Add(1)
		sem <- struct{}{}

		go func(i int) {
			defer wg.Done()
			defer func() { <-sem }()

			// 随机选择一个 key，比如 key0000..key0999
			idx := rand.Intn(maxKeyIndex + 1)
			key := fmt.Sprintf("key%04d", idx)

			// 从缓存的最近副本读
			target := getReplica()
			if _, _, err := readFromReplica(target, key); err != nil {
				log.Printf("[#%d] read error: %v\n", i, err)
			}
		}(i)
	}

	wg.Wait()

	// 统计总耗时、平均延迟、吞吐量
	elapsed := time.Since(start)
	totalMs := float64(elapsed.Milliseconds())
	avgLatencyMs := totalMs / float64(total)
	throughput := float64(total) / elapsed.Seconds()

	fmt.Printf("\n========== bench_read2 结果 ==========\n")
	fmt.Printf("总读取次数: %d\n", total)
	fmt.Printf("并发度(concurrency): %d\n", concurrency)
	fmt.Printf("总耗时      : %v\n", elapsed)
	fmt.Printf("平均延迟    : %.3f ms\n", avgLatencyMs)
	fmt.Printf("吞吐量      : %.2f ops/s\n", throughput)
	fmt.Printf("====================================\n")
}

// refreshReplicaCache 向 Coordinator 请求所有副本列表并测 RTT，缓存最近副本
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

	// 并行测 RTT
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
	log.Printf("bench_read2: cached nearest replica %s (rtt=%v)\n", best, bestRTT)
}

// getReplica 返回缓存副本地址，若过期则重新刷新
func getReplica() string {
	if replicaCache == "" || time.Since(cacheTime) > cacheTTL {
		refreshReplicaCache()
	}
	return replicaCache
}

// readFromReplica 向指定副本发 Read 请求
func readFromReplica(addr, key string) (string, []byte, error) {
	n := netrpc.NewNodeClient()
	if err := n.Connect(addr); err != nil {
		return "", nil, err
	}
	defer n.Close()
	return n.Read(key)
}
