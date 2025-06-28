// coordinator package manages the state of the chain. The Coordinator is
// responsible for detecting and handling node failures, electing head and tail
// nodes, and adding new nodes to the chain.
//协调器管理链状态，检测和处理节点故障，向链中添加新节点
//若协调器进程失败，则写操作将无法进行，协调器将写请求转发到头结点
// If the Coordinator process fails but the chain is still intact, reads would
// still be possible. Writes will not be possible because all writes are first
// sent to the Coordinator. The coordinator forwards write requests to the head
// node.

package coordinator

import (
	"errors"
	"log"
	"sync"
	"time"

	"github.com/despreston/go-craq/transport"
)

const (
	pingTimeout  = 5 * time.Second
	pingInterval = 1 * time.Second
)

var ErrEmptyChain = errors.New("no nodes in the chain")

// 协调器负责跟踪链中的节点
type Coordinator struct {
	tport    transport.NodeClientFactory
	mu       sync.Mutex
	replicas []*node

	versionTable          map[string]uint64          // new,键值版本控制表,记录各个key的最新版本号
	committedVersion      map[string]uint64          // key -> 最新已提交版本号
	committedVersionTable map[string]map[uint64]bool // key.version -> 是否提交

	// For testing the AddNode method. This WaitGroup is done when updates have
	// been sent to all nodes.
	Updates *sync.WaitGroup
}

// coordinator的构造函数，初始化一个新的协调器实例及字段，返回一个指针类型的*Coordinator 对象
func New(t transport.NodeClientFactory) *Coordinator {
	return &Coordinator{
		Updates:               &sync.WaitGroup{},
		tport:                 t,
		versionTable:          make(map[string]uint64), //new
		committedVersion:      make(map[string]uint64),
		committedVersionTable: make(map[string]map[uint64]bool),
	}
}

func (cdr *Coordinator) Start() {
	cdr.pingReplicas()
}

// Ping each node. If the response returns an error or the pingTimeout is
// reached, remove the node from the list of replicas.
func (cdr *Coordinator) pingReplicas() {
	log.Println("starting pinging")
	for {
		for _, n := range cdr.replicas {
			go func(n *node) {
				resultCh := make(chan bool, 1)

				go func() {
					err := n.rpc.Ping()
					resultCh <- err == nil
				}()

				select {
				case ok := <-resultCh:
					if !ok {
						cdr.RemoveNode(n.Address())
					}
				case <-time.After(pingTimeout):
					cdr.RemoveNode(n.Address())
				}
			}(n)
		}
		time.Sleep(pingInterval)
	}
}

func findReplicaIndex(address string, replicas []*node) (int, bool) {
	for i, replica := range replicas {
		if replica.Address() == address {
			return i, true
		}
	}
	return 0, false
}

func (cdr *Coordinator) RemoveNode(address string) error {
	cdr.mu.Lock()
	defer cdr.mu.Unlock()
	//找到目标节点在replicas中的索引
	idx, found := findReplicaIndex(address, cdr.replicas)
	if !found {
		return errors.New("unknown node")
	}
	//从replicas列表中删除该节点
	cdr.replicas = append(cdr.replicas[:idx], cdr.replicas[idx+1:]...)
	//wasTail := idx == len(cdr.replicas)-1
	log.Printf("removed node %s", address)

	//如果移除后列表为空，则不用更新邻居，直接返回
	if len(cdr.replicas) == 0 {
		return nil
	}
	//遍历剩余节点，更新每个节点的Next地址，形成闭环链
	replicaCount := len(cdr.replicas)
	for i := 0; i < replicaCount; i++ {
		nextIndex := (i + 1) % replicaCount
		nextAddr := cdr.replicas[nextIndex].Address()
		meta := &transport.NodeMeta{
			Next: nextAddr,
		}
		cdr.Updates.Add(1)
		go func(index int, m *transport.NodeMeta) {
			defer cdr.Updates.Done()
			if err := cdr.replicas[index].rpc.Update(m); err != nil {
				log.Printf("failed to update node %s: %v\n", cdr.replicas[index].Address(), err)
			}
		}(i, meta)
	}

	return nil
}

// AddNode should be called by Nodes to announce themselves to the Coordinator.
// The coordinator then adds them to the end of the chain. The coordinator
// replies with some flags to let the node know if they're head or tail, and
// the address to the previous Node in the chain. The node is responsible for
// announcing itself to the previous Node in the chain.
func (cdr *Coordinator) AddNode(address string) (*transport.NodeMeta, error) {
	log.Printf("received AddNode from %s\n", address)

	// 1) 构造并连接新节点
	n := &node{
		last:    time.Now(),
		address: address,
		rpc:     cdr.tport(),
	}
	if err := n.Connect(); err != nil {
		log.Printf("failed to connect to node %s\n", address)
		return nil, err
	}

	// 2) 加入列表，计算当前总节点数
	cdr.replicas = append(cdr.replicas, n)
	replicaCount := len(cdr.replicas)

	// 3) 只有当节点数 > 1 时，才给每个节点下发 Next，形成闭环
	if replicaCount > 1 {
		for i := 0; i < replicaCount-1; i++ {
			nextIndex := (i + 1) % replicaCount
			nextAddr := cdr.replicas[nextIndex].Address()

			meta := &transport.NodeMeta{Next: nextAddr}
			cdr.Updates.Add(1)
			go func(index int, m *transport.NodeMeta) {
				defer cdr.Updates.Done()
				if err := cdr.replicas[index].rpc.Update(m); err != nil {
					log.Printf("failed to update node %s: %v\n",
						cdr.replicas[index].Address(), err)
				}
			}(i, meta)
		}
		// 多节点时，新节点的 Next 指向第一个节点
		return &transport.NodeMeta{Next: cdr.replicas[0].Address()}, nil
	}

	// 4) 单节点链：不设置 Next，返回空字符串
	return &transport.NodeMeta{Next: ""}, nil
}

func (cdr *Coordinator) Write(key string, value []byte) (*transport.WriteReply, error) {
	// 1) 版本号自增
	cdr.mu.Lock()
	version := cdr.versionTable[key] + 1
	cdr.versionTable[key] = version
	// 2) 初始化该 key 的版本提交表（如果不存在）
	if _, ok := cdr.committedVersionTable[key]; !ok {
		cdr.committedVersionTable[key] = make(map[uint64]bool)
	}
	// 3) 预先标记为未提交
	cdr.committedVersionTable[key][version] = false
	cdr.mu.Unlock()

	if len(cdr.replicas) < 1 {
		return nil, ErrEmptyChain
	}
	//add
	// // 2) 随机选副本
	// idx := rand.Intn(len(cdr.replicas))
	// addr := cdr.replicas[idx].Address()

	// // 3) 不再直接调用节点写，改为返回给 Client
	// return &transport.WriteReply{
	// 	Version:     version,
	// 	ReplicaAddr: addr,
	// }, nil
	//add
	return &transport.WriteReply{Version: version}, nil
}

// add
// ListReplicas 返回所有活跃副本地址列表
func (cdr *Coordinator) ListReplicas() ([]string, error) {
	cdr.mu.Lock()
	defer cdr.mu.Unlock()
	addrs := make([]string, len(cdr.replicas))
	for i, n := range cdr.replicas {
		addrs[i] = n.Address()
	}
	return addrs, nil
}

//add

func (cdr *Coordinator) HandleWriteDone(key string, version uint64) error {
	// 1) 更新已提交版本（要在 RPC 返回前做）
	cdr.mu.Lock()
	// 1) 标记当前版本为已提交
	if _, ok := cdr.committedVersionTable[key]; !ok {
		cdr.committedVersionTable[key] = make(map[uint64]bool)
	}
	cdr.committedVersionTable[key][version] = true

	// 2) 从当前的 committedVersion[key]+1 向上查找，更新连续提交的最大版本
	curr := cdr.committedVersion[key]
	for {
		if committed, ok := cdr.committedVersionTable[key][curr+1]; ok && committed {
			curr++
		} else {
			break
		}
	}
	cdr.committedVersion[key] = curr
	cdr.mu.Unlock()

	log.Printf("key=%s version=%d marked as committed\n", key, version)

	// 2) 启动广播（完全异步），不阻塞 RPC 返回
	go func(key string, version uint64) {
		for _, n := range cdr.replicas {
			if err := n.rpc.Commit(key, version); err != nil {
				log.Printf("failed to commit to node %s: %v\n", n.Address(), err)
			}
		}
	}(key, version)

	// 3) 立即返回给调用者（节点）
	return nil
}

func (cdr *Coordinator) GetCommittedVersion(key string) (uint64, error) {
	cdr.mu.Lock()
	defer cdr.mu.Unlock()

	v, ok := cdr.committedVersion[key]
	if !ok {
		return 0, errors.New("no committed version found")
	}
	return v, nil
}

// 返回key值对应的版本号是否提交
func (cdr *Coordinator) IsCommitted(key string, version uint64) bool {
	cdr.mu.Lock()
	defer cdr.mu.Unlock()

	versions, ok := cdr.committedVersionTable[key]
	if !ok {
		return false
	}
	return versions[version]
}
