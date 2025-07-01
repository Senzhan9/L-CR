// node package corresponds to what the CRAQ white paper refers to as a node.

package node

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/despreston/go-craq/store"
	"github.com/despreston/go-craq/transport"
)

// neighbor is another node in the chain
type neighbor struct {
	rpc     transport.NodeClient
	address string
}

// Opts is for passing options to the Node constructor.
type Opts struct {
	// Storage layer to use
	Store store.Storer
	// Local listening address
	Address string
	// Address to advertise to other nodes and coordinator
	PubAddress string
	// Address of coordinator
	CdrAddress string
	// Transport creates new clients for communication with other nodes
	Transport transport.NodeClientFactory
	// For communication with the Coordinator
	CoordinatorClient transport.CoordinatorClient
	// Log
	Log *log.Logger
}

type commitEvent struct {
	Key     string
	Version uint64
}

// Node is what the white paper refers to as a node. This is the client that is
// responsible for storing data and handling reads/writes.
type Node struct {
	// Other nodes in the chain
	neighbors map[transport.NeighborPos]neighbor
	// Storage layer
	store store.Storer
	// Latest version of a given key
	latest map[string]uint64
	// For listening to commit's. For testing.
	committed                    chan commitEvent
	cdrAddress, address, pubAddr string
	cdr                          transport.CoordinatorClient
	mu                           sync.Mutex
	transport                    func() transport.NodeClient
	log                          *log.Logger
}

// New creates a new Node.
func New(opts Opts) *Node {
	logger := opts.Log
	if opts.Log == nil {
		logger = log.Default()
	}
	return &Node{
		latest:     make(map[string]uint64),
		neighbors:  make(map[transport.NeighborPos]neighbor, 3),
		cdrAddress: opts.CdrAddress,
		address:    opts.Address,
		store:      opts.Store,
		transport:  opts.Transport,
		pubAddr:    opts.PubAddress,
		cdr:        opts.CoordinatorClient,
		log:        logger,
	}
}

// ListenAndServe starts listening for messages and connects to the coordinator.
func (n *Node) Start() error {
	if err := n.backfillLatest(); err != nil {
		log.Fatalf("Failed to backfill latest versions.\n Error: %#v", err)
	}
	if err := n.connectToCoordinator(); err != nil {
		log.Fatalf("Failed to connect to the chain.\n Error: %#v", err)
	}
	return nil
}

// backfillLatest queries the store for the latest committed version of
// everything it has in order to fill n.latest.
func (n *Node) backfillLatest() error {
	c, err := n.store.AllCommitted()
	if err != nil {
		return err
	}
	for _, item := range c {
		n.latest[item.Key] = item.Version
	}
	return nil
}

// ConnectToCoordinator let's the Node announce itself to the chain coordinator
// to be added to the chain. The coordinator responds with a message to tell the
// Node if it's the head or tail, and with the address of the previous node in the
// chain and the address to the tail node. The Node announces itself to the
// neighbor using the address given by the coordinator.
func (n *Node) connectToCoordinator() error {
	//连接coordinator
	err := n.cdr.Connect(n.cdrAddress)
	if err != nil {
		n.log.Println("Error connecting to the coordinator")
		return err
	}

	n.log.Printf("Connected to coordinator at %s\n", n.cdrAddress)

	// Announce self to the Coordinator
	reply, err := n.cdr.AddNode(n.pubAddr)
	if err != nil {
		n.log.Println(err.Error())
		return err
	}

	if err := n.Update(reply); err != nil {
		n.log.Printf("Initial Update failed: %v\n", err)
		return err
	}

	if reply.Next != "" {
		if err := n.connectToNode(reply.Next, transport.NeighborPosNext); err != nil {
			n.log.Printf("Failed to connect to next node in connectToCoordinator: %v\n", err)
			return err
		}
		// 调用 fullPropagate（拉取脏数据 + 已提交数据）
		if err := n.fullPropagate(); err != nil {
			n.log.Printf("fullPropagate error: %v\n", err)
			return err
		}
	}

	return nil
}

// send FwdPropagate and BackPropagate requests to new predecessor to get fully
// caught up. Forward propagation should go first so that it has all the dirty
// items needed before receiving backwards propagation response.
// 从后继结点拉取脏数据和已提交数据，同步到本地
func (n *Node) fullPropagate() error {
	nextNeighbor := n.neighbors[transport.NeighborPosNext].rpc

	// 1) 拉取并写入所有脏数据
	if err := n.requestFwdPropagation(nextNeighbor); err != nil {
		n.log.Printf("fullPropagate: requestFwdPropagation error: %v\n", err)
		return err
	}

	// 2) 拉取并提交所有已提交数据
	if err := n.requestBackPropagation(nextNeighbor); err != nil {
		n.log.Printf("fullPropagate: requestBackPropagation error: %v\n", err)
		return err
	}

	return nil
}

func (n *Node) connectToNode(address string, pos transport.NeighborPos) error {
	newNbr := n.transport()
	if err := newNbr.Connect(address); err != nil {
		return err
	}

	n.log.Printf("connected to %s\n", address)

	// Disconnect from current neighbor if there's one connected.
	nbr := n.neighbors[pos]
	if nbr.rpc != nil {
		nbr.rpc.Close()
	}

	n.neighbors[pos] = neighbor{
		rpc:     newNbr,
		address: address,
	}

	return nil
}

func (n *Node) writePropagated(reply *transport.PropagateResponse) error {
	// Save items from reply to store.
	for key, forKey := range *reply {
		for _, item := range forKey {
			if err := n.store.Write(key, item.Value, item.Version, item.ReplicaPos); err != nil {
				n.log.Printf("Failed to write item %+v to store: %#v\n", item, err)
				return err
			}
			n.log.Printf("wrote %s", key)
		}
	}
	return nil
}

func (n *Node) commit(key string, version uint64) error {
	const maxRetries = 15
	const retryInterval = 1000 * time.Millisecond

	var lastErr error

	for i := 0; i < maxRetries; i++ {
		err := n.store.Commit(key, version)
		if err == nil {
			n.latest[key] = version
			if n.committed != nil {
				n.committed <- commitEvent{Key: key, Version: version}
			}
			return nil
		}

		if errors.Is(err, store.ErrNotFound) {
			lastErr = err
			time.Sleep(retryInterval)
			continue
		}

		// 其他错误不重试
		return err
	}

	return fmt.Errorf("node commit retry failed for key %s version %d: %v", key, version, lastErr)
}

// Commit the version to the store, update n.latest for this key, and announce
// the commit to the n.committed channel if there is one.
/*
func (n *Node) commit(key string, version uint64) error {
	if err := n.store.Commit(key, version); err != nil {
		//n.log.Printf("Failed to commit. Key: %s Version: %d Error: %#v", key, version, err)
		return err
	}

	n.latest[key] = version

	//n.log.Printf("Node %s: committed key=%s version=%d", n.address, key, version)

	if n.committed != nil {
		n.committed <- commitEvent{Key: key, Version: version}
	}

	return nil
}*/

func (n *Node) commitPropagated(reply *transport.PropagateResponse) error {
	// Commit items from reply to store.
	for key, forKey := range *reply {
		for _, item := range forKey {
			// It's possible the item doesn't exist. In that case, add it first.
			// This sort of a poor man's upsert, but it saves from having to
			// deal w/ it in the storage layer, which should make it easier to
			// write new storers.
			if err := n.commit(key, item.Version); err != nil {
				if err == store.ErrNotFound {
					if err := n.store.Write(key, item.Value, item.Version, item.ReplicaPos); err != nil {
						return err
					}
					if err := n.commit(key, item.Version); err != nil {
						return err
					}
				} else {
					return err
				}
			}
		}
	}
	return nil
}

// Create a map of the highest versions for each key.
func propagateRequestFromItems(items []*store.Item) *transport.PropagateRequest {
	req := transport.PropagateRequest{}
	for _, item := range items {
		if req[item.Key] < item.Version {
			req[item.Key] = item.Version
		}
	}
	return &req
}

// requestFwdPropagation asks client to respond with all uncommitted (dirty)
// items that this node either does not have or are newer than what this node
// has.
func (n *Node) requestFwdPropagation(client transport.NodeClient) error {
	dirty, err := n.store.AllDirty()
	if err != nil {
		n.log.Printf("Failed to get all dirty items: %#v\n", err)
		return err
	}

	reply, err := client.FwdPropagate(propagateRequestFromItems(dirty))
	if err != nil {
		n.log.Printf("Failed during forward propagation: %#v\n", err)
		return err
	}

	return n.writePropagated(reply)
}

// requestBackPropagation asks client to respond with all committed items that
// this node either does not have or are newer than what this node has.
func (n *Node) requestBackPropagation(client transport.NodeClient) error {
	committed, err := n.store.AllCommitted()
	if err != nil {
		n.log.Printf("Failed to get all committed items: %#v\n", err)
		return err
	}

	reply, err := client.BackPropagate(propagateRequestFromItems(committed))
	if err != nil {
		n.log.Printf("Failed during back propagation: %#v\n", err)
		return err
	}

	return n.commitPropagated(reply)
}

// resetNeighbor closes any open connection and resets the neighbor.
func (n *Node) resetNeighbor(pos transport.NeighborPos) {
	n.neighbors[pos].rpc.Close()
	n.neighbors[pos] = neighbor{}
}

// Ping responds to ping messages.
func (n *Node) Ping() error {
	return nil
}

func (n *Node) connectToSuccessor(address string) error {
	next := n.neighbors[transport.NeighborPosNext]

	if next.address == address {
		n.log.Println("New successor same address as last one, keeping conn.")
		return nil
	} else if address == "" {
		n.log.Println("Resetting successor")
		n.resetNeighbor(transport.NeighborPosNext)
		return nil
	}

	n.log.Printf("connecting to new successor %s\n", address)
	if err := n.connectToNode(address, transport.NeighborPosNext); err != nil {
		return err
	}

	nextC := n.neighbors[transport.NeighborPosNext].rpc
	return n.requestBackPropagation(nextC)
}

// Update is for updating a node's metadata. If new neighbors are given, the
// Node will disconnect from the current neighbors before connecting to the new
// ones. Coordinator uses this method to update metadata of the node when there
// is a failure or re-organization of the chain.
func (n *Node) Update(meta *transport.NodeMeta) error {
	n.log.Printf("Received metadata update: %+v\n", meta)
	n.mu.Lock()
	defer n.mu.Unlock()

	if err := n.connectToSuccessor(meta.Next); err != nil {
		return err
	}

	return nil
}

// new 直接采用传入的version,Coordinator发起的写请求入口，是写请求的起点
// ClientWrite handles a write request with a specified version number.
// It writes the data to the local store and forwards it to the successor node.
func (n *Node) ClientWrite(key string, val []byte, version uint64) error {
	// 直接使用传入的 version 写入
	// 调用store.write，传入当前节点的地址
	if err := n.store.Write(key, val, version, n.pubAddr); err != nil {
		n.log.Printf("Failed to write during ClientWrite. %v\n", err)
		return err
	}

	n.log.Printf("Node RPC ClientWrite() stored version %d of key %s at node %s\n", version, key, n.pubAddr)

	// 转发给下一个副本（successor）
	next := n.neighbors[transport.NeighborPosNext]

	// 如果没有 successor，说明是尾节点，直接 commit。
	if next.address == "" {
		n.log.Println("No successor (tail node)")
		if err := n.commit(key, version); err != nil {
			return err
		}
		return nil
	}

	// 向下一个副本继续传播写请求
	if err := next.rpc.Write(key, val, version, n.pubAddr); err != nil {
		n.log.Printf("Failed to send to successor during ClientWrite. %v\n", err)
		return err
	}

	return nil
}

func (n *Node) Write(key string, val []byte, version uint64, replicaPos string) error {
	//n.log.Printf("[DEBUG] Write entry @%s: key=%s ver=%d replicaPos=%s next=%s\n",
	//n.address, key, version, replicaPos, n.neighbors[transport.NeighborPosNext].address)
	n.log.Printf("Node RPC Write() received %s version %d with replicaPos %s\n", key, version, replicaPos)
	//写入本地store
	if err := n.store.Write(key, val, version, replicaPos); err != nil {
		n.log.Printf("Failed to write. %v\n", err)
		return err
	}

	next := n.neighbors[transport.NeighborPosNext]
	//n.log.Printf("replicaPos=%s next.address=%s\n", replicaPos, next.address)
	// 如果下一个副本回环到起始副本，通知 Coordinator 提交
	if replicaPos == next.address {
		n.log.Printf("Write 回环完毕到%s，通知 Coordinator：key=%s version=%d\n", n.address, key, version)
		if err := n.cdr.Connect(n.cdrAddress); err != nil {
			n.log.Printf("连接 Coordinator 失败：%v\n", err)
		} else {
			if err := n.cdr.HandleWriteDone(key, version); err != nil {
				n.log.Printf("通知 Coordinator 失败：%v\n", err)
			}
		}
		return nil
	}

	if next.address == "" {
		n.log.Printf("No successor available at node %s for key %s\n", n.address, key)
		return errors.New("no successor available")
	}

	if err := next.rpc.Write(key, val, version, replicaPos); err != nil {
		n.log.Printf("Failed to send to successor during Write at node %s: %v\n", n.address, err)
		return err
	}
	return nil
}

// commitAndSend commits an item to the store and sends a message to the
// predecessor node to tell it to commit as well.
// func (n *Node) commitAndSend(key string, version uint64) error {
// 	//检查是否已经提交，避免重复传播
// 	item, err := n.store.ReadVersion(key, version)
// 	if err != nil {
// 		if err == store.ErrNotFound {
// 			n.log.Printf("Commit: key %s version %d not found\n", key, version)
// 		} else {
// 			n.log.Printf("Commit: failed to read key %s version %d: %v\n", key, version, err)
// 		}
// 		return err
// 	}

// 	if item.Committed {
// 		n.log.Printf("Commit: key %s version %d is already committed at %s, stopping propagation.\n", key, version, n.address)
// 		return nil // 不继续向前传播
// 	}
// 	// 还未提交，执行本地提交
// 	if err := n.commit(key, version); err != nil {
// 		return err
// 	}

// 	// 向后一个节点发送 Commit 请求
// 	next := n.neighbors[transport.NeighborPosNext]
// 	if next.address != "" {
// 		n.log.Printf("Propagating commit to next node %s for key %s version %d\n", next.address, key, version)
// 		return next.rpc.Commit(key, version)
// 	}

// 	return nil
// }

// Commit marks an object as committed in storage.
func (n *Node) Commit(key string, version uint64) error {
	//return n.commitAndSend(key, version)
	return n.commit(key, version)
}

func (n *Node) Read(key string) (string, []byte, error) {
	// 1) 本地读最新数据
	item, err := n.store.Read(key)
	if err == store.ErrNotFound {
		// 从未写入过
		return "", nil, errors.New("key doesn't exist")
	}
	if err == nil {
		// 最新版本已提交，直接返回
		return key, item.Value, nil
	}
	if err != store.ErrDirtyItem {
		// 其它错误
		return "", nil, err
	}

	// 2) 本地最新版本是 dirty，向 Coordinator 请求已提交版本号
	//    确保与 Coordinator 已连接
	if err := n.cdr.Connect(n.cdrAddress); err != nil {
		n.log.Printf("Read: 连接 Coordinator 失败：%v\n", err)
	}
	v, err := n.cdr.GetCommittedVersion(key)
	if err != nil {
		n.log.Printf("Read: 获取已提交版本失败：%v\n", err)
		return "", nil, err
	}

	// 3) 用该已提交版本号 v 去本地 store 读取并返回
	stored, err := n.store.ReadVersion(key, v)
	if err != nil {
		return "", nil, err
	}
	return key, stored.Value, nil
}

// ReadAll returns all committed key/value pairs in the store.
func (n *Node) ReadAll() (*[]transport.Item, error) {
	fullItems, err := n.store.AllCommitted()
	if err != nil {
		return nil, err
	}

	items := []transport.Item{}
	for _, itm := range fullItems {
		items = append(items, transport.Item{
			Key:   itm.Key,
			Value: itm.Value,
		})
	}

	return &items, nil
}

// LatestVersion provides the latest committed version for a given key in the
// store.
func (n *Node) LatestVersion(key string) (string, uint64, error) {
	return key, n.latest[key], nil
}

// BackPropagate let's another node ask this node to send it all the committed
// items it has in it's storage. The node requesting back propagation should
// send the key + latest version of all committed items it has. This node
// responds with all committed items that: have a newer version, weren't
// included in the request.
func (n *Node) BackPropagate(
	verByKey *transport.PropagateRequest,
) (*transport.PropagateResponse, error) {
	unseen, err := n.store.AllNewerCommitted(map[string]uint64(*verByKey))
	if err != nil {
		return nil, err
	}
	return makePropagateResponse(unseen), nil
}

// FwdPropagate let's another node ask this node to send it all the dirty items
// it has in it's storage. The node requesting forward propagation should send
// the key + latest version of all uncommitted items it has. This node responds
// with all uncommitted items that: have a newer version, weren't included in
// the request.
func (n *Node) FwdPropagate(
	verByKey *transport.PropagateRequest,
) (*transport.PropagateResponse, error) {
	unseen, err := n.store.AllNewerDirty(map[string]uint64(*verByKey))
	if err != nil {
		return nil, err
	}
	return makePropagateResponse(unseen), nil
}

func makePropagateResponse(items []*store.Item) *transport.PropagateResponse {
	response := transport.PropagateResponse{}

	for _, item := range items {
		response[item.Key] = append(response[item.Key], transport.ValueVersion{
			Value:      item.Value,
			Version:    item.Version,
			ReplicaPos: item.ReplicaPos,
		})
	}

	return &response
}
