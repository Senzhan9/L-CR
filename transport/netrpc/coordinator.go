package netrpc

import (
	"github.com/despreston/go-craq/transport"
)

func NewCoordinatorClient() transport.CoordinatorClient {
	return &CoordinatorClient{Client: &Client{}}
}

// CoordinatorBinding provides a layer of translation between the
// CoordinatorService which is transport agnostic and the net/rpc package. This
// allows using the net/rpc package to invoke CoordinatorService methods.
type CoordinatorBinding struct {
	Svc transport.CoordinatorService
}

func (c *CoordinatorBinding) AddNode(addr *string, r *transport.NodeMeta) error {
	meta, err := c.Svc.AddNode(*addr)
	if err != nil {
		return err
	}
	*r = *meta
	return nil
}

func (c *CoordinatorBinding) RemoveNode(addr *string, r *EmptyReply) error {
	return c.Svc.RemoveNode(*addr)
}

// add
// ListReplicas RPC 绑定
func (b *CoordinatorBinding) ListReplicas(_ *EmptyArgs, reply *[]string) error {
	addrs, err := b.Svc.ListReplicas()
	if err != nil {
		return err
	}
	*reply = addrs
	return nil
}

//add

func (c *CoordinatorBinding) HandleWriteDone(args *WriteDoneArgs, reply *WriteDoneReply) error {
	return c.Svc.HandleWriteDone(args.Key, args.Version)
}

func (c *CoordinatorBinding) GetCommittedVersion(args *GetCommittedVersionArgs, reply *GetCommittedVersionReply) error {
	v, err := c.Svc.GetCommittedVersion(args.Key)
	if err != nil {
		return err
	}
	reply.Version = v
	return nil
}

// Write RPC 绑定：直接把 transport.WriteReply 填进 net/rpc reply
func (c *CoordinatorBinding) Write(args *ClientWriteArgs, r *transport.WriteReply) error {
	tr, err := c.Svc.Write(args.Key, args.Value)
	if err != nil {
		return err
	}
	*r = *tr
	return nil
}

// netrpc/coordinator.go
type WriteReply struct {
	Version     uint64
	ReplicaAddr string
}

// CoordinatorClient is for invoking net/rpc methods on a Coordinator.
type CoordinatorClient struct {
	*Client
}

// add
func (cc *CoordinatorClient) ListReplicas() ([]string, error) {
	var reply []string
	if err := cc.rpc.Call("RPC.ListReplicas", &EmptyArgs{}, &reply); err != nil {
		return nil, err
	}
	return reply, nil
}

// add
func (cc *CoordinatorClient) AddNode(addr string) (*transport.NodeMeta, error) {
	reply := &transport.NodeMeta{}
	err := cc.Client.rpc.Call("RPC.AddNode", addr, reply)
	return reply, err
}

func (cc *CoordinatorClient) RemoveNode(addr string) error {
	return cc.Client.rpc.Call("RPC.RemoveNode", addr, &EmptyReply{})
}

func (cc *CoordinatorClient) HandleWriteDone(key string, version uint64) error {
	args := WriteDoneArgs{Key: key, Version: version}
	var reply WriteDoneReply
	return cc.rpc.Call("RPC.HandleWriteDone", &args, &reply)
}

func (cc *CoordinatorClient) GetCommittedVersion(key string) (uint64, error) {
	args := GetCommittedVersionArgs{Key: key}
	var reply GetCommittedVersionReply
	if err := cc.rpc.Call("RPC.GetCommittedVersion", &args, &reply); err != nil {
		return 0, err
	}
	return reply.Version, nil
}

// 新签名：完全匹配 transport.CoordinatorService.Write
func (cc *CoordinatorClient) Write(k string, v []byte) (*transport.WriteReply, error) {
	args := ClientWriteArgs{Key: k, Value: v}
	var reply transport.WriteReply
	if err := cc.Client.rpc.Call("RPC.Write", &args, &reply); err != nil {
		return nil, err
	}
	return &reply, nil
}
