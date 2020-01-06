package transport

import (
	"context"
	"github.com/Els-y/kvdb/raft"
	pb "github.com/Els-y/kvdb/rpc"
	"google.golang.org/grpc"
	"net/url"
)

type Peer struct {
	u        url.URL
	conn     *grpc.ClientConn
	client   pb.RaftClient
	raftNode *raft.Node
}

func newPeer(u url.URL, r *raft.Node) *Peer {
	p := &Peer{
		u:        u,
		raftNode: r,
	}
	p.dial()
	return p
}

func (p *Peer) dial() {
	ctx := context.Background()
	conn, err := grpc.DialContext(ctx, p.u.Host, grpc.WithInsecure())
	if err != nil {
		// TODO: 怎么处理这一块？
		panic("dial error")
	}
	p.conn = conn
	p.client = pb.NewRaftClient(conn)
}

func initPeers(raftNode *raft.Node, peersID []uint64, peersURL []url.URL) map[uint64]*Peer {
	peers := make(map[uint64]*Peer, len(peersID))
	for idx, id := range peersID {
		u := peersURL[idx]
		peers[id] = newPeer(u, raftNode)
	}
	return peers
}
