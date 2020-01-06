package transport

import (
	"context"
	"github.com/Els-y/kvdb/raft"
	pb "github.com/Els-y/kvdb/rpc"
)

type PeerServer struct {
	raftNode *raft.Node
}

func NewPeerServer(r *raft.Node) *PeerServer {
	return &PeerServer{
		raftNode: r,
	}
}

// 接收 AppendEntries 请求处理完直接返回 Response
func (s *PeerServer) Send(ctx context.Context, req *pb.Message) (*pb.Response, error) {
	resp := &pb.Response{}
	if err := s.raftNode.Step(context.TODO(), *req); err != nil {
		resp.Err = []byte(err.Error())
	}
	return resp, nil
}
