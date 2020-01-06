package transport

import (
	"context"
	"github.com/Els-y/kvdb/kv"
	pb "github.com/Els-y/kvdb/rpc"
)

type ClientServer struct {
	kvstore *kv.KVStore
	propc   chan string
}

func NewClientServer(kvstore *kv.KVStore, propc chan string) *ClientServer {
	return &ClientServer{kvstore: kvstore, propc: propc}
}

func (s *ClientServer) Get(ctx context.Context, req *pb.GetRequest) (*pb.GetResponse, error) {
	val, ok := s.kvstore.Get(string(req.Key))
	resp := &pb.GetResponse{Val: []byte(val), Ok: ok}
	return resp, nil
}

func (s *ClientServer) Put(ctx context.Context, req *pb.PutRequest) (*pb.PutResponse, error) {
	prop := s.kvstore.ProposePut(string(req.Key), string(req.Val))
	resp := &pb.PutResponse{
		Ok: true,
	}
	s.propc <- prop
	//if err := s.raftNode.Propose(context.TODO(), []byte(prop)); err != nil {
	//	resp.Ok = false
	//}
	return resp, nil
}

func (s *ClientServer) Del(ctx context.Context, req *pb.DelRequest) (*pb.DelResponse, error) {
	prop := s.kvstore.ProposeDel(string(req.Key))
	resp := &pb.DelResponse{
		Ok: true,
	}
	s.propc <- prop
	//if err := s.raftNode.Propose(context.TODO(), []byte(prop)); err != nil {
	//	resp.Ok = false
	//}
	return resp, nil
}
