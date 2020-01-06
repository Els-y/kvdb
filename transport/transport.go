package transport

import (
	"context"
	"github.com/Els-y/kvdb/kv"
	"github.com/Els-y/kvdb/raft"
	pb "github.com/Els-y/kvdb/rpc"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"net/url"
	"strconv"
)

type Config struct {
	LocalID          uint64
	PeersID          []uint64
	PeersURL         []url.URL
	PeerServerPort   int
	ClientServerPort int
	Storage          *kv.KVStore
	RaftNode         *raft.Node
	PropC            chan string
	StopC            chan struct{}
	Logger           *zap.Logger
}

type Transport struct {
	peerServerPort   int
	clientServerPort int
	peers            map[uint64]*Peer
	storage          *kv.KVStore
	raftNode         *raft.Node
	logger           *zap.Logger
	PropC            chan string
	StopC            chan struct{}
	DoneC            chan struct{}
	ErrorC           chan error
}

func NewTransport(cfg *Config) *Transport {
	return &Transport{
		peerServerPort:   cfg.PeerServerPort,
		clientServerPort: cfg.ClientServerPort,
		peers:            initPeers(cfg.RaftNode, cfg.PeersID, cfg.PeersURL),
		storage:          cfg.Storage,
		raftNode:         cfg.RaftNode,
		logger:           cfg.Logger,
		PropC:            cfg.PropC,
		StopC:            cfg.StopC,
		DoneC:            make(chan struct{}),
		ErrorC:           make(chan error),
	}
}

func (t *Transport) Start() {
	go t.serveClientRPC()
	t.servePeerRPC()
}

func (t *Transport) Stop() {

}

func (t *Transport) Send(messages []pb.Message) {
	for _, msg := range messages {
		if msg.To == 0 {
			// ignore intentionally dropped message
			continue
		}

		if ok := t.send(msg.To, msg); ok {
			continue
		}
	}
}

func (t *Transport) send(id uint64, msg pb.Message) bool {
	p, ok := t.peers[id]
	if !ok {
		return ok
	}

	// TODO: context 是否需要超时？是否需要重试？resp 处理？
	_, err := p.client.Send(context.TODO(), &msg)
	if err != nil {
		t.logger.Debug("send msg fail",
			zap.Uint64("to", msg.To),
			zap.String("msgType", msg.Type.String()))
		return false
	}

	//t.logger.Debug("send msg success",
	//	zap.Uint64("to", msg.To),
	//	zap.String("resp", string(resp.Err)),
	//	zap.String("msgType", msg.Type.String()))
	return true
}

func (t *Transport) servePeerRPC() {
	ln, err := newStoppableListener(":"+strconv.Itoa(t.peerServerPort), t.StopC)
	if err != nil {
		t.logger.Fatal("failed to create peer listener", zap.Error(err))
	}
	srv := grpc.NewServer()
	pb.RegisterRaftServer(srv, NewPeerServer(t.raftNode))
	if err := srv.Serve(ln); err != nil {
		t.logger.Fatal("failed to serve peer rpc server", zap.Error(err))
	}
}

func (t *Transport) serveClientRPC() {
	ln, err := newStoppableListener(":"+strconv.Itoa(t.clientServerPort), t.StopC)
	if err != nil {
		t.logger.Fatal("failed to create client listener", zap.Error(err))
	}
	srv := grpc.NewServer()
	pb.RegisterKVServer(srv, NewClientServer(t.storage, t.PropC))
	if err := srv.Serve(ln); err != nil {
		t.logger.Fatal("failed to serve client rpc server", zap.Error(err))
	}
}
