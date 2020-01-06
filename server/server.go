package server

import (
	"context"
	"github.com/Els-y/kvdb/kv"
	"github.com/Els-y/kvdb/raft"
	"github.com/Els-y/kvdb/transport"
	"github.com/Els-y/kvdb/wal"
	"go.uber.org/zap"
	"sync"
	"time"
)

type Server struct {
	storage   *kv.KVStore
	raftLog   *raft.LogStore
	raftNode  *raft.Node
	wal       *wal.WAL
	transport *transport.Transport

	intervalTick time.Duration

	logger *zap.Logger
	wg     sync.WaitGroup

	propc chan string
	stopc chan struct{}
}

func NewServer(cfg *Config) *Server {
	s := &Server{
		storage:      kv.NewKVStore(),
		raftLog:      raft.NewLogStore(cfg.Logger),
		intervalTick: cfg.IntervalTick,
		logger:       cfg.Logger,
		propc:        make(chan string),
		stopc:        make(chan struct{}),
	}

	raftCfg := &raft.Config{
		LocalID:          cfg.ID,
		PeersID:          cfg.PeersID,
		RaftLog:          s.raftLog,
		Logger:           cfg.Logger,
		ElectionTimeout:  cfg.ElectionTick,
		HeartbeatTimeout: cfg.HeartbeatTick,
	}
	s.raftNode = raft.NewNode(raftCfg)

	transportCfg := &transport.Config{
		LocalID:          cfg.ID,
		PeersID:          cfg.PeersID,
		PeersURL:         cfg.PeersURL,
		PeerServerPort:   cfg.PeerServerPort,
		ClientServerPort: cfg.ClientServerPort,
		Storage:          s.storage,
		RaftNode:         s.raftNode,
		Logger:           cfg.Logger,
		PropC:            s.propc,
	}
	s.transport = transport.NewTransport(transportCfg)

	s.wal = s.replayWAL(cfg.WalDir)

	return s
}

func (s *Server) Start() {
	s.wg.Add(3)
	go s.raftNode.Start()
	go s.transport.Start()
	go s.serveChannels()
	s.wg.Wait()
}

func (s *Server) stop() {
	s.raftNode.Stop()
}

func (s *Server) serveChannels() {
	ticker := time.NewTicker(s.intervalTick)
	defer ticker.Stop()

	go func() {
		for s.propc != nil {
			prop, ok := <-s.propc
			if !ok {
				s.propc = nil
			} else {
				// blocks until accepted by raft state machine
				_ = s.raftNode.Propose(context.TODO(), []byte(prop))
			}
		}
		close(s.stopc)
	}()

	for {
		select {
		case <-ticker.C:
			s.raftNode.Tick()
		case rd := <-s.raftNode.Ready():
			// wal save
			s.wal.Save(rd.HardState, rd.Entries)
			// s.storage.Update()  更新 kv
			s.transport.Send(rd.Messages)
			s.raftNode.Advance()
		case <-s.stopc:
			s.stop()
			return
		}
	}
}

func (s *Server) replayWAL(walDir string) *wal.WAL {
	if !wal.Exist(walDir) {
		// TODO: create wal
		w := wal.Create(walDir, s.logger)
		return w
	}

	w := wal.Restore(walDir, s.logger)
	state, entries, err := w.ReadAll()
	if err != nil {
		s.logger.Panic("replay wal fail", zap.Error(err))
	}
	s.raftNode.SetHardState(state)
	s.raftLog.Restore(state, entries)
	// TODO: update kv
	//s.storage.Restore(entries)
	return w
}
