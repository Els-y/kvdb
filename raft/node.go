package raft

import (
	"context"
	"errors"
	pb "github.com/Els-y/kvdb/rpc"
	"go.uber.org/zap"
)

var (
	emptyState = pb.HardState{}
	ErrStopped = errors.New("raft: stopped")
)

type Config struct {
	LocalID uint64
	PeersID []uint64

	RaftLog *LogStore

	ElectionTimeout  int
	HeartbeatTimeout int

	Logger *zap.Logger
}

type Ready struct {
	// The current state of a Node to be saved to stable storage BEFORE
	// Messages are sent.
	// HardState will be equal to empty state if there is no update.
	pb.HardState

	// Entries specifies entries to be saved to stable storage BEFORE
	// Messages are sent.
	Entries []*pb.Entry

	// CommittedEntries specifies entries to be committed to a
	// store/state-machine. These have previously been committed to stable
	// store.
	CommittedEntries []*pb.Entry

	// Messages specifies outbound messages to be sent AFTER Entries are
	// committed to stable storage.
	// If it contains a MsgSnap message, the application MUST report back to raft
	// when the snapshot has been received or has failed by calling ReportSnapshot.
	Messages []pb.Message
}

type msgWithResult struct {
	m      pb.Message
	result chan error
}

type Node struct {
	propc    chan msgWithResult
	recvc    chan pb.Message
	readyc   chan Ready
	advancec chan struct{}
	tickc    chan struct{}
	stopc    chan struct{}
	done     chan struct{}

	raft       *raft
	prevHardSt pb.HardState

	logger *zap.Logger
}

func NewNode(cfg *Config) *Node {
	return &Node{
		propc:      make(chan msgWithResult),
		recvc:      make(chan pb.Message),
		readyc:     make(chan Ready),
		advancec:   make(chan struct{}),
		tickc:      make(chan struct{}, 128),
		stopc:      make(chan struct{}),
		done:       make(chan struct{}),
		raft:       newRaft(cfg),
		prevHardSt: emptyState,
		logger:     cfg.Logger,
	}
}

func (n *Node) Start() {
	n.run()
}

func (n *Node) Stop() {
	select {
	case n.stopc <- struct{}{}:
	case <-n.done:
		return
	}
	<-n.done
}

func (n *Node) SetHardState(state pb.HardState) {
	n.prevHardSt = state
	n.raft.term = state.Term
	n.raft.vote = state.Vote
}

func (n *Node) run() {
	var propc chan msgWithResult
	var readyc chan Ready
	var advancec chan struct{}
	var rd Ready

	r := n.raft

	leader := None

	for {
		if advancec != nil {
			readyc = nil
		} else if n.HasReady() {
			rd = n.readyWithoutAccept()
			readyc = n.readyc
		}

		if leader != r.leaderID {
			if r.hasLeader() {
				if leader == None {
					r.logger.Info("raft.node elected leader",
						zap.Uint64("localID", r.localID),
						zap.Uint64("leaderID", r.leaderID),
						zap.Uint64("term", r.term))
				} else {
					r.logger.Info("raft.node changed leader",
						zap.Uint64("localID", r.localID),
						zap.Uint64("OldLeaderID", leader),
						zap.Uint64("NewleaderID", r.leaderID),
						zap.Uint64("term", r.term))
				}
				propc = n.propc
			} else {
				r.logger.Info("raft.node lost leader",
					zap.Uint64("localID", r.localID),
					zap.Uint64("leaderID", r.leaderID),
					zap.Uint64("term", r.term))
				propc = nil
			}
			leader = r.leaderID
		}

		select {
		case pm := <-propc:
			m := pm.m
			m.From = r.localID
			n.logger.Debug("raftNode run received propc, execute r.Step")
			err := r.Step(m)
			if pm.result != nil {
				pm.result <- err
				close(pm.result)
			}
		case m := <-n.recvc:
			// 忽略错误
			_ = r.Step(m)
		case <-n.tickc:
			r.tick()
		case readyc <- rd:
			n.acceptReady(rd)
			advancec = n.advancec
		case <-advancec:
			n.advance(rd)
			rd = Ready{}
			advancec = nil
		case <-n.stopc:
			close(n.done)
			return
		}
	}
}

func (n *Node) HasReady() bool {
	r := n.raft
	if hardSt := r.hardState(); !IsEmptyHardState(hardSt) && !IsHardStateEqual(hardSt, n.prevHardSt) {
		return true
	}
	if len(r.msgs) > 0 || len(r.raftLog.unstableEntries()) > 0 || r.raftLog.hasNextEntries() {
		//n.logger.Debug("HasReady true of msgs or entries",
		//	zap.Int("len(r.msgs)", len(r.msgs)),
		//	zap.Int("len(r.raftLog.unstableEntries())", len(r.raftLog.unstableEntries())),
		//	zap.Bool("r.raftLog.hasNextEntries()", r.raftLog.hasNextEntries()))
		return true
	}
	return false
}

func (n *Node) readyWithoutAccept() Ready {
	rd := Ready{
		Entries:          n.raft.raftLog.unstableEntries(),
		CommittedEntries: n.raft.raftLog.nextEntries(),
		Messages:         n.raft.msgs,
	}
	if hardSt := n.raft.hardState(); !IsHardStateEqual(hardSt, n.prevHardSt) {
		rd.HardState = hardSt
	}
	return rd
}

func (n *Node) acceptReady(rd Ready) {
	n.raft.msgs = nil
}

func (n *Node) Ready() <-chan Ready {
	return n.readyc
}

func (n *Node) Advance() {
	select {
	case n.advancec <- struct{}{}:
	case <-n.done:
	}
}

func (n *Node) advance(rd Ready) {
	if !IsEmptyHardState(rd.HardState) {
		n.prevHardSt = rd.HardState
	}
	n.raft.advance(rd)
}

func (n *Node) Tick() {
	select {
	case n.tickc <- struct{}{}:
	case <-n.done:
	default:
		n.logger.Warn("A tick missed to fire. Node blocks too long!",
			zap.Uint64("localID", n.raft.localID))
	}
}

func (n *Node) Propose(ctx context.Context, data []byte) error {
	return n.stepWait(ctx, pb.Message{Type: pb.MessageType_MsgProp, Entries: []*pb.Entry{{Data: data}}})
}

func (n *Node) Step(ctx context.Context, msg pb.Message) error {
	if IsLocalMsg(msg.Type) {
		return nil
	}
	return n.step(ctx, msg)
}

func (n *Node) step(ctx context.Context, m pb.Message) error {
	return n.stepWithWaitOption(ctx, m, false)
}

func (n *Node) stepWait(ctx context.Context, m pb.Message) error {
	return n.stepWithWaitOption(ctx, m, true)
}

func (n *Node) stepWithWaitOption(ctx context.Context, m pb.Message, wait bool) error {
	if m.Type != pb.MessageType_MsgProp {
		select {
		case n.recvc <- m:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-n.done:
			return ErrStopped
		}
	}
	ch := n.propc
	pm := msgWithResult{m: m}
	if wait {
		pm.result = make(chan error, 1)
	}
	select {
	case ch <- pm:
		if !wait {
			return nil
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}

	select {
	case err := <-pm.result:
		if err != nil {
			return err
		}
	case <-ctx.Done():
		return ctx.Err()
	case <-n.done:
		return ErrStopped
	}
	return nil
}

func IsHardStateEqual(a, b pb.HardState) bool {
	return a.Term == b.Term && a.Vote == b.Vote && a.Commit == b.Commit
}

func IsEmptyHardState(st pb.HardState) bool {
	return IsHardStateEqual(st, emptyState)
}

func (rd Ready) appliedCursor() uint64 {
	if n := len(rd.CommittedEntries); n > 0 {
		return rd.CommittedEntries[n-1].Index
	}
	return 0
}
