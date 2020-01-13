package raft

import (
	"bytes"
	"context"
	"github.com/Els-y/kvdb/pkg/logutil"
	pb "github.com/Els-y/kvdb/rpc"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"testing"
	"time"
)

func readyWithTimeout(n Node) Ready {
	select {
	case rd := <-n.Ready():
		return rd
	case <-time.After(time.Second):
		panic("timed out waiting for ready")
	}
}

func isMsgEqual(a, b pb.Message) bool {
	if a.Entries != nil && b.Entries != nil {
		logutil.Info("aaa")
		for i, ent := range a.Entries {
			if ent.Index != b.Entries[i].Index || ent.Term != b.Entries[i].Term {
				return false
			}
		}
	} else if a.Entries != nil {
		logutil.Info("bbb")
		return false
	} else if b.Entries != nil {
		logutil.Info("ccc")
		return false
	}

	//if a.Type != b.Type {
	//	return false
	//} else if a.From != b.From {
	//	return false
	//} else if a.To != b.To {
	//	return false
	//} else if a.Term != b.Term {
	//	return false
	//} else if a.LogTerm != b.LogTerm {
	//	return false
	//} else if a.Index != b.Index {
	//	return false
	//} else if a.Commit != b.Commit {
	//	return false
	//} else if a.Reject != b.Reject {
	//	return false
	//} else if a.RejectHint != b.RejectHint {
	//	return false
	//} else {
	//	return true
	//}
	return a.Type == b.Type && a.From == b.From && a.To == b.To && a.Term == b.Term &&
		a.LogTerm == b.LogTerm && a.Index == b.Index && a.Commit == b.Commit &&
		a.Reject == b.Reject && a.RejectHint == b.RejectHint
}

func newTestNode(localID uint64, peersID []uint64, term uint64) *Node {
	logger := zap.NewExample()
	raftLog := NewLogStore(logger)

	r := &raft{
		localID:          1,
		prs:              initProgress(localID, peersID),
		term:             term,
		vote:             None,
		raftLog:          raftLog,
		electionTimeout:  10,
		heartbeatTimeout: 1,
		logger:           logger,
	}
	r.becomeFollower(r.term, None)

	n := &Node{
		propc:      make(chan msgWithResult),
		recvc:      make(chan pb.Message),
		readyc:     make(chan Ready),
		advancec:   make(chan struct{}),
		tickc:      make(chan struct{}, 128),
		stopc:      make(chan struct{}),
		done:       make(chan struct{}),
		raft:       r,
		prevHardSt: emptyState,
		logger:     zap.NewExample(),
	}
	return n
}

// TestNodeStep ensures that node.Step sends msgProp to propc chan
// and other kinds of messages to recvc chan.
func TestNodeStep(t *testing.T) {
	for i, msgn := range pb.MessageType_name {
		n := &Node{
			propc: make(chan msgWithResult, 1),
			recvc: make(chan pb.Message, 1),
		}
		msgt := pb.MessageType(i)
		n.Step(context.TODO(), pb.Message{Type: msgt})
		// Proposal goes to proc chan. Others go to recvc chan.
		if msgt == pb.MessageType_MsgProp {
			select {
			case <-n.propc:
			default:
				t.Errorf("%d: cannot receive %s on propc chan", msgt, msgn)
			}
		} else {
			if IsLocalMsg(msgt) {
				select {
				case <-n.recvc:
					t.Errorf("%d: step should ignore %s", msgt, msgn)
				default:
				}
			} else {
				select {
				case <-n.recvc:
				default:
					t.Errorf("%d: cannot receive %s on recvc chan", msgt, msgn)
				}
			}
		}
	}
}

// Cancel and Stop should unblock Step()
func TestNodeStepUnblock(t *testing.T) {
	// a node without buffer to block step
	n := &Node{
		propc: make(chan msgWithResult),
		done:  make(chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	stopFunc := func() { close(n.done) }

	tests := []struct {
		unblock func()
		werr    error
	}{
		{stopFunc, ErrStopped},
		{cancel, context.Canceled},
	}

	for i, tt := range tests {
		errc := make(chan error, 1)
		go func() {
			err := n.Step(ctx, pb.Message{Type: pb.MessageType_MsgProp})
			errc <- err
		}()
		tt.unblock()
		select {
		case err := <-errc:
			if err != tt.werr {
				t.Errorf("#%d: err = %v, want %v", i, err, tt.werr)
			}
			//clean up side-effect
			if ctx.Err() != nil {
				ctx = context.TODO()
			}
			select {
			case <-n.done:
				n.done = make(chan struct{})
			default:
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("#%d: failed to unblock step", i)
		}
	}
}

// TestNodeTick ensures that node.Tick() will increase the
// elapsed of the underlying raft state machine.
func TestNodeTick(t *testing.T) {
	n := newTestNode(1, []uint64{2, 3}, 0)
	r := n.raft
	go n.run()
	elapsed := r.electionElapsed
	n.Tick()

	for len(n.tickc) != 0 {
		time.Sleep(100 * time.Millisecond)
	}

	n.Stop()
	if r.electionElapsed != elapsed+1 {
		t.Errorf("elapsed = %d, want %d", r.electionElapsed, elapsed+1)
	}
}

// TestNodePropose ensures that node.Propose sends the given proposal to the underlying raft.
func TestNodePropose(t *testing.T) {
	var msgs []pb.Message
	appendStep := func(r *raft, m pb.Message) error {
		msgs = append(msgs, m)
		return nil
	}

	n := newTestNode(1, []uint64{2, 3}, 0)
	go n.Start()

	r := n.raft
	r.campaign()

	for {
		rd := <-n.Ready()
		if len(rd.Messages) != 0 {
			for _, msg := range rd.Messages {
				if msg.Type == pb.MessageType_MsgVote {
					t.Log("get vote request")
					//go func() {
					n.Step(context.TODO(), pb.Message{From: 2, To: 1, Term: msg.Term, Type: pb.MessageType_MsgVoteResp})
					//}()
					break
				}
			}
		}
		// change the step function to appendStep until this raft becomes leader
		if r.leaderID == r.localID {
			r.step = appendStep
			n.Advance()
			break
		}
		n.Advance()
	}
	n.Propose(context.TODO(), []byte("somedata"))
	n.Stop()

	if len(msgs) != 1 {
		t.Fatalf("len(msgs) = %d, want %d", len(msgs), 1)
	}
	if msgs[0].Type != pb.MessageType_MsgProp {
		t.Errorf("msg type = %d, want %d", msgs[0].Type, pb.MessageType_MsgProp)
	}
	if !bytes.Equal(msgs[0].Entries[0].Data, []byte("somedata")) {
		t.Errorf("data = %v, want %v", msgs[0].Entries[0].Data, []byte("somedata"))
	}
}

// 测试 follower 收到 Propose 消息，转发给 leader
func TestFollower_RecvPropose(t *testing.T) {
	localID := uint64(1)
	term := uint64(2)
	n := newTestNode(localID, []uint64{2, 3}, term)
	go n.Start()

	r := n.raft

	// no leader
	msg := pb.Message{Type: pb.MessageType_MsgProp, Entries: []*pb.Entry{{Data: []byte("somedata")}}}
	err := r.Step(msg)
	assert.Equal(t, ErrProposalDropped, err)

	// leader 2
	leaderID := uint64(2)
	r.leaderID = leaderID
	err = r.Step(msg)
	assert.Nil(t, err)

	redirectMsg := pb.Message{
		From:    localID,
		To:      leaderID,
		Type:    pb.MessageType_MsgProp,
		Entries: []*pb.Entry{{Data: []byte("somedata")}}}
	assert.True(t, isMsgEqual(r.msgs[0], redirectMsg))
}

// 测试 follower 收到 MsgApp 且 prevLogIndex、prevLogTerm 匹配，更新 log
func TestFollowerRecvMsgAppMatch(t *testing.T) {
	localID := uint64(1)
	leaderID := uint64(2)
	term := uint64(2)
	n := newTestNode(localID, []uint64{2, 3}, term)
	r := n.raft
	r.leaderID = leaderID
	go n.Start()

	state := pb.HardState{Commit: 3, Term: term, Vote: leaderID}
	entries := []*pb.Entry{
		{},
		{Index: 1, Term: 1, Data: []byte("somedata")},
		{Index: 2, Term: 1, Data: []byte("somedata")},
		{Index: 3, Term: 2, Data: []byte("somedata")},
		{Index: 4, Term: 2, Data: []byte("somedata")},
		{Index: 5, Term: 2, Data: []byte("somedata")},
	}
	n.SetHardState(state)
	r.raftLog.Restore(state, entries)
	r.leaderID = leaderID
	assert.Equal(t, uint64(3), r.raftLog.committed)
	assert.Equal(t, uint64(5), r.raftLog.lastIndex())
	assert.Equal(t, uint64(2), r.raftLog.lastTerm())

	msgApp := pb.Message{
		Type: pb.MessageType_MsgApp, From: leaderID, To: localID, Term: term, LogTerm: 2, Index: 5, Commit: 5,
		Entries: []*pb.Entry{
			{Index: 6, Term: 2, Data: []byte("newdata")},
			{Index: 7, Term: 2, Data: []byte("newdata")},
		}}
	err := r.Step(msgApp)
	assert.Nil(t, err)
	assert.True(t, r.raftLog.matchTerm(7, 2))
	assert.Equal(t, uint64(5), r.raftLog.committed)

	wantAppResp := pb.Message{
		Type: pb.MessageType_MsgAppResp, From: localID, To: leaderID, Term: term, Index: 7,
	}
	assert.True(t, isMsgEqual(wantAppResp, r.msgs[0]))
}

// 测试 follower 收到 MsgApp，日志缺失，需要重发
func TestFollowerRecvMsgAppLost(t *testing.T) {
	localID := uint64(1)
	leaderID := uint64(2)
	term := uint64(2)
	n := newTestNode(localID, []uint64{2, 3}, term)
	r := n.raft
	r.leaderID = leaderID
	go n.Start()

	state := pb.HardState{Commit: 3, Term: term, Vote: leaderID}
	entries := []*pb.Entry{
		{},
		{Index: 1, Term: 1, Data: []byte("somedata")},
		{Index: 2, Term: 1, Data: []byte("somedata")},
		{Index: 3, Term: 2, Data: []byte("somedata")},
		{Index: 4, Term: 2, Data: []byte("somedata")},
		{Index: 5, Term: 2, Data: []byte("somedata")},
	}
	n.SetHardState(state)
	r.raftLog.Restore(state, entries)
	r.leaderID = leaderID
	assert.Equal(t, uint64(3), r.raftLog.committed)
	assert.Equal(t, uint64(5), r.raftLog.lastIndex())
	assert.Equal(t, uint64(2), r.raftLog.lastTerm())

	msgApp := pb.Message{
		Type: pb.MessageType_MsgApp, From: leaderID, To: localID, Term: term, LogTerm: 2, Index: 7, Commit: 5,
		Entries: []*pb.Entry{
			{Index: 8, Term: 2, Data: []byte("newdata")},
			{Index: 9, Term: 2, Data: []byte("newdata")},
		}}
	err := r.Step(msgApp)
	assert.Nil(t, err)

	wantAppResp := pb.Message{
		Type: pb.MessageType_MsgAppResp, From: localID, To: leaderID,
		Term: term, Index: msgApp.Index, Reject: true, RejectHint: 5,
	}
	assert.True(t, isMsgEqual(wantAppResp, r.msgs[0]))
}

// 测试 follower 收到 MsgApp，日志冲突，截断更新
func TestFollowerRecvMsgAppConflict(t *testing.T) {
	localID := uint64(1)
	leaderID := uint64(2)
	term := uint64(2)
	n := newTestNode(localID, []uint64{2, 3}, term)
	r := n.raft
	r.leaderID = leaderID
	go n.Start()

	state := pb.HardState{Commit: 3, Term: term, Vote: leaderID}
	entries := []*pb.Entry{
		{},
		{Index: 1, Term: 1, Data: []byte("somedata")},
		{Index: 2, Term: 1, Data: []byte("somedata")},
		{Index: 3, Term: 2, Data: []byte("somedata")},
		{Index: 4, Term: 2, Data: []byte("somedata")},
		{Index: 5, Term: 2, Data: []byte("somedata")},
	}
	n.SetHardState(state)
	r.raftLog.Restore(state, entries)
	r.leaderID = leaderID
	assert.Equal(t, uint64(3), r.raftLog.committed)
	assert.Equal(t, uint64(5), r.raftLog.lastIndex())
	assert.Equal(t, uint64(2), r.raftLog.lastTerm())

	msgApp := pb.Message{
		Type: pb.MessageType_MsgApp, From: leaderID, To: localID, Term: 3, LogTerm: 2, Index: 4, Commit: 4,
		Entries: []*pb.Entry{
			{Index: 5, Term: 3, Data: []byte("newdata")},
			{Index: 6, Term: 3, Data: []byte("newdata")},
		},
	}
	err := r.Step(msgApp)
	assert.Nil(t, err)
	assert.Equal(t, msgApp.Term, r.term)
	assert.Equal(t, uint64(4), r.raftLog.committed)
	assert.Equal(t, uint64(6), r.raftLog.lastIndex())
	assert.Equal(t, uint64(3), r.raftLog.lastTerm())
	assert.Equal(t, uint64(3), r.raftLog.termOrPanic(r.raftLog.term(5)))

	wantAppResp := pb.Message{
		Type: pb.MessageType_MsgAppResp, From: localID, To: leaderID, Term: 3, Index: 6,
	}
	assert.True(t, isMsgEqual(wantAppResp, r.msgs[0]))
}

// 测试 follower 收到心跳包，更新 commitIndex
func TestFollowerRecvHeartbeat(t *testing.T) {
	localID := uint64(1)
	leaderID := uint64(2)
	term := uint64(2)
	n := newTestNode(localID, []uint64{2, 3}, term)
	r := n.raft
	r.leaderID = leaderID
	go n.Start()

	state := pb.HardState{Commit: 3, Term: term, Vote: leaderID}
	entries := []*pb.Entry{
		{},
		{Index: 1, Term: 1, Data: []byte("somedata")},
		{Index: 2, Term: 1, Data: []byte("somedata")},
		{Index: 3, Term: 2, Data: []byte("somedata")},
		{Index: 4, Term: 2, Data: []byte("somedata")},
		{Index: 5, Term: 2, Data: []byte("somedata")},
	}
	n.SetHardState(state)
	r.raftLog.Restore(state, entries)
	r.leaderID = leaderID

	msg := pb.Message{Type: pb.MessageType_MsgHeartbeat, From: leaderID, To: localID, Term: term, Commit: 4}
	err := r.Step(msg)
	assert.Nil(t, err)
	assert.Equal(t, uint64(4), r.raftLog.committed)
	assert.Equal(t, uint64(5), r.raftLog.lastIndex())
	assert.Equal(t, uint64(2), r.raftLog.lastTerm())

	wantResp := pb.Message{Type: pb.MessageType_MsgHeartbeatResp, From: localID, To: leaderID, Term: term}
	assert.True(t, isMsgEqual(wantResp, r.msgs[0]))
}

// 测试 candidate 收到 Propose 消息
func TestCandidate_RecvPropose(t *testing.T) {
	localID := uint64(1)
	term := uint64(2)
	n := newTestNode(localID, []uint64{2, 3}, term)
	go n.Start()

	r := n.raft
	r.campaign()

	msg := pb.Message{Type: pb.MessageType_MsgProp, Entries: []*pb.Entry{{Data: []byte("somedata")}}}
	err := r.Step(msg)
	assert.Equal(t, ErrProposalDropped, err)
}

// 测试 candidate 收到 MsgApp 变回 follower
func TestCandidate_RecvMsgApp(t *testing.T) {
	localID := uint64(1)
	leaderID := uint64(2)
	n := newTestNode(localID, []uint64{2, 3}, 1)
	go n.Start()

	r := n.raft
	r.campaign()
	assert.Equal(t, StateCandidate, r.state)

	msg := pb.Message{
		Type: pb.MessageType_MsgApp, From: leaderID, To: localID, Term: 2, LogTerm: 0, Index: 0, Commit: 0,
		Entries: []*pb.Entry{
			{Index: 1, Term: 2, Data: []byte("newdata")},
			{Index: 2, Term: 2, Data: []byte("newdata")},
		},
	}
	err := r.Step(msg)
	assert.Nil(t, err)
	assert.Equal(t, StateFollower, r.state)

	wantResp := pb.Message{Type: pb.MessageType_MsgAppResp, From: localID, To: leaderID, Term: 2, Index: 2}
	// r.msgs[0:2] 为 MsgVote
	assert.True(t, isMsgEqual(wantResp, r.msgs[2]))
}

// 测试 candidate 收到心跳包，变回 follower
func TestCandidate_RecvHeartbeat(t *testing.T) {
	localID := uint64(1)
	leaderID := uint64(2)
	n := newTestNode(localID, []uint64{2, 3}, 1)
	go n.Start()

	r := n.raft
	r.campaign()
	assert.Equal(t, StateCandidate, r.state)

	msg := pb.Message{Type: pb.MessageType_MsgHeartbeat, From: leaderID, To: localID, Term: 2, Commit: 0}
	err := r.Step(msg)
	assert.Nil(t, err)
	assert.Equal(t, StateFollower, r.state)

	wantResp := pb.Message{Type: pb.MessageType_MsgHeartbeatResp, From: localID, To: leaderID, Term: 2}
	// r.msgs[0:2] 为 MsgVote
	assert.True(t, isMsgEqual(wantResp, r.msgs[2]))
}

// 测试 candidate 收到 MsgVoteResp 更新状态，大多数同意，最终变成 leader
func TestCandidate_RecvMsgVoteRespWon(t *testing.T) {
	localID := uint64(1)
	n := newTestNode(localID, []uint64{2, 3, 4, 5}, 1)
	go n.Start()

	r := n.raft
	r.campaign()
	assert.Equal(t, StateCandidate, r.state)

	msg := pb.Message{Type: pb.MessageType_MsgVoteResp, From: 2, To: localID, Term: 2}
	err := r.Step(msg)
	assert.Nil(t, err)
	assert.Equal(t, StateCandidate, r.state)

	msg = pb.Message{Type: pb.MessageType_MsgVoteResp, From: 3, To: localID, Term: 2, Reject: true}
	err = r.Step(msg)
	assert.Nil(t, err)
	assert.Equal(t, StateCandidate, r.state)

	msg = pb.Message{Type: pb.MessageType_MsgVoteResp, From: 4, To: localID, Term: 2}
	err = r.Step(msg)
	assert.Nil(t, err)
	assert.Equal(t, StateLeader, r.state)
}

// 测试 candidate 收到 MsgVoteResp 更新状态，大多数拒绝，变回 follower
func TestCandidate_RecvMsgVoteRespLose(t *testing.T) {
	localID := uint64(1)
	n := newTestNode(localID, []uint64{2, 3, 4, 5}, 1)
	go n.Start()

	r := n.raft
	r.leaderID = uint64(2)
	r.campaign()
	assert.Equal(t, StateCandidate, r.state)

	msg := pb.Message{Type: pb.MessageType_MsgVoteResp, From: 2, To: localID, Term: 2, Reject: true}
	err := r.Step(msg)
	assert.Nil(t, err)
	assert.Equal(t, StateCandidate, r.state)

	msg = pb.Message{Type: pb.MessageType_MsgVoteResp, From: 3, To: localID, Term: 2, Reject: true}
	err = r.Step(msg)
	assert.Nil(t, err)
	assert.Equal(t, StateCandidate, r.state)

	msg = pb.Message{Type: pb.MessageType_MsgVoteResp, From: 4, To: localID, Term: 2, Reject: true}
	err = r.Step(msg)
	assert.Nil(t, err)
	assert.Equal(t, StateFollower, r.state)
	assert.Equal(t, None, r.leaderID)
}

// 测试 leader 收到 Propose 更新日志，广播 MsgApp
func TestLeader_RecvPropose(t *testing.T) {
	localID := uint64(1)
	n := newTestNode(localID, []uint64{2, 3}, 1)
	go n.Start()

	r := n.raft
	r.campaign()
	msg := pb.Message{Type: pb.MessageType_MsgVoteResp, From: 2, To: localID, Term: 2}
	err := r.Step(msg)
	assert.Nil(t, err)
	assert.Equal(t, StateLeader, r.state)

	msg = pb.Message{Type: pb.MessageType_MsgProp, Entries: []*pb.Entry{{Data: []byte("somedata")}}}
	err = r.Step(msg)
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), r.raftLog.applied)
	assert.Equal(t, uint64(0), r.raftLog.committed)
	assert.Equal(t, uint64(1), r.raftLog.lastIndex())
	assert.Equal(t, uint64(2), r.raftLog.lastTerm())

	for i, msg := range r.msgs[:2] {
		assert.Equal(t, pb.MessageType_MsgVote, msg.Type)
		assert.Equal(t, uint64(i+2), msg.To)
	}

	for i, msg := range r.msgs[2:4] {
		assert.Equal(t, pb.MessageType_MsgApp, msg.Type)
		assert.Equal(t, uint64(i+2), msg.To)
		assert.Nil(t, msg.Entries)
	}

	for i, msg := range r.msgs[4:] {
		assert.Equal(t, pb.MessageType_MsgApp, msg.Type)
		assert.Equal(t, uint64(i+2), msg.To)
		assert.Equal(t, 1, len(msg.Entries))
	}
}

// 测试 leader 收到 MsgBeat，广播 MsgApp
func TestLeader_RecvMsgBeat(t *testing.T) {
	localID := uint64(1)
	n := newTestNode(localID, []uint64{2, 3}, 1)
	go n.Start()

	r := n.raft
	r.campaign()
	msg := pb.Message{Type: pb.MessageType_MsgVoteResp, From: 2, To: localID, Term: 2}
	err := r.Step(msg)
	assert.Nil(t, err)
	assert.Equal(t, StateLeader, r.state)

	msg = pb.Message{Type: pb.MessageType_MsgBeat, From: localID}
	err = r.Step(msg)
	assert.Nil(t, err)

	for i, msg := range r.msgs[4:] {
		assert.Equal(t, pb.MessageType_MsgHeartbeat, msg.Type)
		assert.Equal(t, uint64(i+2), msg.To)
		assert.Nil(t, msg.Entries)
	}
}

// 测试 leader 收到 MsgAppResp，更新 Progress
func TestLeader_RecvMsgAppResp(t *testing.T) {
	localID := uint64(1)
	n := newTestNode(localID, []uint64{2, 3}, 1)
	go n.Start()

	r := n.raft

	state := pb.HardState{Commit: 3, Term: 1, Vote: localID}
	entries := []*pb.Entry{
		{},
		{Index: 1, Term: 1, Data: []byte("somedata")},
		{Index: 2, Term: 1, Data: []byte("somedata")},
		{Index: 3, Term: 2, Data: []byte("somedata")},
		{Index: 4, Term: 2, Data: []byte("somedata")},
		{Index: 5, Term: 2, Data: []byte("somedata")},
	}
	n.SetHardState(state)
	r.raftLog.Restore(state, entries)

	r.campaign()
	msg := pb.Message{Type: pb.MessageType_MsgVoteResp, From: 2, To: localID, Term: 2}
	err := r.Step(msg)
	assert.Nil(t, err)
	assert.Equal(t, StateLeader, r.state)

	assert.Equal(t, uint64(6), r.prs.cluster[2].NextIndex)
	assert.Equal(t, uint64(6), r.prs.cluster[3].NextIndex)
	assert.Equal(t, uint64(0), r.prs.cluster[2].MatchIndex)
	assert.Equal(t, uint64(0), r.prs.cluster[3].MatchIndex)

	// 正常 resp
	msg = pb.Message{Type: pb.MessageType_MsgAppResp, From: 2, To: localID, Term: 2, Index: 4}
	err = r.Step(msg)
	assert.Equal(t, uint64(4), r.prs.cluster[2].MatchIndex)

	// reject
	msg = pb.Message{Type: pb.MessageType_MsgAppResp, From: 3, To: localID, Term: 2, Index: 5, Reject: true, RejectHint: 2}
	err = r.Step(msg)
	// retry MsgApp
	retryMsg := r.msgs[6]
	assert.Equal(t, uint64(3), r.prs.cluster[3].NextIndex)
	assert.Equal(t, pb.MessageType_MsgApp, retryMsg.Type)
	assert.Equal(t, uint64(2), retryMsg.Index)
	assert.Equal(t, 3, len(retryMsg.Entries))
	assert.Equal(t, uint64(3), retryMsg.Entries[0].Index)
	assert.Equal(t, uint64(5), retryMsg.Entries[2].Index)
}

// 测试 leader 收到 MsgHeartbeatResp，判断 follower MatchIndex 是否匹配，不匹配发送 MsgApp
func TestLeader_RecvHeartbeatResp(t *testing.T) {
	localID := uint64(1)
	n := newTestNode(localID, []uint64{2, 3}, 1)
	go n.Start()

	r := n.raft

	state := pb.HardState{Commit: 3, Term: 1, Vote: localID}
	entries := []*pb.Entry{
		{},
		{Index: 1, Term: 1, Data: []byte("somedata")},
		{Index: 2, Term: 1, Data: []byte("somedata")},
		{Index: 3, Term: 2, Data: []byte("somedata")},
		{Index: 4, Term: 2, Data: []byte("somedata")},
		{Index: 5, Term: 2, Data: []byte("somedata")},
	}
	n.SetHardState(state)
	r.raftLog.Restore(state, entries)

	r.campaign()
	msg := pb.Message{Type: pb.MessageType_MsgVoteResp, From: 2, To: localID, Term: 2}
	err := r.Step(msg)
	assert.Nil(t, err)
	assert.Equal(t, StateLeader, r.state)
	assert.Equal(t, 4, len(r.msgs))

	msg = pb.Message{Type: pb.MessageType_MsgHeartbeatResp, From: 2, To: localID, Term: 2}
	err = r.Step(msg)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(r.msgs))

	msgApp := r.msgs[4]
	assert.True(t, isMsgEqual(r.msgs[2], r.msgs[4]))
	assert.Equal(t, pb.MessageType_MsgApp, msgApp.Type)
	assert.Equal(t, uint64(5), msgApp.Index)
	assert.Equal(t, uint64(2), msgApp.Term)

	r.prs.maybeUpdate(2, 5)
	err = r.Step(msg)
	assert.Nil(t, err)
	assert.Equal(t, 5, len(r.msgs))
}

// 测试 server 将日志应用到 kv 和 wal 持久化并更新 appliedIndex 和 unstable
func TestNode_ApplyAndPersistentLog(t *testing.T) {
	localID := uint64(1)
	n := newTestNode(localID, []uint64{2, 3}, 1)
	go n.Start()

	r := n.raft
	done := make(chan struct{})
	go func() {
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		for {
			select {
			case <-tick.C:
				n.Tick()
			case <-done:
				return
			}
		}
	}()

	rd := <-n.Ready()
	assert.Equal(t, 1, len(rd.Entries))
	assert.Equal(t, 0, len(rd.CommittedEntries))

	//n.Advance()
	n.advancec <- struct{}{}
	r.advance(rd)
	assert.Equal(t, uint64(1), r.raftLog.unstable)
	assert.Equal(t, uint64(0), r.raftLog.applied)
	assert.Equal(t, uint64(0), r.raftLog.committed)

	msgApp := pb.Message{
		Type: pb.MessageType_MsgApp, From: 2, To: localID, Term: 1, LogTerm: 0, Index: 0, Commit: 0,
		Entries: []*pb.Entry{
			{Index: 1, Term: 1, Data: []byte("somedata")},
			{Index: 2, Term: 1, Data: []byte("somedata")},
			{Index: 3, Term: 2, Data: []byte("somedata")},
		}}
	err := r.Step(msgApp)
	assert.Nil(t, err)
	assert.Equal(t, uint64(3), r.raftLog.lastIndex())
	assert.Equal(t, uint64(2), r.raftLog.lastTerm())
	assert.Equal(t, 3, len(r.raftLog.unstableEntries()))

	assert.True(t, n.HasReady())

	rd = <-n.Ready()
	assert.Equal(t, 3, len(rd.Entries))
	assert.Equal(t, 0, len(rd.CommittedEntries))

	n.advancec <- struct{}{}
	r.advance(rd)
	assert.Equal(t, uint64(4), r.raftLog.unstable)
	assert.Equal(t, uint64(0), r.raftLog.applied)
	assert.Equal(t, uint64(0), r.raftLog.committed)

	msgApp = pb.Message{
		Type: pb.MessageType_MsgApp, From: 2, To: localID, Term: 2, LogTerm: 2, Index: 3, Commit: 2,
		Entries: []*pb.Entry{
			{Index: 4, Term: 2, Data: []byte("newdata")},
			{Index: 5, Term: 2, Data: []byte("newdata")},
		}}
	err = r.Step(msgApp)
	assert.Nil(t, err)
	assert.True(t, r.raftLog.matchTerm(5, 2))
	assert.Equal(t, uint64(2), r.raftLog.committed)

	rd = <-n.Ready()
	assert.Equal(t, 2, len(rd.Entries))
	assert.Equal(t, 2, len(rd.CommittedEntries))
	n.Advance()
	close(done)
}

// 测试 server 收到 candidate 发来的 MsgVote，包括三种情况：
// 1. candidate 日志比自身要旧，拒绝；
// 2. 该轮已经投过票了，拒绝；
// 3. 日志一致或更新，同意
//func TestNode_RecvMsgVote(t *testing.T) {
//
//}
