package raft

import (
	"errors"
	"fmt"
	pb "github.com/Els-y/kvdb/rpc"
	"go.uber.org/zap"
	"math/rand"
	"sync"
	"time"
)

type StateType uint64

const (
	StateFollower StateType = iota
	StateCandidate
	StateLeader
)

const None uint64 = 0

var ErrProposalDropped = errors.New("raft proposal dropped")

type LockedRand struct {
	mu   sync.Mutex
	rand *rand.Rand
}

func (r *LockedRand) Intn(n int) int {
	r.mu.Lock()
	v := r.rand.Intn(n)
	r.mu.Unlock()
	return v
}

var globalRand = &LockedRand{
	rand: rand.New(rand.NewSource(time.Now().UnixNano())),
}

var globalLogger = zap.NewExample()

type stepFunc func(r *raft, msg pb.Message) error

type raft struct {
	localID  uint64
	leaderID uint64

	// Persistent state
	term    uint64    // currentterm
	vote    uint64    // votedFor
	raftLog *LogStore // commitIndex, lastApplied

	state StateType

	// Volatile state on leader
	prs *Progress // NextIndex, MatchIndex 由该结构管理

	msgs []pb.Message

	electionElapsed  int
	heartbeatElapsed int

	electionTimeout  int
	heartbeatTimeout int

	randomizedElectionTimeout int

	tick func()
	step stepFunc

	logger *zap.Logger
}

func newRaft(cfg *Config) *raft {
	r := &raft{
		localID:          cfg.LocalID,
		prs:              initProgress(cfg.LocalID, cfg.PeersID),
		term:             0,
		vote:             None,
		raftLog:          cfg.RaftLog,
		electionTimeout:  cfg.ElectionTimeout,
		heartbeatTimeout: cfg.HeartbeatTimeout,
		logger:           cfg.Logger,
	}
	r.becomeFollower(r.term, None)
	if len(cfg.PeersID) == 0 {
		r.becomeCandidate()
		r.becomeLeader()
	}
	return r
}

func (r *raft) reset(term uint64) {
	if r.term != term {
		r.term = term
		r.vote = None
	}
	r.leaderID = None
	r.prs.resetVotes()

	r.electionElapsed = 0
	r.heartbeatElapsed = 0
	r.resetRandomizedElectionTimeout()
}

func (r *raft) resetRandomizedElectionTimeout() {
	r.randomizedElectionTimeout = r.electionTimeout + globalRand.Intn(r.electionTimeout)
}

func (r *raft) becomeFollower(term uint64, leaderId uint64) {
	r.step = stepFollower
	r.reset(term)
	r.tick = r.tickElection
	r.leaderID = leaderId
	r.state = StateFollower
	r.logger.Info("became follower",
		zap.Uint64("localID", r.localID),
		zap.Uint64("term", r.term))
}

func (r *raft) becomeCandidate() {
	if r.state == StateLeader {
		panic("invalid transition [leader -> candidate]")
	}
	r.step = stepCandidate
	r.reset(r.term + 1)
	r.prs.vote(r.localID, false)
	r.tick = r.tickElection
	r.vote = r.localID
	r.state = StateCandidate
	r.logger.Info("became candidate",
		zap.Uint64("localID", r.localID),
		zap.Uint64("term", r.term))
}

func (r *raft) becomeLeader() {
	if r.state == StateFollower {
		panic("invalid transition [follower -> leader]")
	}
	r.step = stepLeader
	r.reset(r.term)
	r.tick = r.tickHeartBeat
	r.leaderID = r.localID
	r.state = StateLeader
	r.prs.resetForLeader(r.raftLog.lastIndex())
	r.logger.Info("became leader",
		zap.Uint64("localID", r.localID),
		zap.Uint64("term", r.term))
}

func (r *raft) tickElection() {
	r.electionElapsed++
	if r.electionElapsed >= r.randomizedElectionTimeout {
		r.electionElapsed = 0
		r.Step(pb.Message{From: r.localID, Type: pb.MessageType_MsgHup})
	}
}

func (r *raft) tickHeartBeat() {
	r.heartbeatElapsed++

	if r.heartbeatElapsed >= r.heartbeatTimeout {
		r.heartbeatElapsed = 0
		r.Step(pb.Message{From: r.localID, Type: pb.MessageType_MsgBeat})
	}
}

func (r *raft) Step(msg pb.Message) error {
	switch {
	case msg.Term == 0:
		// local message
	case msg.Term > r.term:
		if msg.Type == pb.MessageType_MsgVote {
			if r.leaderID != None && r.electionElapsed < r.electionTimeout {
				// If a server receives a RequestVote request within the minimum election timeout
				// of hearing from a current leader, it does not update its term or grant its vote
				r.logger.Info("ignored MsgVote",
					zap.Uint64("localID", r.localID),
					zap.Uint64("term", r.term),
					zap.Uint64("vote", r.vote),
					zap.Uint64("msgFrom", msg.From),
					zap.Uint64("msgLogTerm", msg.LogTerm),
					zap.Uint64("msgIndex", msg.Index),
					zap.Int("remainTime", r.electionTimeout-r.electionElapsed))
				return nil
			}
		}
		r.logger.Info("received a message with higher term",
			zap.Uint64("localID", r.localID),
			zap.Uint64("term", r.term),
			zap.String("msgType", msg.Type.String()),
			zap.Uint64("msgFrom", msg.From),
			zap.Uint64("msgTerm", msg.Term))
		if msg.Type == pb.MessageType_MsgApp || msg.Type == pb.MessageType_MsgHeartbeat {
			r.becomeFollower(msg.Term, msg.From)
		} else {
			r.becomeFollower(msg.Term, None)
		}

	case msg.Term < r.term:
		r.logger.Info("ignored a message with lower term",
			zap.Uint64("localID", r.localID),
			zap.Uint64("term", r.term),
			zap.String("msgType", msg.Type.String()),
			zap.Uint64("msgFrom", msg.From),
			zap.Uint64("msgTerm", msg.Term))
		return nil
	}

	switch msg.Type {
	case pb.MessageType_MsgHup:
		if r.state != StateLeader {
			r.logger.Info("starting a new election",
				zap.Uint64("localID", r.localID),
				zap.Uint64("term", r.term))
			r.campaign()
		} else {
			r.logger.Debug("ignoring MsgHup because already leader", zap.Uint64("localID", r.localID))
		}

	case pb.MessageType_MsgVote:
		// We can vote if this is a repeat of a vote we've already cast...
		canVote := r.vote == msg.From ||
			// ...we haven't voted and we don't think there's a leader yet in this termsg...
			(r.vote == None && r.leaderID == None)
		// ...and we believe the candidate is up to date.
		if canVote && r.raftLog.isUpToDate(msg.Index, msg.LogTerm) {
			r.logger.Info("receive MsgVote and vote granted",
				zap.Uint64("localID", r.localID),
				zap.Uint64("term", r.term),
				zap.Uint64("lastTerm", r.raftLog.lastTerm()),
				zap.Uint64("lastIndex", r.raftLog.lastIndex()),
				zap.Uint64("vote", r.vote),
				zap.String("msgType", msg.Type.String()),
				zap.Uint64("msgFrom", msg.From),
				zap.Uint64("msgTerm", msg.Term),
				zap.Uint64("msgIndex", msg.Index))
			r.send(pb.Message{To: msg.From, Term: msg.Term, Type: pb.MessageType_MsgVoteResp})
			if msg.Type == pb.MessageType_MsgVote {
				// Only record real votes.
				r.electionElapsed = 0
				r.vote = msg.From
			}
		} else {
			r.logger.Info("receive MsgVote and vote rejected",
				zap.Uint64("localID", r.localID),
				zap.Uint64("term", r.term),
				zap.Uint64("lastTerm", r.raftLog.lastTerm()),
				zap.Uint64("lastIndex", r.raftLog.lastIndex()),
				zap.Uint64("vote", r.vote),
				zap.String("msgType", msg.Type.String()),
				zap.Uint64("msgFrom", msg.From),
				zap.Uint64("msgTerm", msg.Term),
				zap.Uint64("msgIndex", msg.Index))
			r.send(pb.Message{To: msg.From, Term: r.term, Type: pb.MessageType_MsgVoteResp, Reject: true})
		}

	default:
		err := r.step(r, msg)
		if err != nil {
			return err
		}
	}
	return nil
}

func stepFollower(r *raft, msg pb.Message) error {
	switch msg.Type {
	case pb.MessageType_MsgProp:
		if r.leaderID == None {
			r.logger.Info("no leader at term; dropping proposal",
				zap.Uint64("localID", r.localID),
				zap.Uint64("term", r.term))
			return ErrProposalDropped
		}
		r.logger.Info("received MsgProp, redirect to leader",
			zap.Uint64("localID", r.localID),
			zap.Uint64("leaderID", r.leaderID),
			zap.Uint64("term", r.term))
		msg.To = r.leaderID
		r.send(msg)
	case pb.MessageType_MsgApp:
		r.logger.Debug("follower recv MsgApp")
		r.electionElapsed = 0
		r.leaderID = msg.From
		r.handleAppendEntries(msg)
	case pb.MessageType_MsgHeartbeat:
		r.electionElapsed = 0
		r.leaderID = msg.From
		r.handleHeartbeat(msg)
	}
	return nil
}

func stepCandidate(r *raft, msg pb.Message) error {
	switch msg.Type {
	case pb.MessageType_MsgProp:
		r.logger.Info("no leader at term %d; dropping proposal",
			zap.Uint64("localID", r.localID),
			zap.Uint64("term", r.term))
		return ErrProposalDropped
	case pb.MessageType_MsgApp:
		r.logger.Debug("candidate recv MsgApp")
		r.becomeFollower(msg.Term, msg.From) // always m.Term == r.Term
		r.handleAppendEntries(msg)
	case pb.MessageType_MsgHeartbeat:
		r.logger.Debug("candidate recv MsgHeartbeat")
		r.becomeFollower(msg.Term, msg.From) // always m.Term == r.Term
		r.handleHeartbeat(msg)
	case pb.MessageType_MsgVoteResp:
		r.logger.Info("received MsgVoteResp",
			zap.Uint64("localID", r.localID),
			zap.Uint64("term", r.term),
			zap.Uint64("msgFrom", msg.From),
			zap.Bool("msgReject", msg.Reject))
		r.prs.vote(msg.From, msg.Reject)
		if r.prs.isVoteWon() {
			r.becomeLeader()
			r.bcastAppend()
		} else if r.prs.isVoteReject() {
			r.becomeFollower(r.term, None)
		}
	}
	return nil
}

func stepLeader(r *raft, msg pb.Message) error {
	switch msg.Type {
	case pb.MessageType_MsgBeat:
		r.bcastHeartbeat()
		return nil
	case pb.MessageType_MsgProp:
		if len(msg.Entries) == 0 {
			r.logger.Panic("stepped empty MsgProp", zap.Uint64("localID", r.localID))
		}
		r.appendEntry(msg.Entries)
		r.bcastAppend()
		return nil
	}

	pr := r.prs.cluster[msg.From]
	if pr == nil {
		r.logger.Debug("no progress available",
			zap.Uint64("localID", r.localID),
			zap.Uint64("msgFrom", msg.From))
		return nil
	}
	switch msg.Type {
	case pb.MessageType_MsgAppResp:
		if msg.Reject {
			r.logger.Debug("received MsgAppResp(MsgApp was rejected, lastindex: %d) from %x for index %d",
				zap.Uint64("localID", r.localID),
				zap.Uint64("msgFrom", msg.From),
				zap.Uint64("msgIndex", msg.Index),
				zap.Uint64("msgRejectHint", msg.RejectHint))
			if r.prs.maybeDecrTo(msg.From, msg.Index, msg.RejectHint) {
				r.logger.Debug("decreased progress",
					zap.Uint64("localID", r.localID),
					zap.Uint64("msgFrom", msg.From),
					zap.String("to", pr.String()))
				r.sendAppend(msg.From)
			}
		} else {
			if r.prs.maybeUpdate(msg.From, msg.Index) && r.maybeCommit() {
				r.bcastAppend()
			}
		}
	case pb.MessageType_MsgHeartbeatResp:
		if pr.MatchIndex < r.raftLog.lastIndex() {
			r.sendAppend(msg.From)
		}
	}
	return nil
}

func (r *raft) appendEntry(es []*pb.Entry) {
	li := r.raftLog.lastIndex()
	r.logger.Debug("appendEntry raftLog.lastIndex", zap.Uint64("lastIndex", li))
	for i := range es {
		es[i].Term = r.term
		es[i].Index = li + 1 + uint64(i)
	}
	// use latest "last" index after truncate/append
	lastIndex := r.raftLog.append(es)
	r.logger.Debug("after appendEntry",
		zap.Uint64("committed", r.raftLog.committed),
		zap.Uint64("applied", r.raftLog.applied),
		zap.Uint64("lastLogIndex", r.raftLog.lastIndex()),
		zap.Uint64("lastLogTerm", r.raftLog.lastTerm()))
	r.prs.maybeUpdate(r.localID, lastIndex)
	// Regardless of maybeCommit's return, our caller will call bcastAppend.
	r.maybeCommit()
}

func (r *raft) maybeCommit() bool {
	mci := r.prs.commited()
	return r.raftLog.maybeCommit(mci, r.term)
}

func (r *raft) hasLeader() bool { return r.leaderID != None }

func (r *raft) hardState() pb.HardState {
	return pb.HardState{
		Term:   r.term,
		Vote:   r.vote,
		Commit: r.raftLog.committed,
	}
}

func (r *raft) campaign() {
	r.becomeCandidate()
	for _, id := range r.prs.PeersID {
		r.logger.Info("send RequestVote",
			zap.Uint64("localID", r.localID),
			zap.Uint64("to", id),
			zap.Uint64("term", r.term),
			zap.Uint64("lastTerm", r.raftLog.lastTerm()),
			zap.Uint64("lastIndex", r.raftLog.lastIndex()))
		r.send(pb.Message{Term: r.term, To: id, Type: pb.MessageType_MsgVote, Index: r.raftLog.lastIndex(), LogTerm: r.raftLog.lastTerm()})
	}
}

func (r *raft) send(msg pb.Message) {
	msg.From = r.localID
	if msg.Type == pb.MessageType_MsgVote || msg.Type == pb.MessageType_MsgVoteResp {
		if msg.Term == 0 {
			// All {pre-,}campaign messages need to have the term set when
			// sending.
			// - MsgVote: m.Term is the term the node is campaigning for,
			//   non-zero as we increment the term when campaigning.
			// - MsgVoteResp: m.Term is the new r.Term if the MsgVote was
			//   granted, non-zero for the same reason MsgVote is
			panic(fmt.Sprintf("term should be set when sending %s", msg.Type))
		}
	} else {
		if msg.Term != 0 {
			panic(fmt.Sprintf("term should not be set when sending %s (was %d)", msg.Type, msg.Term))
		}
		// do not attach term to MsgProp, MsgReadIndex
		// proposals are a way to forward to the leader and
		// should be treated as local message.
		// MsgReadIndex is also forwarded to leader.
		if msg.Type != pb.MessageType_MsgProp {
			msg.Term = r.term
		}
	}
	r.msgs = append(r.msgs, msg)
}

func (r *raft) bcastHeartbeat() {
	for _, id := range r.prs.PeersID {
		r.sendHeartbeat(id)
	}
}

func (r *raft) sendHeartbeat(to uint64) {
	// Attach the commit as min(to.matched, r.committed).
	// When the leader sends out heartbeat message,
	// the receiver(follower) might not be matched with the leader
	// or it might not have all the committed entries.
	// The leader MUST NOT forward the follower's commit to
	// an unmatched index.
	commit := min(r.prs.cluster[to].MatchIndex, r.raftLog.committed)
	m := pb.Message{
		To:     to,
		Type:   pb.MessageType_MsgHeartbeat,
		Commit: commit,
	}
	r.send(m)
}

func (r *raft) bcastAppend() {
	r.logger.Debug("bcastAppend",
		zap.Uint64("committed", r.raftLog.committed),
		zap.Uint64("applied", r.raftLog.applied),
		zap.Uint64("lastLogIndex", r.raftLog.lastIndex()),
		zap.Uint64("lastLogTerm", r.raftLog.lastTerm()))
	for _, id := range r.prs.PeersID {
		r.sendAppend(id)
	}
}

func (r *raft) sendAppend(to uint64) {
	r.maybeSendAppend(to, true)
}

func (r *raft) maybeSendAppend(to uint64, sendIfEmpty bool) bool {
	pr := r.prs.cluster[to]
	m := pb.Message{}
	m.To = to

	// TODO: 未处理的异常
	term, _ := r.raftLog.term(pr.NextIndex - 1)
	entries, _ := r.raftLog.tailEntriesFrom(pr.NextIndex)
	if len(entries) == 0 && !sendIfEmpty {
		return false
	}

	m.Type = pb.MessageType_MsgApp
	m.Index = pr.NextIndex - 1 // prevLogIndex
	m.LogTerm = term           // prevLogTerm
	m.Entries = entries        // nextIndex 开始往后所有的 entries
	m.Commit = r.raftLog.committed

	r.send(m)
	return true
}

func (r *raft) handleAppendEntries(msg pb.Message) {
	if msg.Index < r.raftLog.committed {
		r.logger.Debug("handleAppendEntries msg.Index < committed",
			zap.Uint64("msgIndex", msg.Index),
			zap.Uint64("committed", r.raftLog.committed))
		r.send(pb.Message{To: msg.From, Type: pb.MessageType_MsgAppResp, Index: r.raftLog.committed})
		return
	}

	if mlastIndex, ok := r.raftLog.maybeAppend(msg.Index, msg.LogTerm, msg.Commit, msg.Entries); ok {
		r.logger.Debug("handleAppendEntries append success",
			zap.Uint64("msgIndex", msg.Index),
			zap.Uint64("msgLogTerm", msg.LogTerm),
			zap.Uint64("msgCommitted", msg.Commit),
			zap.Uint64("committed", r.raftLog.committed),
			zap.Uint64("applied", r.raftLog.applied),
			zap.Uint64("lastLogIndex", r.raftLog.lastIndex()),
			zap.Uint64("lastLogTerm", r.raftLog.lastTerm()))
		r.send(pb.Message{To: msg.From, Type: pb.MessageType_MsgAppResp, Index: mlastIndex})
	} else {
		r.logger.Debug("rejected MsgApp",
			zap.Uint64("localID", r.localID),
			zap.Uint64("logTermAtMsgIndex", r.raftLog.termOrPanic(r.raftLog.term(msg.Index))),
			zap.Uint64("msgIndex", msg.Index),
			zap.Uint64("msgLogTerm", msg.LogTerm),
			zap.Uint64("msgFrom", msg.From))
		r.send(pb.Message{To: msg.From, Type: pb.MessageType_MsgAppResp, Index: msg.Index, Reject: true, RejectHint: r.raftLog.lastIndex()})
	}
}

func (r *raft) handleHeartbeat(msg pb.Message) {
	r.raftLog.commitTo(msg.Commit)
	r.send(pb.Message{To: msg.From, Type: pb.MessageType_MsgHeartbeatResp})
}

func (r *raft) advance(rd Ready) {
	// If entries were applied (or a snapshot), update our cursor for
	// the next Ready. Note that if the current HardState contains a
	// new Commit index, this does not mean that we're also applying
	// all of the new entries due to commit pagination by size.
	if index := rd.appliedCursor(); index > 0 {
		r.raftLog.appliedTo(index)
		r.logger.Debug("advance applied log",
			zap.Uint64("committed", r.raftLog.committed),
			zap.Uint64("applied", r.raftLog.applied),
			zap.Uint64("lastLogIndex", r.raftLog.lastIndex()),
			zap.Uint64("lastLogTerm", r.raftLog.lastTerm()))
	}
	if len(rd.Entries) > 0 {
		e := rd.Entries[len(rd.Entries)-1]
		r.raftLog.stableTo(e.Index, e.Term)
		r.logger.Debug("advance store unstable log",
			zap.Uint64("e.Index", e.Index),
			zap.Uint64("e.Term", e.Term),
			zap.Uint64("unstable", r.raftLog.unstable),
			zap.Uint64("lastLogIndex", r.raftLog.lastIndex()),
			zap.Uint64("lastLogTerm", r.raftLog.lastTerm()))
	}
}
