package raft

import (
	"errors"
	pb "github.com/Els-y/kvdb/rpc"
	"go.uber.org/zap"
)

var ErrUnavailable = errors.New("requested entry at index is unavailable")

type LogStore struct {
	committed uint64
	applied   uint64
	unstable  uint64 // offset to first unstable
	entries   []*pb.Entry
	logger    *zap.Logger
}

// TODO: 从 WAL 中恢复 log
func NewLogStore(logger *zap.Logger) *LogStore {
	l := &LogStore{
		committed: 0,
		applied:   0,
		unstable:  0,
		logger:    logger,
		entries:   make([]*pb.Entry, 1),
	}
	l.entries[0] = &pb.Entry{}
	return l
}

func (l *LogStore) Restore(entries []*pb.Entry) {
	if len(entries) == 0 {
		l.logger.Panic("restore raftLog with empty entries")
	}
	l.unstable = entries[len(entries)-1].Index + 1
	l.entries = entries
}

func (l *LogStore) maybeAppend(prevLogIndex, prevLogTerm, leaderCommit uint64, entries []*pb.Entry) (uint64, bool) {
	l.logger.Debug("call maybeAppend",
		zap.Uint64("prevLogIndex", prevLogIndex),
		zap.Uint64("prevLogTerm", prevLogTerm),
		zap.Uint64("leaderCommit", leaderCommit))
	if l.matchTerm(prevLogIndex, prevLogTerm) {
		lastNewIndex := prevLogIndex + uint64(len(entries))
		ci := l.findConflict(entries)
		switch {
		case ci == 0:
		case ci <= l.committed:
			l.logger.Panic("entry conflict with committed entry",
				zap.Uint64("conflictEntry", ci),
				zap.Uint64("committed", l.committed))
		default:
			offset := prevLogIndex + 1
			l.append(entries[ci-offset:])
		}
		l.commitTo(min(leaderCommit, lastNewIndex))
		return lastNewIndex, true
	}
	return 0, false
}

func (l *LogStore) append(entries []*pb.Entry) uint64 {
	if len(entries) == 0 {
		return l.lastIndex()
	}
	if after := entries[0].Index - 1; after < l.committed {
		l.logger.Panic("after is out of range",
			zap.Uint64("after", after),
			zap.Uint64("commited", l.committed))
	}
	l.truncateAndAppend(entries)
	return l.lastIndex()
}

func (l *LogStore) truncateAndAppend(entries []*pb.Entry) {
	after := entries[0].Index
	switch {
	case after == l.lastIndex()+1:
		// after is the next index in the u.entries
		// directly append
		l.entries = append(l.entries, entries...)
	case after <= l.unstable:
		l.logger.Info("replace the unstable entries from index",
			zap.Uint64("index", after))
		// The log is being truncated to before our current offset
		// portion, so set the offset and replace the entries
		l.unstable = after
		tmpEntries, err := l.slice(l.firstIndex(), after)
		if err != nil {
			l.logger.Panic("truncateAndAppend out of bound",
				zap.Uint64("firstIndex", l.firstIndex()),
				zap.Uint64("after", after))
		}
		l.entries = append(tmpEntries, entries...)
	default:
		// truncate to after and copy to u.entries
		// then append
		l.logger.Info("truncate the unstable entries before index",
			zap.Uint64("index", after))
		tmpEntries, err := l.slice(l.unstable, after)
		if err != nil {
			l.logger.Panic("truncateAndAppend out of bound",
				zap.Uint64("firstIndex", l.firstIndex()),
				zap.Uint64("after", after))
		}
		l.entries = append([]*pb.Entry{}, tmpEntries...)
		l.entries = append(l.entries, entries...)
	}
}

// findConflict finds the index of the conflict.
// It returns the first pair of conflicting entries between the existing
// entries and the given entries, if there are any.
// If there is no conflicting entries, and the existing entries contains
// all the given entries, zero will be returned.
// If there is no conflicting entries, but the given entries contains new
// entries, the index of the first new entry will be returned.
// An entry is considered to be conflicting if it has the same index but
// a different term.
// The first entry MUST have an index equal to the argument 'from'.
// The index of the given entries MUST be continuously increasing.
func (l *LogStore) findConflict(entries []*pb.Entry) uint64 {
	// TODO: 会有第0个冲突么？
	for _, ne := range entries {
		if !l.matchTerm(ne.Index, ne.Term) {
			if ne.Index <= l.lastIndex() {
				l.logger.Info("found conflict at index %d [existing term: %d, conflicting term: %d]",
					zap.Uint64("conflictIndex", ne.Index),
					zap.Uint64("existTerm", l.termOrPanic(l.term(ne.Index))),
					zap.Uint64("conflictTerm", ne.Term))
			}
			return ne.Index
		}
	}
	return 0
}

func (l *LogStore) isUpToDate(lastIndex, lastTerm uint64) bool {
	return lastTerm > l.lastTerm() || (lastTerm == l.lastTerm() && lastIndex >= l.lastIndex())
}

func (l *LogStore) matchTerm(i, term uint64) bool {
	t, err := l.term(i)
	if err != nil {
		return false
	}
	return t == term
}

func (l *LogStore) term(index uint64) (uint64, error) {
	if index < 0 {
		return 0, ErrUnavailable
	} else if index > l.lastIndex() {
		// TODO: need error?
		return 0, nil
	}
	return l.entries[index].Term, nil
}

func (l *LogStore) termOrPanic(t uint64, err error) uint64 {
	if err == nil {
		return t
	}
	l.logger.Panic("unexpected error",
		zap.Error(err))
	return 0
}

func (l *LogStore) firstIndex() uint64 {
	return l.entries[0].Index
}

func (l *LogStore) lastIndex() uint64 {
	return l.entries[0].Index + uint64(len(l.entries)) - 1
}

func (l *LogStore) lastTerm() uint64 {
	t, err := l.term(l.lastIndex())
	if err != nil {
		l.logger.Panic("unexpected error when getting the last term", zap.Error(err))
	}
	return t
}

// return [lo, hi)
func (l *LogStore) slice(lo, hi uint64) ([]*pb.Entry, error) {
	err := l.mustCheckOutOfBounds(lo, hi)
	if err != nil {
		return nil, err
	}
	if lo == hi {
		return nil, nil
	}
	return l.entries[lo:hi], nil
}

func (l *LogStore) mustCheckOutOfBounds(lo, hi uint64) error {
	if lo > hi {
		l.logger.Panic("invalid slice lo > hi",
			zap.Uint64("lo", lo),
			zap.Uint64("hi", hi))
		return ErrUnavailable
	}
	if lo < l.firstIndex() || hi > l.lastIndex()+1 {
		l.logger.Panic("slice out of bound",
			zap.Uint64("lo", lo),
			zap.Uint64("hi", hi),
			zap.Uint64("firstIndex", l.firstIndex()),
			zap.Uint64("lastIndex", l.lastIndex()))
		return ErrUnavailable
	}
	return nil
}

func (l *LogStore) tailEntriesFrom(i uint64) ([]*pb.Entry, error) {
	return l.slice(i, l.lastIndex()+1)
}

func (l *LogStore) unstableEntries() []*pb.Entry {
	entries, err := l.slice(l.unstable, l.lastIndex()+1)
	if err != nil {
		l.logger.Panic("unexpected error when getting unstable entries", zap.Error(err))
	}
	return entries
}

func (l *LogStore) hasNextEntries() bool {
	return l.committed > l.applied
}

func (l *LogStore) nextEntries() []*pb.Entry {
	if !l.hasNextEntries() {
		return nil
	}
	entries, err := l.slice(l.applied+1, l.committed+1)
	if err != nil {
		l.logger.Panic("unexpected error when getting unapplied entries", zap.Error(err))
	}
	return entries
}

func (l *LogStore) maybeCommit(maxIndex, term uint64) bool {
	if maxIndex > l.committed && l.termOrPanic(l.term(maxIndex)) == term {
		l.commitTo(maxIndex)
		return true
	}
	return false
}

func (l *LogStore) commitTo(toCommit uint64) {
	if l.committed < toCommit {
		if l.lastIndex() < toCommit {
			l.logger.Panic("toCommit is out of range. Was the raft log corrupted, truncated, or lost?",
				zap.Uint64("toCommit", toCommit),
				zap.Uint64("lastIndex", l.lastIndex()))
		}
		l.committed = toCommit
	}
}

func (l *LogStore) appliedTo(toApplied uint64) {
	if toApplied == 0 {
		return
	}
	if l.committed < toApplied || toApplied < l.applied {
		l.logger.Panic("applied is out of range [prevApplied(%d), committed(%d)]",
			zap.Uint64("toApplied", toApplied),
			zap.Uint64("prevApplied", l.applied),
			zap.Uint64("prevCommitted", l.committed))
	}
	l.applied = toApplied
}

func (l *LogStore) stableTo(index, term uint64) {
	if l.matchTerm(index, term) && index >= l.unstable {
		l.unstable = index + 1
	}
}
