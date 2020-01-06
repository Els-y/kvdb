package raft

import (
	pb "github.com/Els-y/kvdb/rpc"
	"go.uber.org/zap"
	"reflect"
	"testing"
)

func TestFindConflict(t *testing.T) {
	previousEnts := []*pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}
	tests := []struct {
		ents      []*pb.Entry
		wconflict uint64
	}{
		// no conflict, empty ent
		{[]*pb.Entry{}, 0},
		// no conflict
		{[]*pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}, 0},
		{[]*pb.Entry{{Index: 2, Term: 2}, {Index: 3, Term: 3}}, 0},
		{[]*pb.Entry{{Index: 3, Term: 3}}, 0},
		// no conflict, but has new entries
		{[]*pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		{[]*pb.Entry{{Index: 2, Term: 2}, {Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		{[]*pb.Entry{{Index: 3, Term: 3}, {Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		{[]*pb.Entry{{Index: 4, Term: 4}, {Index: 5, Term: 4}}, 4},
		// conflicts with existing entries
		{[]*pb.Entry{{Index: 1, Term: 4}, {Index: 2, Term: 4}}, 1},
		{[]*pb.Entry{{Index: 2, Term: 1}, {Index: 3, Term: 4}, {Index: 4, Term: 4}}, 2},
		{[]*pb.Entry{{Index: 3, Term: 1}, {Index: 4, Term: 2}, {Index: 5, Term: 4}, {Index: 6, Term: 4}}, 3},
	}

	for i, tt := range tests {
		raftLog := NewLogStore(zap.NewExample())
		raftLog.append(previousEnts)

		gconflict := raftLog.findConflict(tt.ents)
		if gconflict != tt.wconflict {
			t.Errorf("#%d: conflict = %d, want %d", i, gconflict, tt.wconflict)
		}
	}
}

func TestIsUpToDate(t *testing.T) {
	previousEnts := []*pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}
	raftLog := NewLogStore(zap.NewExample())
	raftLog.append(previousEnts)
	tests := []struct {
		lastIndex uint64
		term      uint64
		wUpToDate bool
	}{
		// greater term, ignore lastIndex
		{raftLog.lastIndex() - 1, 4, true},
		{raftLog.lastIndex(), 4, true},
		{raftLog.lastIndex() + 1, 4, true},
		// smaller term, ignore lastIndex
		{raftLog.lastIndex() - 1, 2, false},
		{raftLog.lastIndex(), 2, false},
		{raftLog.lastIndex() + 1, 2, false},
		// equal term, equal or lager lastIndex wins
		{raftLog.lastIndex() - 1, 3, false},
		{raftLog.lastIndex(), 3, true},
		{raftLog.lastIndex() + 1, 3, true},
	}

	for i, tt := range tests {
		gUpToDate := raftLog.isUpToDate(tt.lastIndex, tt.term)
		if gUpToDate != tt.wUpToDate {
			t.Errorf("#%d: uptodate = %v, want %v", i, gUpToDate, tt.wUpToDate)
		}
	}
}

func TestAppend(t *testing.T) {
	previousEnts := []*pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}}
	tests := []struct {
		ents      []*pb.Entry
		windex    uint64
		wents     []*pb.Entry
		wunstable uint64
	}{
		{
			[]*pb.Entry{},
			2,
			[]*pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}},
			3,
		},
		{
			[]*pb.Entry{{Index: 3, Term: 2}},
			3,
			[]*pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 2}},
			3,
		},
		// conflicts with index 1
		{
			[]*pb.Entry{{Index: 1, Term: 2}},
			1,
			[]*pb.Entry{{Index: 1, Term: 2}},
			1,
		},
		// conflicts with index 2
		{
			[]*pb.Entry{{Index: 2, Term: 3}, {Index: 3, Term: 3}},
			3,
			[]*pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 3}, {Index: 3, Term: 3}},
			2,
		},
	}

	for i, tt := range tests {
		raftLog := NewLogStore(zap.NewExample())
		raftLog.append(previousEnts)
		raftLog.stableTo(2, 2)

		index := raftLog.append(tt.ents)
		if index != tt.windex {
			t.Errorf("#%d: lastIndex = %d, want %d", i, index, tt.windex)
		}
		g, err := raftLog.tailEntriesFrom(1)
		if err != nil {
			t.Fatalf("#%d: unexpected error %v", i, err)
		}
		if !reflect.DeepEqual(g, tt.wents) {
			t.Errorf("#%d: logEnts = %+v, want %+v", i, g, tt.wents)
		}
		if goff := raftLog.unstable; goff != tt.wunstable {
			t.Errorf("#%d: unstable = %d, want %d", i, goff, tt.wunstable)
		}
	}
}

// TestLogMaybeAppend ensures:
// If the given (index, term) matches with the existing log:
// 	1. If an existing entry conflicts with a new one (same index
// 	but different terms), delete the existing entry and all that
// 	follow it
// 	2.Append any new entries not already in the log
// If the given (index, term) does not match with the existing log:
// 	return false
func TestLogMaybeAppend(t *testing.T) {
	previousEnts := []*pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}, {Index: 3, Term: 3}}
	lastindex := uint64(3)
	lastterm := uint64(3)
	commit := uint64(1)

	tests := []struct {
		logTerm   uint64
		index     uint64
		committed uint64
		ents      []*pb.Entry

		wlasti  uint64
		wappend bool
		wcommit uint64
		wpanic  bool
	}{
		// not match: term is different
		{
			lastterm - 1, lastindex, lastindex, []*pb.Entry{{Index: lastindex + 1, Term: 4}},
			0, false, commit, false,
		},
		// not match: index out of bound
		{
			lastterm, lastindex + 1, lastindex, []*pb.Entry{{Index: lastindex + 2, Term: 4}},
			0, false, commit, false,
		},
		// match with the last existing entry
		{
			lastterm, lastindex, lastindex, nil,
			lastindex, true, lastindex, false,
		},
		{
			lastterm, lastindex, lastindex + 1, nil,
			lastindex, true, lastindex, false, // do not increase commit higher than lastnewi
		},
		{
			lastterm, lastindex, lastindex - 1, nil,
			lastindex, true, lastindex - 1, false, // commit up to the commit in the message
		},
		{
			lastterm, lastindex, 0, nil,
			lastindex, true, commit, false, // commit do not decrease
		},
		{
			0, 0, lastindex, nil,
			0, true, commit, false, // commit do not decrease
		},
		{
			lastterm, lastindex, lastindex, []*pb.Entry{{Index: lastindex + 1, Term: 4}},
			lastindex + 1, true, lastindex, false,
		},
		{
			lastterm, lastindex, lastindex + 1, []*pb.Entry{{Index: lastindex + 1, Term: 4}},
			lastindex + 1, true, lastindex + 1, false,
		},
		{
			lastterm, lastindex, lastindex + 2, []*pb.Entry{{Index: lastindex + 1, Term: 4}},
			lastindex + 1, true, lastindex + 1, false, // do not increase commit higher than lastnewi
		},
		{
			lastterm, lastindex, lastindex + 2, []*pb.Entry{{Index: lastindex + 1, Term: 4}, {Index: lastindex + 2, Term: 4}},
			lastindex + 2, true, lastindex + 2, false,
		},
		// match with the the entry in the middle
		{
			lastterm - 1, lastindex - 1, lastindex, []*pb.Entry{{Index: lastindex, Term: 4}},
			lastindex, true, lastindex, false,
		},
		{
			lastterm - 2, lastindex - 2, lastindex, []*pb.Entry{{Index: lastindex - 1, Term: 4}},
			lastindex - 1, true, lastindex - 1, false,
		},
		{
			lastterm - 3, lastindex - 3, lastindex, []*pb.Entry{{Index: lastindex - 2, Term: 4}},
			lastindex - 2, true, lastindex - 2, true, // conflict with existing committed entry
		},
		{
			lastterm - 2, lastindex - 2, lastindex, []*pb.Entry{{Index: lastindex - 1, Term: 4}, {Index: lastindex, Term: 4}},
			lastindex, true, lastindex, false,
		},
	}

	for i, tt := range tests {
		raftLog := NewLogStore(zap.NewExample())
		raftLog.append(previousEnts)
		raftLog.committed = commit
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()
			glasti, gappend := raftLog.maybeAppend(tt.index, tt.logTerm, tt.committed, tt.ents)
			gcommit := raftLog.committed

			if glasti != tt.wlasti {
				t.Errorf("#%d: lastindex = %d, want %d", i, glasti, tt.wlasti)
			}
			if gappend != tt.wappend {
				t.Errorf("#%d: append = %v, want %v", i, gappend, tt.wappend)
			}
			if gcommit != tt.wcommit {
				t.Errorf("#%d: committed = %d, want %d", i, gcommit, tt.wcommit)
			}
			if gappend && len(tt.ents) != 0 {
				gents, err := raftLog.slice(raftLog.lastIndex()-uint64(len(tt.ents))+1, raftLog.lastIndex()+1)
				if err != nil {
					t.Fatalf("unexpected error %v", err)
				}
				if !reflect.DeepEqual(tt.ents, gents) {
					t.Errorf("#%d: appended entries = %v, want %v", i, gents, tt.ents)
				}
			}
		}()
	}
}

func TestHasNextEnts(t *testing.T) {
	ents := []*pb.Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
		{Term: 1, Index: 3},
		{Term: 1, Index: 4},
		{Term: 1, Index: 5},
		{Term: 1, Index: 6},
	}
	tests := []struct {
		applied uint64
		hasNext bool
	}{
		{0, true},
		{3, true},
		{4, true},
		{5, false},
	}
	for i, tt := range tests {
		raftLog := NewLogStore(zap.NewExample())
		raftLog.append(ents)
		raftLog.stableTo(3, 1)
		raftLog.maybeCommit(5, 1)
		raftLog.appliedTo(tt.applied)

		hasNext := raftLog.hasNextEntries()
		if hasNext != tt.hasNext {
			t.Errorf("#%d: hasNext = %v, want %v", i, hasNext, tt.hasNext)
		}
	}
}

func TestNextEnts(t *testing.T) {
	ents := []*pb.Entry{
		{Term: 1, Index: 1},
		{Term: 1, Index: 2},
		{Term: 1, Index: 3},
		{Term: 1, Index: 4},
		{Term: 1, Index: 5},
		{Term: 1, Index: 6},
	}
	tests := []struct {
		applied uint64
		wents   []*pb.Entry
	}{
		{0, ents[0:5]},
		{3, ents[3:5]},
		{4, ents[4:5]},
		{5, nil},
	}
	for i, tt := range tests {
		raftLog := NewLogStore(zap.NewExample())
		raftLog.append(ents)
		raftLog.stableTo(3, 1)
		raftLog.maybeCommit(5, 1)
		raftLog.appliedTo(tt.applied)

		nents := raftLog.nextEntries()
		if !reflect.DeepEqual(nents, tt.wents) {
			t.Errorf("#%d: nents = %+v, want %+v", i, nents, tt.wents)
		}
	}
}

// TestUnstableEnts ensures unstableEntries returns the unstable part of the
// entries correctly.
func TestUnstableEnts(t *testing.T) {
	previousEnts := []*pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}}
	tests := []struct {
		unstable uint64
		wents    []*pb.Entry
	}{
		{3, nil},
		{1, previousEnts},
	}

	for i, tt := range tests {
		raftLog := NewLogStore(zap.NewExample())
		raftLog.append(previousEnts)
		raftLog.unstable = tt.unstable

		ents := raftLog.unstableEntries()
		if l := len(ents); l > 0 {
			raftLog.stableTo(ents[l-1].Index, ents[l-1].Term)
		}
		if !reflect.DeepEqual(ents, tt.wents) {
			t.Errorf("#%d: unstableEnts = %+v, want %+v", i, ents, tt.wents)
		}
		w := previousEnts[len(previousEnts)-1].Index + 1
		if g := raftLog.unstable; g != w {
			t.Errorf("#%d: unstable = %d, want %d", i, g, w)
		}
	}
}

func TestCommitTo(t *testing.T) {
	previousEnts := []*pb.Entry{{Term: 1, Index: 1}, {Term: 2, Index: 2}, {Term: 3, Index: 3}}
	commit := uint64(2)
	tests := []struct {
		commit  uint64
		wcommit uint64
		wpanic  bool
	}{
		{3, 3, false},
		{1, 2, false}, // never decrease
		{4, 0, true},  // commit out of range -> panic
	}
	for i, tt := range tests {
		func() {
			defer func() {
				if r := recover(); r != nil {
					if !tt.wpanic {
						t.Errorf("%d: panic = %v, want %v", i, true, tt.wpanic)
					}
				}
			}()
			raftLog := NewLogStore(zap.NewExample())
			raftLog.append(previousEnts)
			raftLog.committed = commit
			raftLog.commitTo(tt.commit)
			if raftLog.committed != tt.wcommit {
				t.Errorf("#%d: committed = %d, want %d", i, raftLog.committed, tt.wcommit)
			}
		}()
	}
}

func TestStableTo(t *testing.T) {
	tests := []struct {
		stablei   uint64
		stablet   uint64
		wunstable uint64
	}{
		{1, 1, 2},
		{2, 2, 3},
		{2, 1, 0}, // bad term
		{3, 1, 0}, // bad index
	}
	for i, tt := range tests {
		raftLog := NewLogStore(zap.NewExample())
		raftLog.append([]*pb.Entry{{Index: 1, Term: 1}, {Index: 2, Term: 2}})
		raftLog.stableTo(tt.stablei, tt.stablet)
		if raftLog.unstable != tt.wunstable {
			t.Errorf("#%d: unstable = %d, want %d", i, raftLog.unstable, tt.wunstable)
		}
	}
}
