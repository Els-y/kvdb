package wal

import (
	"github.com/Els-y/kvdb/raft"
	pb "github.com/Els-y/kvdb/rpc"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"os"
	"testing"
)

func TestWAL_Save(t *testing.T) {
	entries1 := []*pb.Entry{{}, {Term: 1, Index: 1}}
	st1 := pb.HardState{Term: 1, Vote: 1, Commit: 1}

	w1 := Create("test", zap.NewExample())
	w1.Save(st1, entries1)

	w2 := Restore("test", zap.NewExample())
	st2, entries2, err := w2.ReadAll()
	assert.Nil(t, err)
	assert.True(t, raft.IsHardStateEqual(st1, st2))
	for i, v := range entries1 {
		assert.True(t, isEntryEqual(v, entries2[i]),
			"i= %d, e1.index=%d, e1.term=%d, e2.index=%d, e2.term=%d", i, v.Index, v.Term, entries2[i].Index, entries2[i].Term)
	}

	err = os.RemoveAll("test")
	assert.Nil(t, err)
}

func isEntryEqual(a, b *pb.Entry) bool {
	return a.Term == b.Term && a.Index == b.Index
}
