package raft

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestInitPeerProgress(t *testing.T) {
	peersID := []uint64{2, 3}
	prs := initProgress(1, peersID)
	for _, id := range peersID {
		assert.False(t, prs.cluster[id].Voted)
		assert.False(t, prs.cluster[id].Granted)
	}
}

func TestIsVoteWon(t *testing.T) {
	tests := []struct {
		giveMembers int
		giveVoted   int
		wantWon     bool
	}{
		{
			giveMembers: 2,
			giveVoted:   2,
			wantWon:     true,
		},
		{
			giveMembers: 3,
			giveVoted:   1,
			wantWon:     false,
		},
		{
			giveMembers: 3,
			giveVoted:   2,
			wantWon:     true,
		},
		{
			giveMembers: 4,
			giveVoted:   2,
			wantWon:     false,
		},
		{
			giveMembers: 4,
			giveVoted:   3,
			wantWon:     true,
		},
		{
			giveMembers: 5,
			giveVoted:   2,
			wantWon:     false,
		},
		{
			giveMembers: 5,
			giveVoted:   3,
			wantWon:     true,
		},
	}
	for _, tt := range tests {
		t.Run(fmt.Sprintf("Members:%d,Voted:%d", tt.giveMembers, tt.giveVoted), func(t *testing.T) {
			var peersID []uint64
			for i := 2; i <= tt.giveMembers; i++ {
				peersID = append(peersID, uint64(i))
			}
			prs := initProgress(1, peersID)
			for i := 1; i <= tt.giveVoted; i++ {
				prs.vote(uint64(i), false)
			}
			assert.Equal(t, tt.wantWon, prs.isVoteWon())
		})
	}
}
