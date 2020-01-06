package raft

import (
	"fmt"
)

type Member struct {
	MatchIndex uint64
	NextIndex  uint64

	Voted   bool // 区分有无投票
	Granted bool // true 表示同意，false 表示拒绝
}

type Progress struct {
	LocalID uint64
	PeersID []uint64
	cluster map[uint64]*Member
}

func (m *Member) String() string {
	return fmt.Sprintf("match=%d, next=%d", m.MatchIndex, m.NextIndex)
}

func initProgress(localID uint64, peersID []uint64) *Progress {
	prs := &Progress{
		LocalID: localID,
		PeersID: peersID,
		cluster: make(map[uint64]*Member, len(peersID)+1),
	}
	prs.cluster[localID] = &Member{
		MatchIndex: 0,
		NextIndex:  1,
	}
	for _, id := range peersID {
		prs.cluster[id] = &Member{
			MatchIndex: 0,
			NextIndex:  1,
		}
	}
	prs.resetVotes()
	return prs
}

func (prs *Progress) resetForLeader(lastLogIndex uint64) {
	for id, m := range prs.cluster {
		m.MatchIndex = 0
		m.NextIndex = lastLogIndex + 1
		m.Voted = false
		m.Granted = false
		if id == prs.LocalID {
			m.MatchIndex = lastLogIndex
		}
	}
}

func (prs *Progress) resetVotes() {
	for _, v := range prs.cluster {
		v.Voted = false
		v.Granted = false
	}
}

func (prs *Progress) vote(id uint64, reject bool) {
	prs.cluster[id].Voted = true
	prs.cluster[id].Granted = !reject
}

func (prs *Progress) isVoteWon() bool {
	granted, _ := prs.tallyVotes()
	return granted >= (len(prs.cluster)/2 + 1)
}

func (prs *Progress) isVoteReject() bool {
	_, rejected := prs.tallyVotes()
	return rejected >= (len(prs.cluster)/2 + 1)
}

func (prs *Progress) tallyVotes() (granted int, rejected int) {
	for _, v := range prs.cluster {
		if !v.Voted {
			continue
		}
		if v.Granted {
			granted++
		} else {
			rejected++
		}
	}
	return
}

func (prs *Progress) maybeUpdate(id, n uint64) bool {
	var updated bool
	m := prs.cluster[id]
	if m.MatchIndex < n {
		m.MatchIndex = n
		updated = true
	}
	if m.NextIndex < n+1 {
		m.NextIndex = n + 1
	}
	return updated
}

func (prs *Progress) maybeDecrTo(id, rejected, last uint64) bool {
	m := prs.cluster[id]
	// TODO: why?
	if m.NextIndex-1 != rejected {
		return false
	}

	// TODO: why? 什么情况会 m.NextIndex < 1？第 0 个为空的无用 log
	if m.NextIndex = min(rejected, last+1); m.NextIndex < 1 {
		m.NextIndex = 1
	}
	return true
}

func (prs *Progress) commited() uint64 {
	for n := prs.cluster[prs.LocalID].MatchIndex; n >= 0; n-- {
		count := 0
		for _, m := range prs.cluster {
			if m.MatchIndex >= n {
				count++
			}
			if count >= (len(prs.cluster)/2 + 1) {
				return n
			}
		}
	}
	return 0
}
