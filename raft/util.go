package raft

import (
	pb "github.com/Els-y/kvdb/rpc"
)

func IsLocalMsg(msgt pb.MessageType) bool {
	return msgt == pb.MessageType_MsgHup || msgt == pb.MessageType_MsgBeat
}

func min(a, b uint64) uint64 {
	if a > b {
		return b
	}
	return a
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}
