package raft

import raftpb "github.com/voyager-db/raftx/raftpb"

type Node interface {
	Storage() Storage

	Tick()
	Ready() <-chan Ready
	Advance()
	Propose(data []byte) error
	ProposeConfChange(cc raftpb.ConfChange) error
	ApplyConfChange(cc raftpb.ConfChange) error
	Step(m raftpb.Message) error
	Stop()
}

type Config struct {
	ID              uint64
	ElectionTick    int
	HeartbeatTick   int
	MaxSizePerMsg   uint64
	MaxInflightMsgs int

	// Optional fast/CRaft knobs (honored by raftx)
	EnableFastPath      bool
	EnableCRaft         bool
	GlobalElectionTick  int
	GlobalHeartbeatTick int
	GlobalVoters        []uint64
}

type Storage interface {
	CreateSnapshot(index uint64, cs *raftpb.ConfState, data []byte) (raftpb.Snapshot, error)
	Compact(compactIndex uint64) error
	LastIndex() (uint64, error)
	FirstIndex() (uint64, error)
}
