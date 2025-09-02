package raft

import (
	"context"
	"fmt"
	"log"

	raftx "github.com/voyager-db/raftx"
	raftpb "github.com/voyager-db/raftx/raftpb"
)

// Ready is the minimal surface our app consumes (raftx types).
type Ready struct {
	HardState raftpb.HardState
	Entries   []raftpb.Entry
	Snapshot  raftpb.Snapshot
	Messages  []raftpb.Message
	Committed []raftpb.Entry
}

// nodeImpl wraps raftx.Node and persists Ready() to WAL and memory storage.
type nodeImpl struct {
	n       raftx.Node
	storage *raftx.MemoryStorage
	wal     *WAL
	ready   chan Ready
}

// NewNode starts a raft node; classic by default, Fast/C-Raft if enabled in cfg.
func NewNode(cfg Config, peers []raftx.Peer) Node {
	return NewNodeWithWAL(cfg, peers, nil)
}

func NewNodeWithWAL(cfg Config, peers []raftx.Peer, wal *WAL) Node {
	ms := raftx.NewMemoryStorage()
	// Rebuild from WAL if present (best effort)
	if wal != nil {
		if err := wal.RestoreToMemory(ms); err != nil {
			log.Printf("raft: WAL restore failed (continuing in-memory): %v", err)
		}
	}

	rc := &raftx.Config{
		ID:                  cfg.ID,
		ElectionTick:        cfg.ElectionTick,
		HeartbeatTick:       cfg.HeartbeatTick,
		Storage:             ms,
		MaxSizePerMsg:       cfg.MaxSizePerMsg,
		MaxInflightMsgs:     cfg.MaxInflightMsgs,
		EnableFastPath:      cfg.EnableFastPath,
		EnableCRaft:         cfg.EnableCRaft,
		GlobalElectionTick:  cfg.GlobalElectionTick,
		GlobalHeartbeatTick: cfg.GlobalHeartbeatTick,
		GlobalVoters:        cfg.GlobalVoters,
	}
	n := raftx.StartNode(rc, peers)
	ni := &nodeImpl{n: n, storage: ms, wal: wal, ready: make(chan Ready, 64)}
	go ni.pump()
	return ni
}

func (c *nodeImpl) pump() {
	for rd := range c.n.Ready() {
		// Persist to memory storage first.
		if err := c.persistToMemory(rd); err != nil {
			log.Panicf("raft: persist-to-memory failed: %v", err)
		}
		// Persist to WAL if configured.
		if c.wal != nil {
			if err := c.wal.SaveReady(rd); err != nil {
				log.Panicf("raft: WAL SaveReady failed: %v", err)
			}
		}

		// Only after successful persistence, hand the simplified Ready to the app.
		c.ready <- Ready{
			HardState: rd.HardState,
			Entries:   rd.Entries,
			Snapshot:  rd.Snapshot,
			Messages:  rd.Messages,
			Committed: rd.CommittedEntries,
		}
	}
}

func (c *nodeImpl) persistToMemory(rd raftx.Ready) error {
	if rd.Snapshot.Metadata.Index != 0 {
		if err := c.storage.ApplySnapshot(rd.Snapshot); err != nil {
			return fmt.Errorf("apply snapshot: %w", err)
		}
	}
	if len(rd.Entries) > 0 {
		if err := c.storage.Append(rd.Entries); err != nil {
			return fmt.Errorf("append entries: %w", err)
		}
	}
	if !(rd.HardState.Term == 0 && rd.HardState.Vote == 0 && rd.HardState.Commit == 0) {
		if err := c.storage.SetHardState(rd.HardState); err != nil {
			return fmt.Errorf("set hardstate: %w", err)
		}
	}
	return nil
}

// Node interface impl
func (c *nodeImpl) Tick()                  { c.n.Tick() }
func (c *nodeImpl) Ready() <-chan Ready    { return c.ready }
func (c *nodeImpl) Advance()               { c.n.Advance() }
func (c *nodeImpl) Propose(b []byte) error { return c.n.Propose(context.Background(), b) }
func (c *nodeImpl) ProposeConfChange(cc raftpb.ConfChange) error {
	return c.n.ProposeConfChange(context.Background(), cc)
}
func (c *nodeImpl) ApplyConfChange(cc raftpb.ConfChange) error { c.n.ApplyConfChange(cc); return nil }
func (c *nodeImpl) Step(m raftpb.Message) error                { return c.n.Step(context.Background(), m) }
func (c *nodeImpl) Stop()                                      { c.n.Stop() }
func (c *nodeImpl) Storage() Storage                           { return c.storage } // NEW
