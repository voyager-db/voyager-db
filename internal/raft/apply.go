package raft

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"sync"
	"sync/atomic"
	"time"

	eraftpb "github.com/voyager-db/raftx/raftpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/voyager-db/voyager-db/internal/cluster"
	"github.com/voyager-db/voyager-db/internal/lease"
	"github.com/voyager-db/voyager-db/internal/mvcc"
)

// Result types returned to the KV layer after apply.
type PutResult struct{ Prev *mvccpb.KeyValue }
type DeleteResult struct {
	Deleted int64
	PrevKvs []*mvccpb.KeyValue
}
type result struct {
	put *PutResult
	del *DeleteResult
	err error
}

type Applier struct {
	Store  mvcc.KV
	Leases lease.ManagerAPI
	Hub    *mvcc.WatchHub
}

type Proposer struct {
	n         Node
	app       *Applier
	transport *Transport
	cluster   *cluster.Manager // NEW: to mutate membership on apply

	mu     sync.Mutex
	waiter map[uint64]chan result
	nextID uint64

	// waiters for conf changes (keyed by NodeID)
	confMu   sync.Mutex
	confWait map[uint64]chan error

	stopped chan struct{}

	lastTerm, lastCommit, leaderID uint64
	lastApplied, lastSnap          uint64

	snapEveryN, compactKeepN uint64
}

func NewProposer(n Node, app *Applier, tr *Transport, snapEveryN, compactKeepN uint64, mgr *cluster.Manager) *Proposer {
	if snapEveryN == 0 {
		snapEveryN = 10000
	}
	if compactKeepN == 0 {
		compactKeepN = 5000
	}
	return &Proposer{
		n:            n,
		app:          app,
		transport:    tr,
		cluster:      mgr,
		waiter:       make(map[uint64]chan result),
		confWait:     make(map[uint64]chan error),
		stopped:      make(chan struct{}),
		snapEveryN:   snapEveryN,
		compactKeepN: compactKeepN,
	}
}

func (p *Proposer) Run(ctx context.Context) error {
	defer close(p.stopped)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			p.n.Tick()
		case rd := <-p.n.Ready():
			if rd.HardState.Term != 0 {
				atomic.StoreUint64(&p.lastTerm, rd.HardState.Term)
			}
			if rd.HardState.Commit != 0 {
				atomic.StoreUint64(&p.lastCommit, rd.HardState.Commit)
			}
			if p.transport != nil && len(rd.Messages) > 0 {
				p.transport.Send(rd.Messages)
			}

			// apply committed entries
			for _, e := range rd.Committed {
				switch e.Type {
				case eraftpb.EntryConfChange:
					var cc eraftpb.ConfChange
					if err := cc.Unmarshal(e.Data); err != nil {
						log.Printf("raft: confchange unmarshal: %v", err)
						continue
					}
					// Apply to raft core
					if err := p.n.ApplyConfChange(cc); err != nil {
						log.Printf("raft: apply confchange: %v", err)
					}
					// Update cluster + transport from cc.Context
					var meta confMeta
					if len(cc.Context) > 0 {
						_ = json.Unmarshal(cc.Context, &meta)
					}
					p.applyConfChange(cc, meta)
					p.signalConfDone(cc.NodeID, nil)
					p.lastApplied = e.Index

				default:
					if len(e.Data) == 0 {
						p.lastApplied = e.Index
						continue
					}
					p.applyOne(ctx, e.Data)
					p.lastApplied = e.Index
				}
			}
			p.maybeSnapshot()
			p.n.Advance()
		}
	}
}

func (p *Proposer) maybeSnapshot() {
	applied := p.lastApplied
	if applied == 0 || applied-p.lastSnap < p.snapEveryN {
		return
	}
	// Build snapshot. We leave Data empty for now (MVCC persisted in Bolt).
	snap, err := p.n.Storage().CreateSnapshot(applied, nil, nil)
	if err != nil {
		log.Printf("raft: create snapshot at %d failed: %v", applied, err)
		return
	}
	// Compact log up to (applied - compactKeepN), but not before FirstIndex.
	compactTo := uint64(0)
	if applied > p.compactKeepN {
		compactTo = applied - p.compactKeepN
	}
	if compactTo > 0 {
		if err := p.n.Storage().Compact(compactTo); err != nil {
			log.Printf("raft: compact to %d failed: %v", compactTo, err)
			// not fatal; continue
		}
	}
	// Persist snapshot to WAL on next Ready() (engine already saves rd.Snapshot).
	_ = snap // the engine will emit a Ready carrying snapshot when raft decides to send it
	p.lastSnap = applied
}

func (p *Proposer) applyOne(ctx context.Context, data []byte) {
	prop, err := Unmarshal(data)
	if err != nil {
		p.finish(prop, result{err: err})
		return
	}
	switch prop.Kind {
	case KindNOP:
		p.finish(prop, result{})
	case KindPut:
		kv, prev, err := p.app.Store.Put(ctx, prop.Put.Key, prop.Put.Value, prop.Put.PrevKv, prop.Put.LeaseID)
		if err == nil {
			if prop.Put.LeaseID != 0 && p.app.Leases != nil && kv != nil {
				_ = p.app.Leases.AttachKey(prop.Put.LeaseID, kv.Key)
			}
			if p.app.Hub != nil && kv != nil {
				rev, _ := p.app.Store.CurrentRevision(ctx)
				p.app.Hub.Broadcast(mvcc.Event{
					Type:     mvccpb.PUT,
					Key:      append([]byte(nil), kv.Key...),
					Value:    append([]byte(nil), kv.Value...),
					Revision: rev,
				})
			}
		}
		p.finish(prop, result{put: &PutResult{Prev: prev}, err: err})
	case KindDeleteRange:
		deleted, prevs, err := p.app.Store.DeleteRange(ctx, prop.Del.Key, prop.Del.RangeEnd, prop.Del.PrevKv)
		if err == nil && p.app.Leases != nil {
			for _, pr := range prevs {
				if pr.Lease != 0 {
					_ = p.app.Leases.DetachKey(pr.Lease, pr.Key)
				}
			}
		}
		if err == nil && p.app.Hub != nil && deleted > 0 {
			rev, _ := p.app.Store.CurrentRevision(ctx)
			for _, pr := range prevs {
				p.app.Hub.Broadcast(mvcc.Event{
					Type:     mvccpb.DELETE,
					Key:      append([]byte(nil), pr.Key...),
					Revision: rev,
				})
			}
		}
		p.finish(prop, result{del: &DeleteResult{Deleted: deleted, PrevKvs: prevs}, err: err})
	default:
		p.finish(prop, result{err: errors.New("unknown proposal kind")})
	}
}

func (p *Proposer) finish(prop *Proposal, r result) {
	if prop == nil {
		return
	}
	p.mu.Lock()
	ch, ok := p.waiter[prop.H.ProposalID]
	if ok {
		delete(p.waiter, prop.H.ProposalID)
	}
	p.mu.Unlock()
	if ok {
		ch <- r
		close(ch)
	}
}

func isEmptySnap(s eraftpb.Snapshot) bool { return s.Metadata.Index == 0 }

// ---- Synchronous propose helpers + barrier ----

func (p *Proposer) Put(ctx context.Context, key, val []byte, prevKv bool, leaseID int64) (*PutResult, error) {
	id := atomic.AddUint64(&p.nextID, 1)
	prop := &Proposal{H: Header{ProposalID: id}, Kind: KindPut, Put: &PutReq{Key: key, Value: val, PrevKv: prevKv, LeaseID: leaseID}}
	return p.proposeAndWaitPut(ctx, prop)
}

func (p *Proposer) DeleteRange(ctx context.Context, key, end []byte, prevKv bool) (*DeleteResult, error) {
	id := atomic.AddUint64(&p.nextID, 1)
	prop := &Proposal{H: Header{ProposalID: id}, Kind: KindDeleteRange, Del: &DeleteReq{Key: key, RangeEnd: end, PrevKv: prevKv}}
	return p.proposeAndWaitDel(ctx, prop)
}

func (p *Proposer) Barrier(ctx context.Context) error {
	id := atomic.AddUint64(&p.nextID, 1)
	prop := &Proposal{H: Header{ProposalID: id}, Kind: KindNOP}
	b, err := Marshal(prop)
	if err != nil {
		return err
	}
	ch := make(chan result, 1)
	p.mu.Lock()
	p.waiter[prop.H.ProposalID] = ch
	p.mu.Unlock()

	if err := p.n.Propose(b); err != nil {
		p.mu.Lock()
		delete(p.waiter, prop.H.ProposalID)
		p.mu.Unlock()
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-ch:
		return r.err
	}
}

func (p *Proposer) proposeAndWaitPut(ctx context.Context, prop *Proposal) (*PutResult, error) {
	b, err := Marshal(prop)
	if err != nil {
		return nil, err
	}
	ch := make(chan result, 1)
	p.mu.Lock()
	p.waiter[prop.H.ProposalID] = ch
	p.mu.Unlock()
	if err := p.n.Propose(b); err != nil {
		p.mu.Lock()
		delete(p.waiter, prop.H.ProposalID)
		p.mu.Unlock()
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		return r.put, r.err
	}
}

func (p *Proposer) proposeAndWaitDel(ctx context.Context, prop *Proposal) (*DeleteResult, error) {
	b, err := Marshal(prop)
	if err != nil {
		return nil, err
	}
	ch := make(chan result, 1)
	p.mu.Lock()
	p.waiter[prop.H.ProposalID] = ch
	p.mu.Unlock()
	if err := p.n.Propose(b); err != nil {
		p.mu.Lock()
		delete(p.waiter, prop.H.ProposalID)
		p.mu.Unlock()
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case r := <-ch:
		return r.del, r.err
	}
}

// Stats for Maintenance.Status
func (p *Proposer) RaftTerm() uint64  { return atomic.LoadUint64(&p.lastTerm) }
func (p *Proposer) RaftIndex() uint64 { return atomic.LoadUint64(&p.lastCommit) }
func (p *Proposer) LeaderID() uint64  { return atomic.LoadUint64(&p.leaderID) }

// ---------- ConfChange helpers ----------

// confMeta travels in cc.Context (JSON) to carry name/URLs for Add/Update.
type confMeta struct {
	Name       string   `json:"name,omitempty"`
	PeerURLs   []string `json:"peer_urls,omitempty"`
	ClientURLs []string `json:"client_urls,omitempty"`
	IsLearner  bool     `json:"is_learner,omitempty"`
}

func (p *Proposer) applyConfChange(cc eraftpb.ConfChange, meta confMeta) {
	switch cc.Type {
	case eraftpb.ConfChangeAddNode:
		if p.cluster != nil {
			if find := findByID(p.cluster.Members(), cc.NodeID); find != nil {
				if p.transport != nil && len(meta.PeerURLs) > 0 {
					p.transport.AddPeer(cc.NodeID, meta.PeerURLs[0])
				}
				return
			}
			mem, err := p.cluster.AddMember(cc.NodeID, meta.Name, meta.PeerURLs, meta.ClientURLs)
			if err != nil {
				log.Printf("cluster: add %d failed: %v", cc.NodeID, err)
			} else if mem != nil {
				mem.IsLearner = meta.IsLearner
			}
		}
		if p.transport != nil && len(meta.PeerURLs) > 0 {
			p.transport.AddPeer(cc.NodeID, meta.PeerURLs[0])
		}
	case eraftpb.ConfChangeRemoveNode:
		if p.transport != nil {
			p.transport.RemovePeer(cc.NodeID)
		}
		if p.cluster != nil {
			p.cluster.RemoveMember(cc.NodeID)
		}
	case eraftpb.ConfChangeUpdateNode:
		if p.cluster != nil {
			if err := p.cluster.UpdatePeerURLs(cc.NodeID, meta.PeerURLs); err != nil {
				log.Printf("cluster: update %d failed: %v", cc.NodeID, err)
			}
		}
		if p.transport != nil && len(meta.PeerURLs) > 0 {
			p.transport.AddPeer(cc.NodeID, meta.PeerURLs[0])
		}
	}
}

func (p *Proposer) signalConfDone(id uint64, err error) {
	p.confMu.Lock()
	ch, ok := p.confWait[id]
	if ok {
		delete(p.confWait, id)
	}
	p.confMu.Unlock()
	if ok {
		ch <- err
		close(ch)
	}
}

func findByID(ms []*etcdserverpb.Member, id uint64) *etcdserverpb.Member {
	for _, m := range ms {
		if m.ID == id {
			return m
		}
	}
	return nil
}

// -------- Propose ConfChange (used by API) --------

func (p *Proposer) ProposeAddMember(ctx context.Context, id uint64, name string, peerURLs, clientURLs []string, isLearner bool) error {
	meta, _ := json.Marshal(confMeta{
		Name:       name,
		PeerURLs:   peerURLs,
		ClientURLs: clientURLs,
		IsLearner:  isLearner,
	})
	cc := eraftpb.ConfChange{Type: eraftpb.ConfChangeAddNode, NodeID: id, Context: meta}

	ch := make(chan error, 1)
	p.confMu.Lock()
	p.confWait[id] = ch
	p.confMu.Unlock()

	if err := p.n.ProposeConfChange(cc); err != nil {
		p.confMu.Lock()
		delete(p.confWait, id)
		p.confMu.Unlock()
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ch:
		return err
	}
}

func (p *Proposer) ProposeRemoveMember(ctx context.Context, id uint64) error {
	cc := eraftpb.ConfChange{Type: eraftpb.ConfChangeRemoveNode, NodeID: id}

	ch := make(chan error, 1)
	p.confMu.Lock()
	p.confWait[id] = ch
	p.confMu.Unlock()

	if err := p.n.ProposeConfChange(cc); err != nil {
		p.confMu.Lock()
		delete(p.confWait, id)
		p.confMu.Unlock()
		return err
	}
	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-ch:
		return err
	}
}
