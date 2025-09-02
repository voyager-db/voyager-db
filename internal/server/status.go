package server

import (
	"context"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/voyager-db/voyager-db/internal/api"
)

// statusProvider implements api.StatusProvider using local process state.
// Later we will populate these from raft+mvcc.
type statusProvider struct {
	dataDir   string
	version   string
	memberID  uint64
	clusterID uint64
	raft      raftStats
	snapFn    func(ctx context.Context) (io.ReadCloser, error)
}

type raftStats interface {
	RaftTerm() uint64
	RaftIndex() uint64
	RaftAppliedIndex() uint64
	LeaderID() uint64
}

func newStatusProvider(cfg Config) api.StatusProvider {
	return &statusProvider{
		dataDir: cfg.DataDir,
		version: version,
	}
}

// newStatusProviderWithSnap injects an optimized snapshot function (Bolt).
func newStatusProviderWithSnap(cfg Config, snap func(ctx context.Context) (io.ReadCloser, error)) api.StatusProvider {
	return &statusProvider{
		dataDir: cfg.DataDir,
		version: version,
		snapFn:  snap,
	}
}

func newStatusProviderWithRaft(cfg Config, snap func(ctx context.Context) (io.ReadCloser, error), r raftStats, memberID, clusterID uint64) api.StatusProvider {
	return &statusProvider{
		dataDir:   cfg.DataDir,
		version:   version,
		memberID:  memberID,
		clusterID: clusterID,
		raft:      r,
		snapFn:    snap,
	}
}

func (p *statusProvider) Version() string { return p.version }
func (p *statusProvider) LeaderID() uint64 {
	if p.raft != nil {
		return p.raft.LeaderID()
	}
	return 0
}

func (p *statusProvider) RaftIndex() uint64 {
	if p.raft != nil {
		return p.raft.RaftIndex()
	}
	return 0
}

func (p *statusProvider) RaftTerm() uint64 {
	if p.raft != nil {
		return p.raft.RaftTerm()
	}
	return 0
}

func (p *statusProvider) RaftAppliedIndex() uint64 {
	if p.raft != nil {
		return p.raft.RaftAppliedIndex()
	}
	return 0
}

func (p *statusProvider) IsLearner() bool { return false }

func (p *statusProvider) DbSize() int64 {
	var total int64
	if fi, err := os.Stat(filepath.Join(p.dataDir, "state.db")); err == nil {
		total += fi.Size()
	}
	if fi, err := os.Stat(filepath.Join(p.dataDir, "raft.db")); err == nil {
		total += fi.Size()
	}
	return total
}

// For now DbSizeInUse == DbSize; later we can read Bolt stats.
func (p *statusProvider) DbSizeInUse() int64 { return p.DbSize() }

// HashKV best-effort: FNV-1a of state.db; compactRev/rev=0 until MVCC wired.
func (p *statusProvider) HashKV() (uint32, int64, int64) {
	fp := filepath.Join(p.dataDir, "state.db")
	f, err := os.Open(fp)
	if err != nil {
		return 0, 0, 0
	}
	defer f.Close()
	h := fnv.New32a()
	_, _ = io.Copy(h, f)
	return h.Sum32(), 0, 0
}

func (p *statusProvider) Header() *etcdserverpb.ResponseHeader {
	return &etcdserverpb.ResponseHeader{
		MemberId:  p.memberID,
		ClusterId: p.clusterID,
	}
}

// SnapshotOpen prefers the injected Bolt snapshot if present,
// otherwise falls back to streaming state.db (or raft.db) bytes directly.
func (p *statusProvider) SnapshotOpen(ctx context.Context) (io.ReadCloser, error) {
	if p.snapFn != nil {
		return p.snapFn(ctx)
	}
	paths := []string{
		filepath.Join(p.dataDir, "state.db"),
		filepath.Join(p.dataDir, "raft.db"),
	}
	for _, fp := range paths {
		if fi, err := os.Stat(fp); err == nil && fi.Mode().IsRegular() {
			return os.Open(fp) // best-effort fallback
		}
	}
	return io.NopCloser(&emptyReader{}), nil
}

type emptyReader struct{}

func (e *emptyReader) Read(_ []byte) (int, error) { return 0, io.EOF }
