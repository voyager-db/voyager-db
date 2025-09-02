package mvcc

import (
	"context"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

// OpRange captures etcd RangeRequest knobs we care about now.
type OpRange struct {
	Key       []byte
	RangeEnd  []byte // nil => single key
	Limit     int64
	KeysOnly  bool
	CountOnly bool
}

// KV is the MVCC surface the API layer needs.
// Revisions are monotonically increasing across the entire keyspace.
type KV interface {
	// Put stores a value and returns the resulting KeyValue with CreateRevision/ModRevision/Version set.
	// If PrevKv is true, also returns the previous KeyValue (if existed).
	Put(ctx context.Context, key, val []byte, prevKv bool, leaseID int64) (kv *mvccpb.KeyValue, prev *mvccpb.KeyValue, err error)

	// DeleteRange removes [key, end) (or single key if end==nil).
	// Returns delete count and optionally the previous KeyValues (if prevKv is true).
	DeleteRange(ctx context.Context, key, end []byte, prevKv bool) (deleted int64, prevs []*mvccpb.KeyValue, err error)

	// Range returns KVs and a count (countOnly ignores kvs).
	Range(ctx context.Context, op OpRange) (kvs []*mvccpb.KeyValue, count int64, err error)

	// NextRevision returns the next global logical revision (after the last write).
	// For watchers to tag events consistently with storage.
	CurrentRevision(ctx context.Context) (rev int64, err error)
}
