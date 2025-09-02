package raft

import (
	"encoding/binary"
	"fmt"
	"path/filepath"

	"github.com/golang/protobuf/proto"
	bbolt "go.etcd.io/bbolt"

	raftx "github.com/voyager-db/raftx"
	raftpb "github.com/voyager-db/raftx/raftpb"
)

var (
	bktMeta     = []byte("meta")    // hardstate, snap meta
	bktEntries  = []byte("entries") // index -> raftpb.Entry
	keyHS       = []byte("hardstate")
	keySnap     = []byte("snapshot") // raftpb.Snapshot blob
	keySnapIdx  = []byte("snap_index")
	keySnapTerm = []byte("snap_term")
)

// WAL persists raft Ready() to Bolt and can restore into MemoryStorage on boot.
type WAL struct {
	db *bbolt.DB
}

func OpenWAL(path string) (*WAL, error) {
	db, err := bbolt.Open(filepath.Clean(path), 0o600, &bbolt.Options{NoSync: false})
	if err != nil {
		return nil, fmt.Errorf("open raft wal: %w", err)
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		if _, e := tx.CreateBucketIfNotExists(bktMeta); e != nil {
			return e
		}
		if _, e := tx.CreateBucketIfNotExists(bktEntries); e != nil {
			return e
		}
		return nil
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &WAL{db: db}, nil
}

func (w *WAL) Close() error { return w.db.Close() }

// SaveReady persists snapshot, entries, and hardstate (in that order).
func (w *WAL) SaveReady(rd raftx.Ready) error {
	return w.db.Update(func(tx *bbolt.Tx) error {
		meta := tx.Bucket(bktMeta)
		ents := tx.Bucket(bktEntries)

		// 1) Snapshot
		if rd.Snapshot.Metadata.Index != 0 {
			b, err := proto.Marshal(&rd.Snapshot)
			if err != nil {
				return err
			}
			if err := meta.Put(keySnap, b); err != nil {
				return err
			}
			if err := putU64(meta, keySnapIdx, rd.Snapshot.Metadata.Index); err != nil {
				return err
			}
			if err := putU64(meta, keySnapTerm, rd.Snapshot.Metadata.Term); err != nil {
				return err
			}
			if err := pruneLE(ents, rd.Snapshot.Metadata.Index); err != nil {
				return err
			}
		}

		// 2) Entries
		for _, e := range rd.Entries {
			key := u64key(e.Index)
			val, err := proto.Marshal(&e)
			if err != nil {
				return err
			}
			if err := ents.Put(key, val); err != nil {
				return err
			}
		}

		// 3) HardState
		if !(rd.HardState.Term == 0 && rd.HardState.Vote == 0 && rd.HardState.Commit == 0) {
			b, err := proto.Marshal(&rd.HardState)
			if err != nil {
				return err
			}
			if err := meta.Put(keyHS, b); err != nil {
				return err
			}
		}

		return nil
	})
}

// RestoreToMemory rebuilds the in-memory raft storage from WAL.
func (w *WAL) RestoreToMemory(ms *raftx.MemoryStorage) error {
	return w.db.View(func(tx *bbolt.Tx) error {
		meta := tx.Bucket(bktMeta)
		ents := tx.Bucket(bktEntries)

		// Snapshot (if any)
		if snapBytes := meta.Get(keySnap); snapBytes != nil {
			var snap raftpb.Snapshot
			if err := proto.Unmarshal(snapBytes, &snap); err != nil {
				return err
			}
			if err := ms.ApplySnapshot(snap); err != nil {
				return err
			}
		}

		// Entries (from snapIdx+1 upward)
		var startIdx uint64 = 1
		if si := meta.Get(keySnapIdx); si != nil {
			startIdx = binary.BigEndian.Uint64(si) + 1
		}
		c := ents.Cursor()
		var toAppend []raftpb.Entry
		for k, v := c.Seek(u64key(startIdx)); k != nil; k, v = c.Next() {
			var e raftpb.Entry
			if err := proto.Unmarshal(v, &e); err != nil {
				return err
			}
			toAppend = append(toAppend, e)
		}
		if len(toAppend) > 0 {
			if err := ms.Append(toAppend); err != nil {
				return err
			}
		}

		// HardState (last)
		if hsBytes := meta.Get(keyHS); hsBytes != nil {
			var hs raftpb.HardState
			if err := proto.Unmarshal(hsBytes, &hs); err != nil {
				return err
			}
			if err := ms.SetHardState(hs); err != nil {
				return err
			}
		}
		return nil
	})
}

func pruneLE(b *bbolt.Bucket, le uint64) error {
	c := b.Cursor()
	for k, _ := c.First(); k != nil; k, _ = c.Next() {
		if binary.BigEndian.Uint64(k) <= le {
			if err := b.Delete(k); err != nil {
				return err
			}
		} else {
			break
		}
	}
	return nil
}

func putU64(b *bbolt.Bucket, key []byte, v uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return b.Put(key, buf[:])
}
func u64key(v uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], v)
	return b[:]
}
