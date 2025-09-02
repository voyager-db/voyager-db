package boltstore

import (
	"bytes"
	"context"
	"io"
	"path/filepath"

	bbolt "go.etcd.io/bbolt"
)

var bucketKV = []byte("kv")

// Store implements kv.Store backed by bbolt. This is a simple KV (no MVCC yet).
type Store struct {
	db *bbolt.DB
}

// New opens (or creates) a Bolt DB at the given path and ensures the bucket.
func New(path string) (*Store, error) {
	db, err := bbolt.Open(filepath.Clean(path), 0o600, &bbolt.Options{NoSync: false})
	if err != nil {
		return nil, err
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		_, e := tx.CreateBucketIfNotExists(bucketKV)
		return e
	}); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// DB exposes the underlying *bbolt.DB (for lease manager, snapshots, etc.).
func (s *Store) DB() *bbolt.DB { return s.db }

// Put sets key=val; returns previous value if existed.
func (s *Store) Put(key, val []byte) (prev []byte, had bool) {
	_ = s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketKV)
		old := b.Get(key)
		if old != nil {
			prev = append([]byte(nil), old...)
			had = true
		}
		v := append([]byte(nil), val...)
		return b.Put(key, v)
	})
	return prev, had
}

// DeleteRange deletes [key, end). If end==nil/len 0, deletes single key.
func (s *Store) DeleteRange(key, end []byte) (deleted int, prevKVs [][2][]byte) {
	_ = s.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketKV)
		if len(end) == 0 {
			if v := b.Get(key); v != nil {
				prevKVs = append(prevKVs, [2][]byte{append([]byte(nil), key...), append([]byte(nil), v...)})
				deleted++
				_ = b.Delete(key)
			}
			return nil
		}
		c := b.Cursor()
		for k, v := c.Seek(key); k != nil && bytes.Compare(k, end) < 0; k, v = c.Next() {
			prevKVs = append(prevKVs, [2][]byte{append([]byte(nil), k...), append([]byte(nil), v...)})
			deleted++
			_ = c.Delete()
		}
		return nil
	})
	return deleted, prevKVs
}

// Range scans [key, end). If end==nil/len 0, returns single key.
func (s *Store) Range(key, end []byte, limit int64, keysOnly bool, countOnly bool) (kvs [][2][]byte, count int) {
	_ = s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketKV)
		if countOnly {
			if len(end) == 0 {
				if v := b.Get(key); v != nil {
					count = 1
				}
				return nil
			}
			c := b.Cursor()
			for k, _ := c.Seek(key); k != nil && bytes.Compare(k, end) < 0; k, _ = c.Next() {
				count++
				if limit > 0 && int64(count) >= limit {
					break
				}
			}
			return nil
		}

		appendKV := func(k, v []byte) {
			if limit > 0 && int64(len(kvs)) >= limit {
				return
			}
			kc := append([]byte(nil), k...)
			if keysOnly {
				kvs = append(kvs, [2][]byte{kc, nil})
			} else {
				vc := append([]byte(nil), v...)
				kvs = append(kvs, [2][]byte{kc, vc})
			}
		}

		if len(end) == 0 {
			if v := b.Get(key); v != nil {
				appendKV(key, v)
			}
			count = len(kvs)
			return nil
		}

		c := b.Cursor()
		for k, v := c.Seek(key); k != nil && bytes.Compare(k, end) < 0; k, v = c.Next() {
			appendKV(k, v)
			if limit > 0 && int64(len(kvs)) >= limit {
				break
			}
		}
		count = len(kvs)
		return nil
	})
	return kvs, count
}

// SnapshotBytes materializes the KV into a stable byte slice (best-effort).
// This is only used for HashKV placeholder in Phase A.
func (s *Store) SnapshotBytes() []byte {
	var out []byte
	_ = s.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketKV)
		c := b.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			out = append(out, k...)
			out = append(out, 0)
			out = append(out, v...)
			out = append(out, '\n')
		}
		return nil
	})
	return out
}

// SnapshotOpen returns a consistent snapshot reader by piping bbolt Tx.WriteTo.
func (s *Store) SnapshotOpen(ctx context.Context) (io.ReadCloser, error) {
	pr, pw := io.Pipe()
	go func() {
		err := s.db.View(func(tx *bbolt.Tx) error {
			_, werr := tx.WriteTo(pw)
			return werr
		})
		_ = pw.CloseWithError(err)
	}()
	return pr, nil
}

// Close closes the underlying DB.
func (s *Store) Close() error { return s.db.Close() }
