package mvcc

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	bbolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

var (
	bktMeta = []byte("meta") // holds current_rev
	bktKV   = []byte("kv")   // key -> encoded ValueMeta (latest only, Phase A)
	keyCur  = []byte("current_rev")
)

type boltMVCC struct {
	db *bbolt.DB
}

// OpenBolt opens (or ensures) MVCC buckets in an existing bbolt DB.
func OpenBolt(db *bbolt.DB) (KV, error) {
	err := db.Update(func(tx *bbolt.Tx) error {
		if _, e := tx.CreateBucketIfNotExists(bktMeta); e != nil {
			return e
		}
		if _, e := tx.CreateBucketIfNotExists(bktKV); e != nil {
			return e
		}
		bm := tx.Bucket(bktMeta)
		if bm.Get(keyCur) == nil {
			var z [8]byte
			return bm.Put(keyCur, z[:])
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &boltMVCC{db: db}, nil
}

func (b *boltMVCC) CurrentRevision(ctx context.Context) (int64, error) {
	var rev int64
	err := b.db.View(func(tx *bbolt.Tx) error {
		v := tx.Bucket(bktMeta).Get(keyCur)
		if v == nil {
			rev = 0
			return nil
		}
		rev = int64(binary.BigEndian.Uint64(v))
		return nil
	})
	return rev, err
}

func (b *boltMVCC) nextRev(tx *bbolt.Tx) (int64, error) {
	bm := tx.Bucket(bktMeta)
	cur := bm.Get(keyCur)
	if cur == nil {
		return 0, errors.New("meta missing current_rev")
	}
	n := binary.BigEndian.Uint64(cur) + 1
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], n)
	if err := bm.Put(keyCur, buf[:]); err != nil {
		return 0, err
	}
	return int64(n), nil
}

type valmeta struct {
	Value          []byte
	CreateRevision int64
	ModRevision    int64
	Version        int64
	Lease          int64
}

// ---- encoding helpers (simple, fixed layout: [u64 cr][u64 mr][u64 ver][u64 lease][len|val]) ----

func encode(vm valmeta) []byte {
	// header 32 bytes + value
	out := make([]byte, 32+len(vm.Value))
	binary.BigEndian.PutUint64(out[0:8], uint64(vm.CreateRevision))
	binary.BigEndian.PutUint64(out[8:16], uint64(vm.ModRevision))
	binary.BigEndian.PutUint64(out[16:24], uint64(vm.Version))
	binary.BigEndian.PutUint64(out[24:32], uint64(vm.Lease))
	copy(out[32:], vm.Value)
	return out
}

func decode(b []byte, key []byte) *mvccpb.KeyValue {
	if len(b) < 32 {
		return &mvccpb.KeyValue{Key: key}
	}
	cr := int64(binary.BigEndian.Uint64(b[0:8]))
	mr := int64(binary.BigEndian.Uint64(b[8:16]))
	ver := int64(binary.BigEndian.Uint64(b[16:24]))
	lease := int64(binary.BigEndian.Uint64(b[24:32]))
	val := append([]byte(nil), b[32:]...)
	return &mvccpb.KeyValue{
		Key:            append([]byte(nil), key...),
		Value:          val,
		CreateRevision: cr,
		ModRevision:    mr,
		Version:        ver,
		Lease:          lease,
	}
}

func (b *boltMVCC) Put(ctx context.Context, key, val []byte, prevKv bool, leaseID int64) (*mvccpb.KeyValue, *mvccpb.KeyValue, error) {
	var out, prev *mvccpb.KeyValue
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(bktKV)
		rev, err := b.nextRev(tx)
		if err != nil {
			return err
		}
		old := bk.Get(key)
		var cr, ver int64
		if old != nil {
			op := decode(old, key)
			cr = op.CreateRevision
			ver = op.Version + 1
			if prevKv {
				prev = op
			}
		} else {
			cr = rev
			ver = 1
		}
		vm := valmeta{
			Value:          append([]byte(nil), val...),
			CreateRevision: cr,
			ModRevision:    rev,
			Version:        ver,
			Lease:          leaseID,
		}
		if err := bk.Put(key, encode(vm)); err != nil {
			return err
		}
		out = decode(encode(vm), key)
		return nil
	})
	return out, prev, err
}

func (b *boltMVCC) DeleteRange(ctx context.Context, key, end []byte, prevKv bool) (int64, []*mvccpb.KeyValue, error) {
	var del int64
	var prevs []*mvccpb.KeyValue
	err := b.db.Update(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(bktKV)
		// bump revision once, but etcd bumps per change; for Phase A we’ll bump per key to keep mod_rev unique.
		c := bk.Cursor()
		if len(end) == 0 {
			k := key
			v := bk.Get(k)
			if v == nil {
				return nil
			}
			if prevKv {
				prevs = append(prevs, decode(v, k))
			}
			// bump revision per deletion
			if _, err := b.nextRev(tx); err != nil {
				return err
			}
			if err := bk.Delete(k); err != nil {
				return err
			}
			del = 1
			return nil
		}
		for k, v := c.Seek(key); k != nil && bytes.Compare(k, end) < 0; k, v = c.Next() {
			if prevKv {
				prevs = append(prevs, decode(v, k))
			}
			if _, err := b.nextRev(tx); err != nil {
				return err
			}
			if err := c.Delete(); err != nil {
				return err
			}
			del++
		}
		return nil
	})
	return del, prevs, err
}

func (b *boltMVCC) Range(ctx context.Context, op OpRange) ([]*mvccpb.KeyValue, int64, error) {
	var out []*mvccpb.KeyValue
	var count int64
	err := b.db.View(func(tx *bbolt.Tx) error {
		bk := tx.Bucket(bktKV)
		if op.CountOnly {
			if len(op.RangeEnd) == 0 {
				if v := bk.Get(op.Key); v != nil {
					count = 1
				}
				return nil
			}
			c := bk.Cursor()
			for k, _ := c.Seek(op.Key); k != nil && bytes.Compare(k, op.RangeEnd) < 0; k, _ = c.Next() {
				count++
				if op.Limit > 0 && count >= op.Limit {
					break
				}
			}
			return nil
		}

		appendKV := func(k, v []byte) {
			if op.Limit > 0 && int64(len(out)) >= op.Limit {
				return
			}
			kv := decode(v, k)
			if op.KeysOnly {
				kv.Value = nil
			}
			out = append(out, kv)
		}

		if len(op.RangeEnd) == 0 {
			if v := bk.Get(op.Key); v != nil {
				appendKV(op.Key, v)
			}
			count = int64(len(out))
			return nil
		}

		c := bk.Cursor()
		for k, v := c.Seek(op.Key); k != nil && bytes.Compare(k, op.RangeEnd) < 0; k, v = c.Next() {
			appendKV(k, v)
			if op.Limit > 0 && int64(len(out)) >= op.Limit {
				break
			}
		}
		count = int64(len(out))
		return nil
	})
	return out, count, err
}

// DebugDump (optional)
func (b *boltMVCC) String() string { return fmt.Sprintf("mvcc(bolt)") }
