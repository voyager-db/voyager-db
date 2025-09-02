package lease

import (
	"context"
	"encoding/binary"
	"time"

	bbolt "go.etcd.io/bbolt"
)

var (
	bucketLeases   = []byte("leases")     // id -> [ttl,expire]
	bucketLeaseIdx = []byte("lease_keys") // parent: lease_keys ; child bucket per lease id holds keys
)

type BoltManager struct {
	db       *bbolt.DB
	interval time.Duration
	stopCh   chan struct{}
	expCh    chan int64
}

type diskLease struct {
	TTL      int64 // seconds
	ExpireAt int64 // unix seconds
}

func NewBoltManager(db *bbolt.DB) (*BoltManager, error) {
	m := &BoltManager{
		db:       db,
		interval: time.Second,
		stopCh:   make(chan struct{}),
		expCh:    make(chan int64, 1024),
	}
	if err := db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(bucketLeases); err != nil {
			return err
		}
		if _, err := tx.CreateBucketIfNotExists(bucketLeaseIdx); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return nil, err
	}
	go m.reaper()
	return m, nil
}

func (m *BoltManager) Close()                { close(m.stopCh) }
func (m *BoltManager) Expired() <-chan int64 { return m.expCh }

func (m *BoltManager) reaper() {
	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case now := <-t.C:
			var expired []int64
			_ = m.db.Update(func(tx *bbolt.Tx) error {
				b := tx.Bucket(bucketLeases)
				c := b.Cursor()
				for k, v := c.First(); k != nil; k, v = c.Next() {
					var dl diskLease
					decodeLease(v, &dl)
					if now.Unix() >= dl.ExpireAt {
						// remove lease record but keep lease_keys subbucket for server to read
						_ = b.Delete(k)
						expired = append(expired, int64(binary.BigEndian.Uint64(k)))
					}
				}
				return nil
			})
			for _, id := range expired {
				select {
				case m.expCh <- id:
				default:
				}
			}
		}
	}
}

func (m *BoltManager) Grant(ttlSec int64) (id int64, expire time.Time) {
	if ttlSec <= 0 {
		ttlSec = 1
	}
	now := time.Now()
	expire = now.Add(time.Duration(ttlSec) * time.Second)
	_ = m.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketLeases)
		next := b.Sequence() + 1
		_ = b.SetSequence(next)
		id = int64(next)
		dl := diskLease{TTL: ttlSec, ExpireAt: expire.Unix()}
		if err := b.Put(itob(id), encodeLease(dl)); err != nil {
			return err
		}
		// ensure subbucket
		lb := tx.Bucket(bucketLeaseIdx)
		_, _ = lb.CreateBucketIfNotExists(itob(id))
		return nil
	})
	return id, expire
}

func (m *BoltManager) KeepAlive(id int64) (remaining int64, ttl int64, err error) {
	err = m.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketLeases)
		k := itob(id)
		v := b.Get(k)
		if v == nil {
			return ErrNotFound
		}
		var dl diskLease
		decodeLease(v, &dl)
		ttl = dl.TTL
		exp := time.Now().Add(time.Duration(dl.TTL) * time.Second)
		dl.ExpireAt = exp.Unix()
		if err := b.Put(k, encodeLease(dl)); err != nil {
			return err
		}
		remaining = int64(time.Until(exp).Seconds())
		if remaining < 0 {
			remaining = 0
		}
		return nil
	})
	return
}

func (m *BoltManager) TTL(id int64) (Info, error) {
	var info Info
	err := m.db.View(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketLeases)
		v := b.Get(itob(id))
		if v == nil {
			return ErrNotFound
		}
		var dl diskLease
		decodeLease(v, &dl)
		exp := time.Unix(dl.ExpireAt, 0)
		rem := time.Until(exp).Seconds()
		if rem < 0 {
			rem = 0
		}
		info = Info{ID: id, TTL: dl.TTL, ExpireAt: exp, Remaining: int64(rem)}
		return nil
	})
	return info, err
}

func (m *BoltManager) Revoke(id int64) error {
	// remove lease record and notify expiry; keep keys subbucket for server to read & purge
	return m.db.Update(func(tx *bbolt.Tx) error {
		b := tx.Bucket(bucketLeases)
		_ = b.Delete(itob(id))
		select {
		case m.expCh <- id:
		default:
		}
		return nil
	})
}

// --- key tracking ---

func (m *BoltManager) AttachKey(id int64, key []byte) error {
	return m.db.Update(func(tx *bbolt.Tx) error {
		lb := tx.Bucket(bucketLeaseIdx)
		sb, err := lb.CreateBucketIfNotExists(itob(id))
		if err != nil {
			return err
		}
		return sb.Put(key, []byte{1})
	})
}

func (m *BoltManager) DetachKey(id int64, key []byte) error {
	return m.db.Update(func(tx *bbolt.Tx) error {
		lb := tx.Bucket(bucketLeaseIdx)
		sb := lb.Bucket(itob(id))
		if sb == nil {
			return nil
		}
		_ = sb.Delete(key)
		if sb.Stats().KeyN == 0 {
			_ = lb.DeleteBucket(itob(id))
		}
		return nil
	})
}

func (m *BoltManager) Keys(id int64) ([][]byte, error) {
	var out [][]byte
	err := m.db.View(func(tx *bbolt.Tx) error {
		lb := tx.Bucket(bucketLeaseIdx)
		sb := lb.Bucket(itob(id))
		if sb == nil {
			return nil
		}
		c := sb.Cursor()
		for k, _ := c.First(); k != nil; k, _ = c.Next() {
			out = append(out, append([]byte(nil), k...))
		}
		return nil
	})
	return out, err
}

// helpers

func itob(v int64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], uint64(v))
	return b[:]
}

func encodeLease(dl diskLease) []byte {
	var b [16]byte
	binary.BigEndian.PutUint64(b[0:8], uint64(dl.TTL))
	binary.BigEndian.PutUint64(b[8:16], uint64(dl.ExpireAt))
	return b[:]
}

func decodeLease(b []byte, dl *diskLease) {
	if len(b) >= 16 {
		dl.TTL = int64(binary.BigEndian.Uint64(b[0:8]))
		dl.ExpireAt = int64(binary.BigEndian.Uint64(b[8:16]))
	}
}

// satisfy ManagerAPI Close() even if unused in future
func (m *BoltManager) SnapshotOpen(ctx context.Context) {}
