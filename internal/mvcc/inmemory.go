package mvcc

import (
	"context"
	"sort"
	"sync"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

type memMVCC struct {
	mu    sync.RWMutex
	data  map[string]*mvccpb.KeyValue
	keys  []string
	rev   int64
	lease map[string]int64
}

func NewMemory() KV {
	return &memMVCC{
		data:  make(map[string]*mvccpb.KeyValue),
		keys:  make([]string, 0),
		lease: make(map[string]int64),
	}
}

func (m *memMVCC) bump() int64 { m.rev++; return m.rev }

func (m *memMVCC) CurrentRevision(ctx context.Context) (int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.rev, nil
}

func (m *memMVCC) Put(ctx context.Context, key, val []byte, prevKv bool, leaseID int64) (*mvccpb.KeyValue, *mvccpb.KeyValue, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := string(key)
	var prev *mvccpb.KeyValue
	nr := m.bump()
	if cur, ok := m.data[k]; ok {
		if prevKv {
			cp := *cur
			cp.Value = append([]byte(nil), cur.Value...)
			prev = &cp
		}
		cur.Value = append([]byte(nil), val...)
		cur.ModRevision = nr
		cur.Version++
		cur.Lease = leaseID
	} else {
		idx := sort.SearchStrings(m.keys, k)
		if idx == len(m.keys) {
			m.keys = append(m.keys, k)
		} else if m.keys[idx] != k {
			m.keys = append(m.keys, "")
			copy(m.keys[idx+1:], m.keys[idx:])
			m.keys[idx] = k
		}
		m.data[k] = &mvccpb.KeyValue{
			Key:            []byte(k),
			Value:          append([]byte(nil), val...),
			CreateRevision: nr,
			ModRevision:    nr,
			Version:        1,
			Lease:          leaseID,
		}
	}
	cp := *m.data[k]
	cp.Value = append([]byte(nil), m.data[k].Value...)
	return &cp, prev, nil
}

func (m *memMVCC) DeleteRange(ctx context.Context, key, end []byte, prevKv bool) (int64, []*mvccpb.KeyValue, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var del int64
	var prevs []*mvccpb.KeyValue
	if len(end) == 0 {
		k := string(key)
		if cur, ok := m.data[k]; ok {
			if prevKv {
				cp := *cur
				cp.Value = append([]byte(nil), cur.Value...)
				prevs = append(prevs, &cp)
			}
			m.bump()
			delete(m.data, k)
			idx := sort.SearchStrings(m.keys, k)
			if idx < len(m.keys) && m.keys[idx] == k {
				m.keys = append(m.keys[:idx], m.keys[idx+1:]...)
			}
			del = 1
		}
		return del, prevs, nil
	}
	start := string(key)
	lim := string(end)
	kept := m.keys[:0]
	for _, k := range m.keys {
		if k >= start && k < lim {
			if cur, ok := m.data[k]; ok {
				if prevKv {
					cp := *cur
					cp.Value = append([]byte(nil), cur.Value...)
					prevs = append(prevs, &cp)
				}
				m.bump()
				delete(m.data, k)
				del++
			}
			continue
		}
		kept = append(kept, k)
	}
	m.keys = kept
	return del, prevs, nil
}

func (m *memMVCC) Range(ctx context.Context, op OpRange) ([]*mvccpb.KeyValue, int64, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if op.CountOnly {
		if len(op.RangeEnd) == 0 {
			if _, ok := m.data[string(op.Key)]; ok {
				return nil, 1, nil
			}
			return nil, 0, nil
		}
		start, lim := string(op.Key), string(op.RangeEnd)
		c := int64(0)
		for _, k := range m.keys {
			if k >= start && k < lim {
				c++
				if op.Limit > 0 && c >= op.Limit {
					break
				}
			}
		}
		return nil, c, nil
	}
	out := make([]*mvccpb.KeyValue, 0)
	appendKV := func(k string) {
		if op.Limit > 0 && int64(len(out)) >= op.Limit {
			return
		}
		if cur, ok := m.data[k]; ok {
			cp := *cur
			if op.KeysOnly {
				cp.Value = nil
			} else {
				cp.Value = append([]byte(nil), cur.Value...)
			}
			out = append(out, &cp)
		}
	}
	if len(op.RangeEnd) == 0 {
		appendKV(string(op.Key))
		return out, int64(len(out)), nil
	}
	start, lim := string(op.Key), string(op.RangeEnd)
	for _, k := range m.keys {
		if k >= start && k < lim {
			appendKV(k)
			if op.Limit > 0 && int64(len(out)) >= op.Limit {
				break
			}
		}
	}
	return out, int64(len(out)), nil
}
