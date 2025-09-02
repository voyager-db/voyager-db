package kv

import (
	"bytes"
	"sort"
	"strings"
	"sync"
)

// Store is a minimal KV store that supports etcd-like Range semantics we need short-term.
// This is a temporary, in-memory implementation for Phase A smoke; it will be replaced by MVCC.
type Store interface {
	Put(key, val []byte) (prev []byte, had bool)
	DeleteRange(key, end []byte) (deleted int, prevKVs [][2][]byte)
	Range(key, end []byte, limit int64, keysOnly bool, countOnly bool) (kvs [][2][]byte, count int)
	SnapshotBytes() []byte // best-effort for HashKV placeholder
}

type memStore struct {
	mu   sync.RWMutex
	data map[string][]byte
	// keys kept sorted for deterministic Range (prefix scans)
	keys []string
}

func NewMemory() Store {
	return &memStore{data: make(map[string][]byte), keys: make([]string, 0)}
}

func (m *memStore) putNoLock(k string, v []byte) (prev []byte, had bool) {
	if _, ok := m.data[k]; !ok {
		// insert into keys keeping sorted order
		idx := sort.SearchStrings(m.keys, k)
		if idx == len(m.keys) {
			m.keys = append(m.keys, k)
		} else if m.keys[idx] != k {
			m.keys = append(m.keys, "")
			copy(m.keys[idx+1:], m.keys[idx:])
			m.keys[idx] = k
		}
	}
	prev, had = m.data[k], false
	if prev != nil {
		had = true
	}
	// copy to avoid external mutation
	val := make([]byte, len(v))
	copy(val, v)
	m.data[k] = val
	return prev, had
}

func (m *memStore) Put(key, val []byte) (prev []byte, had bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.putNoLock(string(key), val)
}

func (m *memStore) DeleteRange(key, end []byte) (deleted int, prevKVs [][2][]byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	k := string(key)
	if len(end) == 0 {
		// single key delete
		if prev, ok := m.data[k]; ok {
			delete(m.data, k)
			// remove from keys
			idx := sort.SearchStrings(m.keys, k)
			if idx < len(m.keys) && m.keys[idx] == k {
				m.keys = append(m.keys[:idx], m.keys[idx+1:]...)
			}
			prevKVs = append(prevKVs, [2][]byte{[]byte(k), clone(prev)})
			return 1, prevKVs
		}
		return 0, nil
	}
	// range delete: [key, end)
	start := string(key)
	limit := string(end)
	var kept []string
	for _, kk := range m.keys {
		if kk >= start && kk < limit {
			if prev, ok := m.data[kk]; ok {
				prevKVs = append(prevKVs, [2][]byte{[]byte(kk), clone(prev)})
				delete(m.data, kk)
				deleted++
			}
			continue
		}
		kept = append(kept, kk)
	}
	m.keys = kept
	return deleted, prevKVs
}

func (m *memStore) Range(key, end []byte, limit int64, keysOnly bool, countOnly bool) (kvs [][2][]byte, count int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if countOnly {
		// count without materializing values
		if len(end) == 0 {
			if _, ok := m.data[string(key)]; ok {
				return nil, 1
			}
			return nil, 0
		}
		start, lim := string(key), string(end)
		c := 0
		for _, kk := range m.keys {
			if kk >= start && kk < lim {
				c++
			}
		}
		return nil, c
	}

	var out [][2][]byte
	appendKV := func(k string) {
		if limit > 0 && int64(len(out)) >= limit {
			return
		}
		if v, ok := m.data[k]; ok {
			if keysOnly {
				out = append(out, [2][]byte{[]byte(k), nil})
			} else {
				out = append(out, [2][]byte{[]byte(k), clone(v)})
			}
		}
	}

	if len(end) == 0 {
		appendKV(string(key))
		return out, len(out)
	}

	start, lim := string(key), string(end)
	for _, kk := range m.keys {
		if kk >= start && kk < lim {
			appendKV(kk)
			if limit > 0 && int64(len(out)) >= limit {
				break
			}
		}
	}
	return out, len(out)
}

func (m *memStore) SnapshotBytes() []byte {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var buf bytes.Buffer
	for _, k := range m.keys {
		buf.WriteString(k)
		buf.WriteByte(0)
		buf.Write(m.data[k])
		buf.WriteByte('\n')
	}
	return buf.Bytes()
}

func clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// helpers (optional)
func PrefixEnd(prefix string) []byte {
	// etcd's Get with WithPrefix sets end to prefix+0xFF... but a common trick is
	// to increment the last byte. For simplicity, take ASCII and next rune.
	if prefix == "" {
		return nil
	}
	last := prefix[len(prefix)-1]
	return []byte(prefix[:len(prefix)-1] + string(last+1))
}

func HasPrefix(k, prefix string) bool { return strings.HasPrefix(k, prefix) }
