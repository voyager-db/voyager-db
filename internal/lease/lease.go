package lease

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrNotFound = errors.New("lease not found")
)

type Manager struct {
	mu       sync.RWMutex
	leases   map[int64]*lease
	keys     map[int64]map[string]struct{} // leaseID -> set(key)
	nextID   int64
	interval time.Duration
	stopCh   chan struct{}
	wg       sync.WaitGroup

	expiredCh chan int64
}

type lease struct {
	id       int64
	ttlSec   int64
	expireAt time.Time
}

func NewManager() *Manager {
	m := &Manager{
		leases:    make(map[int64]*lease),
		keys:      make(map[int64]map[string]struct{}),
		interval:  time.Second,
		stopCh:    make(chan struct{}),
		expiredCh: make(chan int64, 1024),
	}
	m.wg.Add(1)
	go m.reaper()
	return m
}

func (m *Manager) Close() {
	close(m.stopCh)
	m.wg.Wait()
}

func (m *Manager) Expired() <-chan int64 { return m.expiredCh }

func (m *Manager) reaper() {
	defer m.wg.Done()
	t := time.NewTicker(m.interval)
	defer t.Stop()
	for {
		select {
		case <-m.stopCh:
			return
		case now := <-t.C:
			var toExpire []int64
			m.mu.Lock()
			for id, l := range m.leases {
				if now.After(l.expireAt) {
					toExpire = append(toExpire, id)
					delete(m.leases, id)
				}
			}
			m.mu.Unlock()
			for _, id := range toExpire {
				select {
				case m.expiredCh <- id:
				default:
				}
			}
		}
	}
}

func (m *Manager) Grant(ttlSec int64) (id int64, expireAt time.Time) {
	if ttlSec <= 0 {
		ttlSec = 1
	}
	id = atomic.AddInt64(&m.nextID, 1)
	now := time.Now()
	l := &lease{id: id, ttlSec: ttlSec, expireAt: now.Add(time.Duration(ttlSec) * time.Second)}
	m.mu.Lock()
	m.leases[id] = l
	if _, ok := m.keys[id]; !ok {
		m.keys[id] = make(map[string]struct{})
	}
	m.mu.Unlock()
	return id, l.expireAt
}

func (m *Manager) KeepAlive(id int64) (remaining int64, ttl int64, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	l, ok := m.leases[id]
	if !ok {
		return 0, 0, ErrNotFound
	}
	l.expireAt = time.Now().Add(time.Duration(l.ttlSec) * time.Second)
	return int64(time.Until(l.expireAt).Seconds()), l.ttlSec, nil
}

func (m *Manager) TTL(id int64) (Info, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	l, ok := m.leases[id]
	if !ok {
		return Info{}, ErrNotFound
	}
	rem := time.Until(l.expireAt)
	if rem < 0 {
		rem = 0
	}
	return Info{
		ID: id, TTL: l.ttlSec, ExpireAt: l.expireAt, Remaining: int64(rem.Seconds()),
	}, nil
}

func (m *Manager) Revoke(id int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.leases, id)
	// push to expiry channel so server can delete keys
	select {
	case m.expiredCh <- id:
	default:
	}
	return nil
}

// --- key tracking ---

func (m *Manager) AttachKey(id int64, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.leases[id]; !ok {
		return ErrNotFound
	}
	s, ok := m.keys[id]
	if !ok {
		s = make(map[string]struct{})
		m.keys[id] = s
	}
	s[string(key)] = struct{}{}
	return nil
}

func (m *Manager) DetachKey(id int64, key []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.keys[id]; ok {
		delete(s, string(key))
		if len(s) == 0 {
			delete(m.keys, id)
		}
	}
	return nil
}

func (m *Manager) Keys(id int64) ([][]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	s, ok := m.keys[id]
	if !ok {
		return nil, nil
	}
	out := make([][]byte, 0, len(s))
	for k := range s {
		out = append(out, []byte(k))
	}
	return out, nil
}
