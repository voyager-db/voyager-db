package cluster

import (
	"fmt"
	"sort"
	"strings"
	"sync"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// Manager is an in-process view of cluster membership (IDs, names, peer URLs).
// Phase A: static bootstrap from --initial-cluster.
// Phase B (M3): Add/Remove will be driven via Raft ConfChange and update this view.
type Manager struct {
	mu      sync.RWMutex
	members map[uint64]*etcdserverpb.Member // by ID
	byName  map[string]uint64               // name -> ID
}

// NewManagerFromInitial parses an etcd-style initial cluster string and returns a Manager.
// Format: "n1=http://host1:2380,n2=http://host2:2380,..."
// IDs are assigned in order (1..N) to match etcdctl expectations and your server.parseInitialMembers().
func NewManagerFromInitial(initial string) (*Manager, error) {
	m := &Manager{
		members: make(map[uint64]*etcdserverpb.Member),
		byName:  make(map[string]uint64),
	}
	if strings.TrimSpace(initial) == "" {
		return m, nil
	}
	pairs := strings.Split(initial, ",")
	for i, p := range pairs {
		sp := strings.SplitN(strings.TrimSpace(p), "=", 2)
		if len(sp) != 2 {
			return nil, fmt.Errorf("invalid initial-cluster fragment %q", p)
		}
		name := strings.TrimSpace(sp[0])
		url := strings.TrimSpace(sp[1])
		id := uint64(i + 1)
		m.members[id] = &etcdserverpb.Member{
			ID:       id,
			Name:     name,
			PeerURLs: []string{url},
		}
		m.byName[name] = id
	}
	return m, nil
}

// Members returns a stable-ordered slice (by ID) for RPC payloads and internal use.
func (m *Manager) Members() []*etcdserverpb.Member {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*etcdserverpb.Member, 0, len(m.members))
	for _, v := range m.members {
		// copy to avoid external mutation
		cp := *v
		cp.PeerURLs = append([]string(nil), v.PeerURLs...)
		cp.ClientURLs = append([]string(nil), v.ClientURLs...)
		out = append(out, &cp)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// IDByName returns the numeric ID for a given member name, or 0 if not found.
func (m *Manager) IDByName(name string) uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.byName[name]
}

// PeerURLByID returns the first peer URL for a given member ID (empty if none).
func (m *Manager) PeerURLByID(id uint64) string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if mb, ok := m.members[id]; ok && len(mb.PeerURLs) > 0 {
		return mb.PeerURLs[0]
	}
	return ""
}

// AddMember inserts a new member with a chosen ID and URLs.
// Note: In Phase B, ID allocation should be decided by the Raft leader.
func (m *Manager) AddMember(id uint64, name string, peerURLs, clientURLs []string) (*etcdserverpb.Member, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, exists := m.members[id]; exists {
		return nil, fmt.Errorf("member id %d exists", id)
	}
	if _, exists := m.byName[name]; exists {
		return nil, fmt.Errorf("member name %q exists", name)
	}
	mem := &etcdserverpb.Member{
		ID:         id,
		Name:       name,
		PeerURLs:   append([]string(nil), peerURLs...),
		ClientURLs: append([]string(nil), clientURLs...),
	}
	m.members[id] = mem
	m.byName[name] = id
	return mem, nil
}

func (m *Manager) RemoveMember(id uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if mb, ok := m.members[id]; ok {
		delete(m.byName, mb.Name)
		delete(m.members, id)
	}
}

func (m *Manager) UpdatePeerURLs(id uint64, peerURLs []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	mb, ok := m.members[id]
	if !ok {
		return fmt.Errorf("member id %d not found", id)
	}
	mb.PeerURLs = append([]string(nil), peerURLs...)
	return nil
}

func (m *Manager) NextID() uint64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var max uint64
	for id := range m.members {
		if id > max {
			max = id
		}
	}
	return max + 1
}
