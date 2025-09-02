package mvcc

import (
	"bytes"
	"sync"
	"sync/atomic"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

// Event is a minimal watch event payload used by the hub.
type Event struct {
	Type     mvccpb.Event_EventType
	Key      []byte
	Value    []byte // nil for deletes
	Revision int64
}

type watcher struct {
	id  int64
	key []byte
	end []byte // if len==0, single key; else [key, end)
	ch  chan Event
}

type WatchHub struct {
	mu       sync.RWMutex
	watchers map[int64]*watcher
	nextID   int64
	rev      int64
}

func NewWatchHub() *WatchHub {
	return &WatchHub{
		watchers: make(map[int64]*watcher),
	}
}

// NextRevision increments and returns a global monotonically increasing revision.
func (h *WatchHub) NextRevision() int64 {
	return atomic.AddInt64(&h.rev, 1)
}

// Add registers a watcher for key or [key,end).
// Returns the watcher id, an events channel, and a cancel func.
func (h *WatchHub) Add(key, end []byte) (int64, <-chan Event, func()) {
	id := atomic.AddInt64(&h.nextID, 1)
	w := &watcher{
		id:  id,
		key: append([]byte(nil), key...),
		end: append([]byte(nil), end...),
		ch:  make(chan Event, 128),
	}
	h.mu.Lock()
	h.watchers[id] = w
	h.mu.Unlock()

	cancel := func() {
		h.mu.Lock()
		if ww, ok := h.watchers[id]; ok {
			delete(h.watchers, id)
			close(ww.ch)
		}
		h.mu.Unlock()
	}
	return id, w.ch, cancel
}

// Remove unregisters a watcher by id.
func (h *WatchHub) Remove(id int64) {
	h.mu.Lock()
	if w, ok := h.watchers[id]; ok {
		delete(h.watchers, id)
		close(w.ch)
	}
	h.mu.Unlock()
}

// Broadcast sends an event to all matching watchers (non-blocking).
func (h *WatchHub) Broadcast(ev Event) {
	h.mu.RLock()
	defer h.mu.RUnlock()
	for _, w := range h.watchers {
		if match(w, ev.Key) {
			select {
			case w.ch <- ev:
			default:
				// drop if the client is too slow; keeps server responsive
			}
		}
	}
}

func match(w *watcher, key []byte) bool {
	if len(w.end) == 0 {
		return bytes.Equal(key, w.key)
	}
	// range [key, end)
	return bytes.Compare(key, w.key) >= 0 && bytes.Compare(key, w.end) < 0
}
