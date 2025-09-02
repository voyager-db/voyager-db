package raft

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	raftpb "github.com/voyager-db/raftx/raftpb"
)

type Transport struct {
	selfID uint64
	mu     sync.RWMutex
	peers  map[uint64]string // id -> baseURL

	client *http.Client
	srv    *http.Server

	stepFn func(raftpb.Message) error
}

func NewTransport(selfID uint64, peers map[uint64]string) *Transport {
	m := make(map[uint64]string, len(peers))
	for id, u := range peers {
		m[id] = strings.TrimRight(u, "/")
	}
	return &Transport{
		selfID: selfID,
		peers:  m,
		client: &http.Client{Timeout: 5 * time.Second},
	}
}

func (t *Transport) Start(ctx context.Context, addr string, step func(raftpb.Message) error) error {
	t.stepFn = step
	mux := http.NewServeMux()
	mux.HandleFunc("/raft/step", t.handleStep)

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("transport listen: %w", err)
	}
	t.srv = &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	go func() { <-ctx.Done(); _ = t.srv.Shutdown(context.Background()) }()
	go func() { _ = t.srv.Serve(ln) }()
	return nil
}

func (t *Transport) handleStep(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read", http.StatusBadRequest)
		return
	}
	var m raftpb.Message
	if err := proto.Unmarshal(body, &m); err != nil {
		http.Error(w, "unmarshal", http.StatusBadRequest)
		return
	}
	if t.stepFn == nil {
		http.Error(w, "step unavailable", http.StatusServiceUnavailable)
		return
	}
	if err := t.stepFn(m); err != nil {
		http.Error(w, "step error", http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusNoContent)
}

func (t *Transport) Send(msgs []raftpb.Message) {
	t.mu.RLock()
	peers := t.peers
	t.mu.RUnlock()

	for _, m := range msgs {
		if m.To == t.selfID {
			continue
		}
		base, ok := peers[m.To]
		if !ok || base == "" {
			continue
		}
		b, err := proto.Marshal(&m)
		if err != nil {
			continue
		}
		req, _ := http.NewRequest(http.MethodPost, base+"/raft/step", bytes.NewReader(b))
		req.Header.Set("Content-Type", "application/x-protobuf")
		resp, err := t.client.Do(req)
		if err == nil && resp != nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}
}

// Dynamic updates after ConfChange apply
func (t *Transport) AddPeer(id uint64, baseURL string) {
	t.mu.Lock()
	t.peers[id] = strings.TrimRight(baseURL, "/")
	t.mu.Unlock()
}
func (t *Transport) RemovePeer(id uint64) {
	t.mu.Lock()
	delete(t.peers, id)
	t.mu.Unlock()
}
