package server

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"

	"google.golang.org/grpc"

	raftx "github.com/voyager-db/raftx"
	raftpb "github.com/voyager-db/raftx/raftpb"
	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/voyager-db/voyager-db/internal/api"
	"github.com/voyager-db/voyager-db/internal/cluster"
	"github.com/voyager-db/voyager-db/internal/lease"
	"github.com/voyager-db/voyager-db/internal/mvcc"
	"github.com/voyager-db/voyager-db/internal/raft"
	boltstore "github.com/voyager-db/voyager-db/internal/storage/bolt"
)

const version = "voyager/0.0.1"

type Server struct {
	cfg     Config
	grpcSrv *grpc.Server
	httpSrv *http.Server

	leaseMgr lease.ManagerAPI
	mv       mvcc.KV
	prop     *raft.Proposer

	wal *raft.WAL
}

func New(cfg Config) (*Server, error) { return &Server{cfg: cfg}, nil }

func (s *Server) Start(ctx context.Context) error {
	lis, err := net.Listen("tcp", s.cfg.ClientListen)
	if err != nil {
		return fmt.Errorf("listen client: %w", err)
	}

	// ---- Storage (MVCC) : Bolt preferred ----
	var boltKV *boltstore.Store
	if s.cfg.DataDir != "" {
		_ = os.MkdirAll(s.cfg.DataDir, 0o755)
		path := filepath.Join(s.cfg.DataDir, "state.db")
		if bs, err := boltstore.New(path); err == nil {
			boltKV = bs
			if s.mv, err = mvcc.OpenBolt(bs.DB()); err != nil {
				return fmt.Errorf("open mvcc: %w", err)
			}
		} else {
			log.Printf("storage: bolt open failed, using memory: %v", err)
			s.mv = mvcc.NewMemory()
		}
	} else {
		s.mv = mvcc.NewMemory()
	}

	// ---- Lease manager (Bolt-backed if available) ----
	if boltKV != nil {
		if bm, err := lease.NewBoltManager(boltKV.DB()); err == nil {
			s.leaseMgr = bm
		} else {
			log.Printf("lease: bolt manager init failed, using memory: %v", err)
			s.leaseMgr = lease.NewManager()
		}
	} else {
		s.leaseMgr = lease.NewManager()
	}

	// ---- Watch hub ----
	hub := mvcc.NewWatchHub()

	// ---- Cluster manager from --initial-cluster ----
	mgr, err := cluster.NewManagerFromInitial(s.cfg.InitialCluster)
	if err != nil {
		return fmt.Errorf("cluster init: %w", err)
	}

	idByName := map[string]uint64{}
	peerURLs := map[uint64]string{}
	var peers []raftx.Peer
	for _, m := range mgr.Members() {
		idByName[m.Name] = m.ID
		peers = append(peers, raftx.Peer{ID: m.ID})
		if len(m.PeerURLs) > 0 {
			peerURLs[m.ID] = m.PeerURLs[0] // one URL per member for now
		}
	}

	selfID := idByName[s.cfg.Name]
	if selfID == 0 {
		// Dev fallback: single node
		selfID = 1
		peers = []raftx.Peer{{ID: selfID}}
		log.Printf("cluster: name %q not in initial-cluster, starting single node with ID=%d", s.cfg.Name, selfID)
	}

	// ---- Start Raft node (with WAL) ----
	rcfg := raft.Config{
		ID:              selfID,
		ElectionTick:    10,
		HeartbeatTick:   1,
		MaxSizePerMsg:   64 * 1024,
		MaxInflightMsgs: 256,
		EnableFastPath:  s.cfg.EnableFastPath,
		EnableCRaft:     s.cfg.EnableCRaft,
	}

	if s.cfg.DataDir != "" {
		walPath := filepath.Join(s.cfg.DataDir, "raft.db")
		if w, err := raft.OpenWAL(walPath); err == nil {
			s.wal = w
		} else {
			log.Printf("raft WAL: open failed, running memory-only: %v", err)
		}
	}

	node := raft.NewNodeWithWAL(rcfg, peers, s.wal)

	// ---- Transport over HTTP (peer listen) ----
	baseMap := map[uint64]string{}
	for id, u := range peerURLs {
		baseMap[id] = strings.TrimRight(u, "/")
	}
	tr := raft.NewTransport(selfID, baseMap)
	if err := tr.Start(ctx, s.cfg.PeerListen, func(m raftpb.Message) error { return node.Step(m) }); err != nil {
		return fmt.Errorf("transport start: %w", err)
	}

	// ---- Proposer & apply loop (broadcasts, leases, MVCC) ----
	applier := &raft.Applier{Store: s.mv, Leases: s.leaseMgr, Hub: hub}
	s.prop = raft.NewProposer(node, applier, tr, s.cfg.RaftSnapshotEvery, s.cfg.RaftCompactKeep, mgr)
	go func() { _ = s.prop.Run(ctx) }()

	// ---- Maintenance provider (after Raft is running to expose live stats) ----
	var snapFn func(context.Context) (io.ReadCloser, error)
	if boltKV != nil {
		snapFn = boltKV.SnapshotOpen
	}
	maintProv := newStatusProviderWithRaft(s.cfg, snapFn, s.prop)

	// ---- gRPC servers ----
	s.grpcSrv = grpc.NewServer()

	kvSrv := api.NewKVWithRaft(s.mv, s.prop)
	watch := api.NewWatch(hub)
	leaseSrv := api.NewLeaseWithManager(s.leaseMgr)
	clusterSvc := api.NewCluster(mgr, s.prop)

	maint := api.NewMaintenance(maintProv)

	etcdserverpb.RegisterKVServer(s.grpcSrv, kvSrv)
	etcdserverpb.RegisterWatchServer(s.grpcSrv, watch)
	etcdserverpb.RegisterLeaseServer(s.grpcSrv, leaseSrv)
	etcdserverpb.RegisterClusterServer(s.grpcSrv, clusterSvc)
	etcdserverpb.RegisterMaintenanceServer(s.grpcSrv, maint)

	// ---- start services ----
	go func() { _ = s.grpcSrv.Serve(lis) }()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	s.httpSrv = &http.Server{Addr: ":2381", Handler: mux}
	go func() { _ = s.httpSrv.ListenAndServe() }()

	return nil
}

func (s *Server) GracefulStop(ctx context.Context) {
	if s.grpcSrv != nil {
		s.grpcSrv.GracefulStop()
	}
	if s.httpSrv != nil {
		_ = s.httpSrv.Shutdown(ctx)
	}
	if s.leaseMgr != nil {
		s.leaseMgr.Close()
	}
	if s.wal != nil {
		_ = s.wal.Close()
	}
}
