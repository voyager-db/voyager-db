package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/pflag"
	"go.uber.org/zap"

	"github.com/voyager-db/voyager-db/internal/server"
)

func main() {
	var cfg server.Config

	pflag.StringVar(&cfg.Name, "name", hostnameOr("voyager"), "node name")
	pflag.StringVar(&cfg.DataDir, "data-dir", "./data", "data directory")
	pflag.StringVar(&cfg.ClientListen, "client-listen", ":2379", "client listen address (host:port)")
	pflag.StringVar(&cfg.PeerListen, "peer-listen", ":2380", "peer listen address (host:port)")
	pflag.StringVar(&cfg.InitialCluster, "initial-cluster", "", "static bootstrap cluster, e.g. 'n1=http://voyager1:2380,n2=http://voyager2:2380,n3=http://voyager3:2380'")
	pflag.StringVar(&cfg.AdvertiseClient, "advertise-client", "", "advertise client URL (optional)")
	pflag.StringVar(&cfg.AdvertisePeer, "advertise-peer", "", "advertise peer URL (optional)")
	pflag.StringVar(&cfg.ClusterToken, "cluster-token", "voyager-cluster", "cluster token")
	pflag.BoolVar(&cfg.EnableFastPath, "enable-fast-path", false, "enable Raftx fast path")
	pflag.BoolVar(&cfg.EnableCRaft, "enable-c-raft", false, "enable C-Raft global tier")
	pflag.DurationVar(&cfg.SnapshotInterval, "snapshot-interval", 30*time.Second, "periodic snapshot interval")
	pflag.IntVar(&cfg.ElectionTick, "election-tick", 10, "raft election tick")
	pflag.IntVar(&cfg.HeartbeatTick, "heartbeat-tick", 1, "raft heartbeat tick")
	pflag.Uint64Var(&cfg.RaftSnapshotEvery, "raft-snapshot-every", 10000, "create a raft snapshot every N applied entries")
	pflag.Uint64Var(&cfg.RaftCompactKeep, "raft-compact-keep", 5000, "keep last K entries after raft snapshot compaction")
	pflag.Parse()

	logger, _ := zap.NewProduction()
	defer logger.Sync()
	cfg.Logger = logger

	s, err := server.New(cfg)
	if err != nil {
		panic(err)
	}

	ctx, cancel := signalContext()
	defer cancel()

	if err := s.Start(ctx); err != nil {
		panic(err)
	}

	<-ctx.Done()
	s.GracefulStop(context.Background())
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-ch
		cancel()
	}()
	return ctx, cancel
}

func hostnameOr(def string) string {
	h, err := os.Hostname()
	if err != nil || h == "" {
		return def
	}
	return h
}
