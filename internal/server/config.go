package server

import (
	"time"

	"go.uber.org/zap"
)

type Config struct {
	Name            string
	DataDir         string
	ClientListen    string
	PeerListen      string
	AdvertiseClient string
	AdvertisePeer   string
	InitialCluster  string
	// Optional explicit cluster ID. If 0, Voyager derives & persists one.
	ClusterID uint64

	EnableFastPath   bool
	EnableCRaft      bool
	ElectionTick     int
	HeartbeatTick    int
	SnapshotInterval time.Duration

	RaftSnapshotEvery uint64
	RaftCompactKeep   uint64

	Logger *zap.Logger
}
