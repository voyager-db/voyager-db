package api

import (
	"context"
	"crypto/sha256"
	"io"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
)

// StatusProvider lives in internal/server to avoid import cycles.
type StatusProvider interface {
	Version() string
	LeaderID() uint64
	RaftIndex() uint64
	RaftTerm() uint64
	RaftAppliedIndex() uint64
	DbSize() int64
	DbSizeInUse() int64
	IsLearner() bool
	HashKV() (hash uint32, compactRev int64, rev int64)
	Header() *etcdserverpb.ResponseHeader

	// For Snapshot streaming (Phase A: stream state.db or raft.db)
	SnapshotOpen(ctx context.Context) (io.ReadCloser, error)
}

type Maintenance struct {
	etcdserverpb.UnimplementedMaintenanceServer
	prov StatusProvider
}

func NewMaintenance(p StatusProvider) *Maintenance { return &Maintenance{prov: p} }

// Unary → return (resp, nil) on success
func (m *Maintenance) Status(ctx context.Context, _ *etcdserverpb.StatusRequest) (*etcdserverpb.StatusResponse, error) {
	resp := &etcdserverpb.StatusResponse{
		Header:           m.prov.Header(),
		Version:          m.prov.Version(),
		DbSize:           m.prov.DbSize(),
		DbSizeInUse:      m.prov.DbSizeInUse(),
		RaftIndex:        m.prov.RaftIndex(),
		RaftTerm:         m.prov.RaftTerm(),
		RaftAppliedIndex: m.prov.RaftAppliedIndex(),
		Leader:           m.prov.LeaderID(),
		IsLearner:        m.prov.IsLearner(),
	}
	return resp, nil
}

func (m *Maintenance) HashKV(ctx context.Context, _ *etcdserverpb.HashKVRequest) (*etcdserverpb.HashKVResponse, error) {
	hash, compactRev, rev := m.prov.HashKV()
	resp := &etcdserverpb.HashKVResponse{
		Header:          m.prov.Header(),
		Hash:            uint32(hash),
		CompactRevision: compactRev,
		HashRevision:    rev,
	}
	return resp, nil
}

func (m *Maintenance) Defragment(ctx context.Context, _ *etcdserverpb.DefragmentRequest) (*etcdserverpb.DefragmentResponse, error) {
	return &etcdserverpb.DefragmentResponse{Header: m.prov.Header()}, nil
}

// Snapshot: unary request → server stream response.
// Stream file bytes in chunks; then append a final message with a SHA-256 checksum blob.
func (m *Maintenance) Snapshot(_ *etcdserverpb.SnapshotRequest, srv etcdserverpb.Maintenance_SnapshotServer) error {
	r, err := m.prov.SnapshotOpen(srv.Context())
	if err != nil {
		return status.Errorf(codes.Internal, "open snapshot: %v", err)
	}
	defer r.Close()

	const chunk = 64 * 1024
	buf := make([]byte, chunk)
	h := sha256.New()

	for {
		// Respect cancelation
		select {
		case <-srv.Context().Done():
			return status.Errorf(codes.Canceled, "snapshot canceled")
		default:
		}

		n, readErr := r.Read(buf)
		if n > 0 {
			// feed hasher and send chunk
			_, _ = h.Write(buf[:n])
			if err := srv.Send(&etcdserverpb.SnapshotResponse{
				Header: m.prov.Header(),
				Blob:   buf[:n],
			}); err != nil {
				return status.Errorf(codes.Unavailable, "send snapshot: %v", err)
			}
		}
		if readErr == io.EOF {
			// Send the checksum trailer as a final message
			sum := h.Sum(nil) // 32 bytes
			if err := srv.Send(&etcdserverpb.SnapshotResponse{
				Header: m.prov.Header(),
				Blob:   sum,
			}); err != nil {
				return status.Errorf(codes.Unavailable, "send checksum: %v", err)
			}
			return nil
		}
		if readErr != nil {
			return status.Errorf(codes.Internal, "read snapshot: %v", readErr)
		}
	}
}

func (m *Maintenance) Alarm(ctx context.Context, _ *etcdserverpb.AlarmRequest) (*etcdserverpb.AlarmResponse, error) {
	return &etcdserverpb.AlarmResponse{Header: m.prov.Header()}, nil
}

func (m *Maintenance) MoveLeader(ctx context.Context, _ *etcdserverpb.MoveLeaderRequest) (*etcdserverpb.MoveLeaderResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "MoveLeader not implemented")
}
