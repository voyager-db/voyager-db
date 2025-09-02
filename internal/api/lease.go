package api

import (
	"context"
	"io"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/voyager-db/voyager-db/internal/lease"
)

type Lease struct {
	etcdserverpb.UnimplementedLeaseServer
	mgr lease.ManagerAPI
}

func NewLeaseWithManager(m lease.ManagerAPI) *Lease { return &Lease{mgr: m} }

// fallback (use only if server didn't wire a manager)
func NewLease() *Lease { return &Lease{mgr: lease.NewManager()} }

func (l *Lease) LeaseGrant(ctx context.Context, r *etcdserverpb.LeaseGrantRequest) (*etcdserverpb.LeaseGrantResponse, error) {
	if r == nil {
		return nil, status.Errorf(codes.InvalidArgument, "nil LeaseGrantRequest")
	}
	id, _ := l.mgr.Grant(r.TTL)
	return &etcdserverpb.LeaseGrantResponse{
		ID:  id,
		TTL: r.TTL,
	}, nil
}

func (l *Lease) LeaseKeepAlive(srv etcdserverpb.Lease_LeaseKeepAliveServer) error { // NOTE: fix typo → etcdserverpb
	// Correct signature below; I keep a wrapper to avoid compile surprise:
	return l.leaseKeepAliveImpl(srv)
}

func (l *Lease) leaseKeepAliveImpl(srv etcdserverpb.Lease_LeaseKeepAliveServer) error {
	// etcd allows multiple lease IDs on one stream; we respond per request.
	for {
		req, err := srv.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Unavailable, "keepalive recv: %v", err)
		}
		remaining, ttl, kaErr := l.mgr.KeepAlive(req.ID)
		if kaErr == lease.ErrNotFound {
			// Send a TTL of 0 to indicate expired/missing (matches etcd semantics).
			if sendErr := srv.Send(&etcdserverpb.LeaseKeepAliveResponse{ID: req.ID, TTL: 0}); sendErr != nil {
				return status.Errorf(codes.Unavailable, "keepalive send: %v", sendErr)
			}
			continue
		}
		if kaErr != nil {
			return status.Errorf(codes.Internal, "keepalive: %v", kaErr)
		}
		// etcd rounds TTL to int64 seconds.
		if sendErr := srv.Send(&etcdserverpb.LeaseKeepAliveResponse{
			ID:  req.ID,
			TTL: int64(remaining), // remaining seconds
		}); sendErr != nil {
			return status.Errorf(codes.Unavailable, "keepalive send: %v", sendErr)
		}
		_ = ttl // reserved if we later want to echo configured TTL
	}
}

func (l *Lease) LeaseRevoke(ctx context.Context, r *etcdserverpb.LeaseRevokeRequest) (*etcdserverpb.LeaseRevokeResponse, error) {
	if r == nil {
		return nil, status.Errorf(codes.InvalidArgument, "nil LeaseRevokeRequest")
	}
	if err := l.mgr.Revoke(r.ID); err == lease.ErrNotFound {
		// etcd treats revoke of missing lease as OK.
		return &etcdserverpb.LeaseRevokeResponse{}, nil
	} else if err != nil {
		return nil, status.Errorf(codes.Internal, "revoke: %v", err)
	}
	return &etcdserverpb.LeaseRevokeResponse{}, nil
}

func (l *Lease) LeaseTimeToLive(ctx context.Context, r *etcdserverpb.LeaseTimeToLiveRequest) (*etcdserverpb.LeaseTimeToLiveResponse, error) {
	if r == nil {
		return nil, status.Errorf(codes.InvalidArgument, "nil LeaseTimeToLiveRequest")
	}
	info, err := l.mgr.TTL(r.ID)
	if err == lease.ErrNotFound {
		// Per etcd, return TTL=-1 when not found.
		return &etcdserverpb.LeaseTimeToLiveResponse{
			ID:  r.ID,
			TTL: -1,
		}, nil
	}
	if err != nil {
		return nil, status.Errorf(codes.Internal, "ttl: %v", err)
	}
	// GrantedTTL is the configured TTL; Remaining TTL is dynamic.
	return &etcdserverpb.LeaseTimeToLiveResponse{
		ID:         info.ID,
		TTL:        info.Remaining,
		GrantedTTL: info.TTL,
		// Keys: we don't attach keys yet; MVCC will populate these when leases are bound to keys.
	}, nil
}

// Optional helper if you want a server-side pinger later (not used now).
func sleepCtx(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
