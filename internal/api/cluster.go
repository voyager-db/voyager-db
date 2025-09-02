package api

import (
	"context"
	"fmt"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.etcd.io/etcd/api/v3/etcdserverpb"

	"github.com/voyager-db/voyager-db/internal/cluster"
	"github.com/voyager-db/voyager-db/internal/raft"
)

type Cluster struct {
	mgr  *cluster.Manager
	prop *raft.Proposer
}

func NewCluster(mgr *cluster.Manager, prop *raft.Proposer) *Cluster {
	return &Cluster{mgr: mgr, prop: prop}
}

func (c *Cluster) MemberList(ctx context.Context, _ *etcdserverpb.MemberListRequest) (*etcdserverpb.MemberListResponse, error) {
	return &etcdserverpb.MemberListResponse{Members: c.mgr.Members()}, nil
}

func (c *Cluster) MemberAdd(ctx context.Context, r *etcdserverpb.MemberAddRequest) (*etcdserverpb.MemberAddResponse, error) {
	if r == nil || len(r.PeerURLs) == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "peerURLs required")
	}
	if c.prop == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "raft not ready")
	}

	id := c.mgr.NextID()
	genName := fmt.Sprintf("m-%d", id) // cosmetic display name

	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	if err := c.prop.ProposeAddMember(ctx2, id, genName, r.PeerURLs, nil, r.IsLearner); err != nil {
		return nil, status.Errorf(codes.Unavailable, "member add: %v", err)
	}
	members := c.mgr.Members()
	return &etcdserverpb.MemberAddResponse{Member: findMember(members, id), Members: members}, nil
}

func (c *Cluster) MemberRemove(ctx context.Context, r *etcdserverpb.MemberRemoveRequest) (*etcdserverpb.MemberRemoveResponse, error) {
	if r == nil || r.ID == 0 {
		return nil, status.Errorf(codes.InvalidArgument, "id required")
	}
	if c.prop == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "raft not ready")
	}
	ctx2, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := c.prop.ProposeRemoveMember(ctx2, r.ID); err != nil {
		return nil, status.Errorf(codes.Unavailable, "member remove: %v", err)
	}
	return &etcdserverpb.MemberRemoveResponse{Members: c.mgr.Members()}, nil
}

func (c *Cluster) MemberUpdate(ctx context.Context, _ *etcdserverpb.MemberUpdateRequest) (*etcdserverpb.MemberUpdateResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "MemberUpdate not implemented yet")
}

func (c *Cluster) MemberPromote(ctx context.Context, r *etcdserverpb.MemberPromoteRequest) (*etcdserverpb.MemberPromoteResponse, error) {
	return nil, status.Errorf(codes.Unimplemented, "MemberPromote not implemented yet")
}

func findMember(ms []*etcdserverpb.Member, id uint64) *etcdserverpb.Member {
	for _, m := range ms {
		if m.ID == id {
			return m
		}
	}
	return nil
}
