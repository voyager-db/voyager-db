package api

import (
	"bytes"
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/voyager-db/voyager-db/internal/mvcc"
	"github.com/voyager-db/voyager-db/internal/raft"
)

type KV struct {
	etcdserverpb.UnimplementedKVServer
	// Reads/compares use MVCC directly.
	mvcc mvcc.KV
	// Writes go through Raft (propose -> commit -> apply loop updates MVCC + broadcasts + lease side effects).
	prop *raft.Proposer
}

func NewKVWithRaft(m mvcc.KV, p *raft.Proposer) *KV {
	return &KV{mvcc: m, prop: p}
}

func (s *KV) Range(ctx context.Context, req *etcdserverpb.RangeRequest) (*etcdserverpb.RangeResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "nil RangeRequest")
	}
	// Linearizable by default unless client sets Serializable=true.
	if !req.Serializable && s.prop != nil {
		if err := s.prop.Barrier(ctx); err != nil {
			return nil, status.Errorf(codes.Unavailable, "linearizable read barrier: %v", err)
		}
	}
	kvs, count, err := s.mvcc.Range(ctx, mvcc.OpRange{
		Key:       req.Key,
		RangeEnd:  req.RangeEnd,
		Limit:     req.Limit,
		KeysOnly:  req.KeysOnly,
		CountOnly: req.CountOnly,
	})
	if err != nil {
		return nil, status.Errorf(codes.Internal, "range: %v", err)
	}
	return &etcdserverpb.RangeResponse{Kvs: kvs, Count: count}, nil
}

func (s *KV) Put(ctx context.Context, req *etcdserverpb.PutRequest) (*etcdserverpb.PutResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "nil PutRequest")
	}
	if s.prop == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "raft proposer not initialized")
	}
	// Propose via Raft; the apply loop:
	// - writes to MVCC,
	// - attaches lease (if any),
	// - broadcasts watch events.
	res, err := s.prop.Put(ctx, req.Key, req.Value, req.PrevKv, req.Lease)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "put: %v", err)
	}
	resp := &etcdserverpb.PutResponse{}
	if req.PrevKv && res != nil && res.Prev != nil {
		resp.PrevKv = res.Prev
	}
	return resp, nil
}

func (s *KV) DeleteRange(ctx context.Context, req *etcdserverpb.DeleteRangeRequest) (*etcdserverpb.DeleteRangeResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "nil DeleteRangeRequest")
	}
	if s.prop == nil {
		return nil, status.Errorf(codes.FailedPrecondition, "raft proposer not initialized")
	}
	// Propose via Raft; the apply loop:
	// - deletes in MVCC,
	// - detaches leases for deleted keys,
	// - broadcasts delete events.
	res, err := s.prop.DeleteRange(ctx, req.Key, req.RangeEnd, req.PrevKv)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "delete: %v", err)
	}
	out := &etcdserverpb.DeleteRangeResponse{}
	if res != nil {
		out.Deleted = res.Deleted
		if req.PrevKv && len(res.PrevKvs) > 0 {
			out.PrevKvs = res.PrevKvs
		}
	}
	return out, nil
}

func (s *KV) Txn(ctx context.Context, req *etcdserverpb.TxnRequest) (*etcdserverpb.TxnResponse, error) {
	if req == nil {
		return nil, status.Errorf(codes.InvalidArgument, "nil TxnRequest")
	}

	// Evaluate compares locally (MVCC). If empty, etcd treats it as true.
	succeeded, err := s.evalCompares(ctx, req.Compare)
	if err != nil {
		return nil, err
	}

	var ops []*etcdserverpb.RequestOp
	if succeeded {
		ops = req.Success
	} else {
		ops = req.Failure
	}

	resps := make([]*etcdserverpb.ResponseOp, 0, len(ops))
	for _, op := range ops {
		switch tv := op.Request.(type) {
		case *etcdserverpb.RequestOp_RequestPut:
			// This calls our Put above, which proposes via Raft.
			r, err := s.Put(ctx, tv.RequestPut)
			if err != nil {
				return nil, err
			}
			resps = append(resps, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponsePut{ResponsePut: r},
			})

		case *etcdserverpb.RequestOp_RequestRange:
			// Reads remain local for now.
			r, err := s.Range(ctx, tv.RequestRange)
			if err != nil {
				return nil, err
			}
			resps = append(resps, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseRange{ResponseRange: r},
			})

		case *etcdserverpb.RequestOp_RequestDeleteRange:
			// This calls our DeleteRange above, which proposes via Raft.
			r, err := s.DeleteRange(ctx, tv.RequestDeleteRange)
			if err != nil {
				return nil, err
			}
			resps = append(resps, &etcdserverpb.ResponseOp{
				Response: &etcdserverpb.ResponseOp_ResponseDeleteRange{ResponseDeleteRange: r},
			})

		default:
			return nil, status.Errorf(codes.Unimplemented, "txn op not implemented")
		}
	}

	return &etcdserverpb.TxnResponse{
		Succeeded: succeeded,
		Responses: resps,
	}, nil
}

// ----------------- compares -----------------

func (s *KV) evalCompares(ctx context.Context, cmps []*etcdserverpb.Compare) (bool, error) {
	if len(cmps) == 0 {
		return true, nil
	}
	for _, c := range cmps {
		ok, err := s.evalOne(ctx, c)
		if err != nil {
			return false, err
		}
		if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (s *KV) evalOne(ctx context.Context, c *etcdserverpb.Compare) (bool, error) {
	// Pull the current value/metadata for the key (single key lookup).
	kvs, _, err := s.mvcc.Range(ctx, mvcc.OpRange{Key: c.Key})
	if err != nil {
		return false, status.Errorf(codes.Internal, "compare read: %v", err)
	}
	var cur *mvccpb.KeyValue
	if len(kvs) > 0 {
		cur = kvs[0]
	}

	// If key missing, all numeric fields read as 0 and value as empty.
	var (
		version int64
		mod     int64
		create  int64
		value   []byte
	)
	if cur != nil {
		version = cur.Version
		mod = cur.ModRevision
		create = cur.CreateRevision
		value = cur.Value
	}

	// Compare.Target: VERSION/MOD/CREATE/VALUE
	switch c.Target {
	case etcdserverpb.Compare_VERSION:
		return applyOp(c, version, nil), nil
	case etcdserverpb.Compare_MOD:
		return applyOp(c, mod, nil), nil
	case etcdserverpb.Compare_CREATE:
		return applyOp(c, create, nil), nil
	case etcdserverpb.Compare_VALUE:
		return applyOp(c, 0, value), nil
	default:
		return false, status.Errorf(codes.Unimplemented, "compare target not implemented")
	}
}

func applyOp(c *etcdserverpb.Compare, num int64, bytesVal []byte) bool {
	switch v := c.TargetUnion.(type) {
	case *etcdserverpb.Compare_Version:
		return cmpNum(c.Result, num, v.Version)
	case *etcdserverpb.Compare_ModRevision:
		return cmpNum(c.Result, num, v.ModRevision)
	case *etcdserverpb.Compare_CreateRevision:
		return cmpNum(c.Result, num, v.CreateRevision)
	case *etcdserverpb.Compare_Value:
		return cmpBytes(c.Result, bytesVal, v.Value)
	default:
		return false
	}
}

func cmpNum(op etcdserverpb.Compare_CompareResult, a, b int64) bool {
	switch op {
	case etcdserverpb.Compare_EQUAL:
		return a == b
	case etcdserverpb.Compare_NOT_EQUAL:
		return a != b
	case etcdserverpb.Compare_GREATER:
		return a > b
	case etcdserverpb.Compare_LESS:
		return a < b
	default:
		return false
	}
}

func cmpBytes(op etcdserverpb.Compare_CompareResult, a, b []byte) bool {
	c := bytes.Compare(a, b)
	switch op {
	case etcdserverpb.Compare_EQUAL:
		return c == 0
	case etcdserverpb.Compare_NOT_EQUAL:
		return c != 0
	case etcdserverpb.Compare_GREATER:
		return c > 0
	case etcdserverpb.Compare_LESS:
		return c < 0
	default:
		return false
	}
}
