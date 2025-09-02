package api

import (
	"io"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"go.etcd.io/etcd/api/v3/etcdserverpb"
	"go.etcd.io/etcd/api/v3/mvccpb"

	"github.com/voyager-db/voyager-db/internal/mvcc"
)

type Watch struct {
	etcdserverpb.UnimplementedWatchServer
	hub    *mvcc.WatchHub
	header *etcdserverpb.ResponseHeader
}

func NewWatch(h *mvcc.WatchHub) *Watch {
	return &Watch{
		hub:    h,
		header: &etcdserverpb.ResponseHeader{}, // non-nil header keeps clients happy
	}
}

// Watch is a bi-di stream; handle Create and Cancel.
// All sends are serialized via sendMu to satisfy gRPC stream safety.
func (w *Watch) Watch(srv etcdserverpb.Watch_WatchServer) error {
	type sub struct {
		id     int64
		cancel func()
	}
	subs := map[int64]sub{}
	var sendMu sync.Mutex

	send := func(resp *etcdserverpb.WatchResponse) error {
		sendMu.Lock()
		defer sendMu.Unlock()
		if resp.Header == nil {
			resp.Header = w.header
		}
		return srv.Send(resp)
	}

	defer func() {
		for _, s := range subs {
			s.cancel()
		}
	}()

	for {
		req, err := srv.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return status.Errorf(codes.Unavailable, "recv watch req: %v", err)
		}

		if creq := req.GetCreateRequest(); creq != nil {
			id, ch, cancel := w.hub.Add(creq.Key, creq.RangeEnd)
			subs[id] = sub{id: id, cancel: cancel}

			// Ack creation
			if err := send(&etcdserverpb.WatchResponse{
				Header:  w.header,
				WatchId: id,
				Created: true,
			}); err != nil {
				cancel()
				delete(subs, id)
				return status.Errorf(codes.Unavailable, "send created: %v", err)
			}

			// Pump events (serialize sends)
			go func(wid int64, evch <-chan mvcc.Event) {
				for ev := range evch {
					etype := mvccpb.PUT
					if ev.Type == mvccpb.DELETE {
						etype = mvccpb.DELETE
					}
					resp := &etcdserverpb.WatchResponse{
						Header:  w.header,
						WatchId: wid,
						Events: []*mvccpb.Event{{
							Type: etype,
							Kv: &mvccpb.KeyValue{
								Key:         ev.Key,
								Value:       ev.Value,
								ModRevision: ev.Revision,
							},
						}},
					}
					if err := send(resp); err != nil {
						cancel()
						return
					}
				}
			}(id, ch)
			continue
		}

		if creq := req.GetCancelRequest(); creq != nil {
			id := creq.WatchId
			if s, ok := subs[id]; ok {
				s.cancel()
				delete(subs, id)
			}
			_ = send(&etcdserverpb.WatchResponse{
				Header:   w.header,
				WatchId:  id,
				Canceled: true,
			})
			continue
		}

		// Progress requests can be handled later; ignore for now.
	}
}
