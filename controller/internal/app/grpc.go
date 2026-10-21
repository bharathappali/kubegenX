package app

import (
	"context"
	"time"

	"github.com/bharathappali/kubegenx/internal/leasev1"
	"google.golang.org/grpc"
)

type queue struct {
	leasev1.UnimplementedLeaseQueueServer
	app *App
}

func (q *queue) Register(ctx context.Context, req *leasev1.RegisterRequest) (*leasev1.RegisterResponse, error) {
	return &leasev1.RegisterResponse{PlanUri: "/data/datasets/" + req.Dataset + "/plan/generation.json", PlanDigest: req.Dataset, LeaseTtlSeconds: 600}, nil
}

func (q *queue) Lease(ctx context.Context, req *leasev1.LeaseRequest) (*leasev1.LeaseResponse, error) {
	items, drained, err := q.app.Store.Lease(req.Dataset, req.WorkerId, int(req.N), 10*time.Minute)
	if err != nil {
		return nil, err
	}
	out := &leasev1.LeaseResponse{Drained: drained}
	for _, it := range items {
		out.Items = append(out.Items, &leasev1.Item{
			Id: it.ID, WlLo: int32(it.WlLo), WlHi: int32(it.WlHi), Chunk: int32(it.Chunk), Attempt: int32(it.Attempt), ItemIndex: int32(it.ItemIndex),
		})
	}
	return out, nil
}

func (q *queue) Heartbeat(ctx context.Context, req *leasev1.HeartbeatRequest) (*leasev1.HeartbeatResponse, error) {
	if err := q.app.Store.Heartbeat(req.Dataset, req.WorkerId, 10*time.Minute); err != nil {
		return nil, err
	}
	return &leasev1.HeartbeatResponse{Ok: true}, nil
}

func (q *queue) Progress(ctx context.Context, req *leasev1.ProgressRequest) (*leasev1.ProgressResponse, error) {
	q.app.publish(req.Dataset)
	return &leasev1.ProgressResponse{Ok: true}, nil
}

func (q *queue) Complete(ctx context.Context, req *leasev1.CompleteRequest) (*leasev1.CompleteResponse, error) {
	var bytes, rows int64
	for _, f := range req.Files {
		bytes += f.Bytes
		rows += f.Rows
	}
	if err := q.app.Store.Complete(req.Dataset, req.ItemId, bytes, rows); err != nil {
		return nil, err
	}
	q.app.publish(req.Dataset)
	return &leasev1.CompleteResponse{Ok: true}, nil
}

func (q *queue) Fail(ctx context.Context, req *leasev1.FailRequest) (*leasev1.FailResponse, error) {
	if err := q.app.Store.Fail(req.Dataset, req.ItemId, req.WorkerId, req.Error, req.Retryable); err != nil {
		return nil, err
	}
	q.app.publish(req.Dataset)
	return &leasev1.FailResponse{Ok: true}, nil
}

func RegisterGRPC(s *grpc.Server, a *App) {
	leasev1.RegisterLeaseQueueServer(s, &queue{app: a})
}
