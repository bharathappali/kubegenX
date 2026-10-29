package export

import (
	"context"
	"fmt"
	"time"

	"github.com/bharathappali/kubegenx/internal/planner"
	"github.com/bharathappali/kubegenx/internal/samples"
)

// BlockJob is one independently writable 2h block: a single parquet item
// over one 2h slice. Workers claim these and write them in parallel.
type BlockJob struct {
	ID     string
	Chunk  int
	Index  int
	WlLo   int
	WlHi   int
	StepLo int
	StepHi int
	EmitNS bool
}

// BlockJobs lists every 2h block in the plan. A 6h item becomes three jobs.
func BlockJobs(plan *planner.Plan) []BlockJob {
	if plan == nil {
		return nil
	}
	var out []BlockJob
	for _, it := range plan.Items {
		for _, sl := range itemSlices(plan, it) {
			out = append(out, BlockJob{
				ID:     fmt.Sprintf("c%04d-i%06d-s%d", it.Chunk, it.Index, sl.lo),
				Chunk:  it.Chunk,
				Index:  it.Index,
				WlLo:   it.WlLo,
				WlHi:   it.WlHi,
				StepLo: sl.lo,
				StepHi: sl.hi,
				EmitNS: sl.emitNS,
			})
		}
	}
	return out
}

// ConvertBlock reads one item's parquet and writes its 2h block onto destRoot.
func ConvertBlock(ctx context.Context, plan *planner.Plan, datasetDir, scratch, exportName, destRoot string, origin time.Time, external map[string]string, job BlockJob) (int64, error) {
	if ctx.Err() != nil {
		return 0, ctx.Err()
	}
	cpu, err := samples.ReadCPUFile(samples.ItemFile(datasetDir, "cpu", job.Chunk, job.Index))
	if err != nil {
		return 0, err
	}
	mem, err := samples.ReadMemFile(samples.ItemFile(datasetDir, "memory", job.Chunk, job.Index))
	if err != nil {
		return 0, err
	}
	gpu, err := samples.ReadGPUFile(samples.ItemFile(datasetDir, "gpu", job.Chunk, job.Index))
	if err != nil {
		return 0, err
	}
	dropCache(samples.ItemFile(datasetDir, "cpu", job.Chunk, job.Index))
	dropCache(samples.ItemFile(datasetDir, "memory", job.Chunk, job.Index))
	dropCache(samples.ItemFile(datasetDir, "gpu", job.Chunk, job.Index))
	sec := time.Duration(plan.StepSeconds) * time.Second
	lo := origin.Add(time.Duration(job.StepLo) * sec)
	hi := origin.Add(time.Duration(job.StepHi) * sec)
	_, sz, err := writeBlock(ctx, plan, cpu, mem, gpu, scratch, exportName, job.ID, origin, lo, hi, destRoot, external, job.WlLo, job.WlHi, job.StepLo, job.StepHi, job.EmitNS)
	return sz, err
}
