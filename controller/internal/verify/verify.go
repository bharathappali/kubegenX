package verify

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/bharathappali/kubegenx/internal/planner"
	"github.com/bharathappali/kubegenx/internal/samples"
)

type Report struct {
	Files    int `json:"files"`
	Expected int `json:"expected"`
	Rows     int `json:"rows"`
	Resets   int `json:"resets"`
}

func Dataset(dataDir, name string) (Report, error) {
	var rep Report
	dir := filepath.Join(dataDir, "datasets", name)
	raw, err := os.ReadFile(filepath.Join(dir, "plan", "generation.json"))
	if err != nil {
		return rep, err
	}
	var plan planner.Plan
	if err := json.Unmarshal(raw, &plan); err != nil {
		return rep, err
	}
	rep.Expected = ExpectedFiles(&plan)
	rep.Files, err = samples.CountParquet(dir)
	if err != nil {
		return rep, err
	}
	if rep.Files != rep.Expected {
		return rep, fmt.Errorf("parquet files %d, planned %d", rep.Files, rep.Expected)
	}
	rows, err := samples.ReadCPU(dir)
	if err != nil {
		return rep, err
	}
	rep.Rows = len(rows)
	by := map[uint32][]samples.CPURow{}
	for _, r := range rows {
		by[r.CtrID] = append(by[r.CtrID], r)
	}
	for id, rs := range by {
		sort.Slice(rs, func(i, j int) bool {
			if rs[i].StepIdx == rs[j].StepIdx {
				return rs[i].LifeSeq < rs[j].LifeSeq
			}
			return rs[i].StepIdx < rs[j].StepIdx
		})
		for i := 1; i < len(rs); i++ {
			prev, cur := rs[i-1], rs[i]
			if cur.StepIdx <= prev.StepIdx {
				return rep, fmt.Errorf("ctr %d step did not advance %d -> %d", id, prev.StepIdx, cur.StepIdx)
			}
			if cur.LifeSeq == prev.LifeSeq {
				if cur.UsageUs <= prev.UsageUs {
					return rep, fmt.Errorf("ctr %d counter stalled at step %d (%d -> %d)", id, cur.StepIdx, prev.UsageUs, cur.UsageUs)
				}
			} else if cur.LifeSeq > prev.LifeSeq {
				rep.Resets++
				if cur.UsageUs >= prev.UsageUs {
					return rep, fmt.Errorf("ctr %d restart did not reset at step %d", id, cur.StepIdx)
				}
			}
		}
	}
	if len(plan.Containers) > 0 && rep.Rows == 0 {
		return rep, fmt.Errorf("no cpu rows")
	}
	return rep, nil
}

func ExpectedFiles(plan *planner.Plan) int {
	gpuWL := map[int]bool{}
	for _, g := range plan.GPUs {
		gpuWL[g.WorkloadID] = true
	}
	n := 0
	for _, it := range plan.Items {
		n += 2
		for wl := it.WlLo; wl < it.WlHi; wl++ {
			if gpuWL[wl] {
				n++
				break
			}
		}
	}
	return n
}
