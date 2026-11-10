package verify

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/bharathappali/kubegenx/internal/planner"
	"github.com/bharathappali/kubegenx/internal/samples"
	"github.com/parquet-go/parquet-go"
)

type Report struct {
	Files    int `json:"files"`
	Expected int `json:"expected"`
	Rows     int `json:"rows"`
	Resets   int `json:"resets"`
}

// Progress reports how far verification has gotten (done of total steps).
type Progress func(done, total int)

func Dataset(dataDir, name string) (Report, error) {
	return DatasetWithProgress(dataDir, name, nil)
}

func DatasetWithProgress(dataDir, name string, onProgress Progress) (Report, error) {
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
	sampleN := 0
	if len(plan.Containers) > 0 {
		sampleN = 8
	}
	totalSteps := 1 + sampleN
	if totalSteps < 1 {
		totalSteps = 1
	}
	report := func(done int) {
		if onProgress == nil {
			return
		}
		if done > totalSteps {
			done = totalSteps
		}
		onProgress(done, totalSteps)
	}
	report(0)

	rep.Expected = ExpectedFiles(&plan)
	rep.Files, err = samples.CountParquet(dir)
	if err != nil {
		return rep, err
	}
	if rep.Files != rep.Expected {
		return rep, fmt.Errorf("parquet files %d, planned %d", rep.Files, rep.Expected)
	}
	report(1)

	// A full read of every CPU row does not fit in the controller. Check a
	// spread of item files, one at a time, and drop each file before the next.
	if len(plan.Containers) > 0 {
		n, resets, err := sampleCPU(dir, func(checked, want int) {
			report(1 + checked)
			_ = want
		})
		if err != nil {
			return rep, err
		}
		rep.Rows = n
		rep.Resets = resets
		if n == 0 {
			return rep, fmt.Errorf("no cpu rows")
		}
	}
	report(totalSteps)
	return rep, nil
}

func sampleCPU(dir string, onFile func(checked, want int)) (rows, resets int, err error) {
	pattern := filepath.Join(dir, "samples", "feature=cpu", "chunk=*", "item-*.parquet")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return 0, 0, err
	}
	sort.Strings(matches)
	if len(matches) == 0 {
		return 0, 0, fmt.Errorf("no cpu rows")
	}
	const sampleN = 8
	step := 1
	if len(matches) > sampleN {
		step = len(matches) / sampleN
	}
	checked := 0
	for i := 0; i < len(matches) && checked < sampleN; i += step {
		part, r, err := checkCPUFile(matches[i])
		if err != nil {
			return rows, resets, err
		}
		rows += part
		resets += r
		checked++
		if onFile != nil {
			onFile(checked, sampleN)
		}
	}
	return rows, resets, nil
}

func checkCPUFile(path string) (int, int, error) {
	fileRows, err := parquet.ReadFile[samples.CPURow](path)
	if err != nil {
		return 0, 0, err
	}
	if len(fileRows) == 0 {
		return 0, 0, fmt.Errorf("empty cpu file %s", path)
	}
	by := map[uint32][]samples.CPURow{}
	for _, r := range fileRows {
		by[r.CtrID] = append(by[r.CtrID], r)
	}
	resets := 0
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
				return len(fileRows), resets, fmt.Errorf("ctr %d step did not advance %d -> %d", id, prev.StepIdx, cur.StepIdx)
			}
			if cur.LifeSeq == prev.LifeSeq {
				if cur.UsageUs <= prev.UsageUs {
					return len(fileRows), resets, fmt.Errorf("ctr %d counter stalled at step %d (%d -> %d)", id, cur.StepIdx, prev.UsageUs, cur.UsageUs)
				}
			} else if cur.LifeSeq > prev.LifeSeq {
				resets++
				if cur.UsageUs >= prev.UsageUs {
					return len(fileRows), resets, fmt.Errorf("ctr %d restart did not reset at step %d", id, cur.StepIdx)
				}
			}
		}
	}
	return len(fileRows), resets, nil
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
