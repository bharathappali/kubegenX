package kruize

import (
	"time"

	"github.com/bharathappali/kubegenx/internal/planner"
	"github.com/bharathappali/kubegenx/internal/samples"
)

// Metric is min, max, avg, and sum for one signal over one aggregation bucket.
type Metric struct {
	Min  float64 `json:"min"`
	Max  float64 `json:"max"`
	Avg  float64 `json:"avg"`
	Sum  float64 `json:"sum"`
	Unit string  `json:"unit"`
}

// Bucket is one container over one aggregation interval.
type Bucket struct {
	Namespace string            `json:"namespace"`
	Kind      string            `json:"kind"`
	Workload  string            `json:"workload"`
	Replicas  int               `json:"replicas"`
	Container string            `json:"container"`
	Start     time.Time         `json:"start"`
	End       time.Time         `json:"end"`
	Metrics   map[string]Metric `json:"metrics"`
}

// Buckets aggregates parquet samples into fixed intervals beginning at start.
// A trailing partial interval is dropped.
func Buckets(plan *planner.Plan, dataDir string, start time.Time, interval time.Duration) ([]Bucket, error) {
	step := time.Duration(plan.StepSeconds) * time.Second
	n := int(interval / step)
	if n < 1 {
		n = 1
	}
	nBuckets := plan.Steps / n
	cpuRows, err := samples.ReadCPU(dataDir)
	if err != nil {
		return nil, err
	}
	memRows, err := samples.ReadMem(dataDir)
	if err != nil {
		return nil, err
	}
	cpuIdx := map[[2]uint32]cpuPoint{}
	for _, r := range cpuRows {
		cpuIdx[[2]uint32{r.CtrID, r.StepIdx}] = cpuPoint{r.LifeSeq, r.UsageUs, r.ThrottledUs}
	}
	memIdx := map[[2]uint32]gauge{}
	for _, r := range memRows {
		memIdx[[2]uint32{r.CtrID, r.StepIdx}] = gauge{ws: r.WsBytes, rss: r.RssBytes}
	}
	ns := map[int]planner.Namespace{}
	for _, n := range plan.Namespaces {
		ns[n.ID] = n
	}
	byPod := map[int][]planner.Container{}
	for _, c := range plan.Containers {
		byPod[c.PodID] = append(byPod[c.PodID], c)
	}
	var out []Bucket
	for _, w := range plan.Workloads {
		nspace := ns[w.NamespaceID]
		var pods []planner.Pod
		for _, p := range plan.Pods {
			if p.WorkloadID == w.ID {
				pods = append(pods, p)
			}
		}
		names := []string{}
		seen := map[string]bool{}
		grouped := map[string][]planner.Container{}
		for _, p := range pods {
			for _, c := range byPod[p.ID] {
				grouped[c.Name] = append(grouped[c.Name], c)
				if !seen[c.Name] {
					seen[c.Name] = true
					names = append(names, c.Name)
				}
			}
		}
		reps := w.Replicas
		if reps < 1 {
			reps = 1
		}
		for _, name := range names {
			series := grouped[name]
			if len(series) == 0 {
				continue
			}
			sample := series[0]
			for j := 0; j < nBuckets; j++ {
				step0 := j * n
				metrics := map[string]Metric{}
				if sample.CPUReqMilli > 0 {
					metrics["cpuRequest"] = constant(float64(sample.CPUReqMilli)/1000, reps, "cores")
				}
				if sample.CPULimitMilli > 0 {
					metrics["cpuLimit"] = constant(float64(sample.CPULimitMilli)/1000, reps, "cores")
				}
				cmin, cmax, cavg, csum := aggRate(series, cpuIdx, step0, n, plan.StepSeconds, false)
				metrics["cpuUsage"] = Metric{Min: cmin, Max: cmax, Avg: cavg, Sum: csum, Unit: "cores"}
				tmin, tmax, tavg, tsum := aggRate(series, cpuIdx, step0, n, plan.StepSeconds, true)
				metrics["cpuThrottle"] = Metric{Min: tmin, Max: tmax, Avg: tavg, Sum: tsum, Unit: "cores"}
				if sample.MemReqBytes > 0 {
					metrics["memoryRequest"] = constant(float64(sample.MemReqBytes), reps, "bytes")
				}
				if sample.MemLimitBytes > 0 {
					metrics["memoryLimit"] = constant(float64(sample.MemLimitBytes), reps, "bytes")
				}
				mmin, mmax, mavg, msum := gaugeBytes(series, memIdx, step0, n, true)
				metrics["memoryUsage"] = Metric{Min: mmin, Max: mmax, Avg: mavg, Sum: msum, Unit: "bytes"}
				rmin, rmax, ravg, rsum := gaugeBytes(series, memIdx, step0, n, false)
				metrics["memoryRSS"] = Metric{Min: rmin, Max: rmax, Avg: ravg, Sum: rsum, Unit: "bytes"}
				bStart := start.Add(time.Duration(j) * interval)
				out = append(out, Bucket{
					Namespace: nspace.Name, Kind: w.Kind, Workload: w.Name, Replicas: reps, Container: name,
					Start: bStart, End: bStart.Add(interval), Metrics: metrics,
				})
			}
		}
	}
	if out == nil {
		out = []Bucket{}
	}
	return out, nil
}

func constant(per float64, replicas int, unit string) Metric {
	return Metric{Min: per, Max: per, Avg: per, Sum: per * float64(replicas), Unit: unit}
}

func gaugeBytes(series []planner.Container, idx map[[2]uint32]gauge, step0, n int, ws bool) (min, max, avg, sum float64) {
	min = 1e300
	var means []float64
	for _, c := range series {
		var vals []float64
		for s := step0; s < step0+n; s++ {
			cur, ok := idx[[2]uint32{uint32(c.ID), uint32(s)}]
			if !ok {
				continue
			}
			b := cur.rss
			if ws {
				b = cur.ws
			}
			v := float64(b)
			vals = append(vals, v)
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
		if len(vals) > 0 {
			means = append(means, mean(vals))
		}
	}
	if min == 1e300 {
		min = 0
	}
	for _, m := range means {
		sum += m
	}
	if len(means) > 0 {
		avg = sum / float64(len(means))
	}
	return round6(min), round6(max), round6(avg), round6(sum)
}
