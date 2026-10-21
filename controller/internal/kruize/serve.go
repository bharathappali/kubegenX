package kruize

import (
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/bharathappali/kubegenx/internal/planner"
	"github.com/bharathappali/kubegenx/internal/samples"
)

type cpuPoint struct {
	life  uint8
	usage int64
	throt int64
}

func Experiments(plan *planner.Plan, page, pageSize int) (data []map[string]any, total, totalPages int) {
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 100
	}
	if page < 1 {
		page = 1
	}
	total = len(plan.Workloads)
	totalPages = (total + pageSize - 1) / pageSize
	if totalPages == 0 {
		totalPages = 1
	}
	start := (page - 1) * pageSize
	if start > total {
		start = total
	}
	end := start + pageSize
	if end > total {
		end = total
	}
	ns := map[int]planner.Namespace{}
	for _, n := range plan.Namespaces {
		ns[n.ID] = n
	}
	ctrs := map[int][]planner.Container{}
	for _, c := range plan.Containers {
		ctrs[c.PodID] = append(ctrs[c.PodID], c)
	}
	firstPod := map[int]planner.Pod{}
	for _, p := range plan.Pods {
		if _, ok := firstPod[p.WorkloadID]; !ok {
			firstPod[p.WorkloadID] = p
		}
	}
	for _, w := range plan.Workloads[start:end] {
		n := ns[w.NamespaceID]
		pod := firstPod[w.ID]
		var containers []map[string]any
		seen := map[string]bool{}
		for _, c := range ctrs[pod.ID] {
			if seen[c.Name] {
				continue
			}
			seen[c.Name] = true
			containers = append(containers, map[string]any{
				"container_image_name": c.Image,
				"container_name":       c.Name,
			})
		}
		data = append(data, map[string]any{
			"version":             "v2.0",
			"experiment_name":     planner.ExperimentName(plan.Dataset, n.Name, w.Kind, w.Name),
			"cluster_name":        plan.ClusterName,
			"performance_profile": "resource-optimization-openshift",
			"mode":                "monitor",
			"target_cluster":      "remote",
			"experiment_type":     "container",
			"kubernetes_objects": []map[string]any{{
				"type":       strings.ToLower(w.Kind),
				"name":       w.Name,
				"namespace":  n.Name,
				"containers": containers,
			}},
			"trial_settings":          map[string]any{"measurement_duration": "15min"},
			"recommendation_settings": map[string]any{"threshold": "0.1"},
		})
	}
	if data == nil {
		data = []map[string]any{}
	}
	return data, total, totalPages
}

func Results(plan *planner.Plan, dataDir, experiment string, chunk int, offset time.Duration) ([]map[string]any, error) {
	w, ns, err := findWorkload(plan, experiment)
	if err != nil {
		return nil, err
	}
	if chunk < 0 || chunk >= plan.Chunks {
		return nil, fmt.Errorf("chunk %d out of range", chunk)
	}
	cpuRows, err := samples.ReadCPU(dataDir)
	if err != nil {
		return nil, err
	}
	memRows, err := samples.ReadMem(dataDir)
	if err != nil {
		return nil, err
	}
	gpuRows, err := samples.ReadGPU(dataDir)
	if err != nil {
		return nil, err
	}
	pods := []planner.Pod{}
	for _, p := range plan.Pods {
		if p.WorkloadID == w.ID {
			pods = append(pods, p)
		}
	}
	byPod := map[int][]planner.Container{}
	names := []string{}
	seenName := map[string]bool{}
	for _, c := range plan.Containers {
		if c.WorkloadID != w.ID {
			continue
		}
		byPod[c.PodID] = append(byPod[c.PodID], c)
		if !seenName[c.Name] {
			seenName[c.Name] = true
			names = append(names, c.Name)
		}
	}
	cpuIdx := map[[2]uint32]cpuPoint{}
	for _, r := range cpuRows {
		cpuIdx[[2]uint32{r.CtrID, r.StepIdx}] = cpuPoint{r.LifeSeq, r.UsageUs, r.ThrottledUs}
	}
	memIdx := map[[2]uint32]gauge{}
	for _, r := range memRows {
		memIdx[[2]uint32{r.CtrID, r.StepIdx}] = gauge{ws: r.WsBytes, rss: r.RssBytes}
	}
	gpuByCtr := map[int][]planner.GPUEntity{}
	for _, g := range plan.GPUs {
		gpuByCtr[g.ContainerID] = append(gpuByCtr[g.ContainerID], g)
	}
	gpuIdx := map[[2]uint32]samples.GPURow{}
	for _, r := range gpuRows {
		gpuIdx[[2]uint32{r.GPUEntID, r.StepIdx}] = r
	}
	epoch, err := time.Parse(time.RFC3339, plan.Epoch)
	if err != nil {
		return nil, err
	}
	step := time.Duration(plan.StepSeconds) * time.Second
	var out []map[string]any
	for j := 0; j < 24; j++ {
		step0 := chunk*plan.ChunkSamples + j*60
		containers := []map[string]any{}
		for _, name := range names {
			var sampleCtr planner.Container
			var series []planner.Container
			for _, pod := range pods {
				for _, c := range byPod[pod.ID] {
					if c.Name == name {
						series = append(series, c)
						sampleCtr = c
					}
				}
			}
			p := len(series)
			if p == 0 {
				continue
			}
			metrics := []map[string]any{}
			if sampleCtr.CPUReqMilli > 0 {
				avg := float64(sampleCtr.CPUReqMilli) / 1000
				metrics = append(metrics, metric("cpuRequest", "", avg*float64(p), avg, 0, 0, "cores", false))
			}
			if sampleCtr.CPULimitMilli > 0 {
				avg := float64(sampleCtr.CPULimitMilli) / 1000
				metrics = append(metrics, metric("cpuLimit", "", avg*float64(p), avg, 0, 0, "cores", false))
			}
			cmin, cmax, cavg, csum := aggRate(series, cpuIdx, step0, 60, plan.StepSeconds, false)
			metrics = append(metrics, metric("cpuUsage", "", csum, cavg, cmin, cmax, "cores", true))
			_, tmax, tavg, tsum := aggRate(series, cpuIdx, step0, 60, plan.StepSeconds, true)
			metrics = append(metrics, metric("cpuThrottle", "", tsum, tavg, 0, tmax, "cores", true))
			if sampleCtr.MemReqBytes > 0 {
				avg := float64(sampleCtr.MemReqBytes) / (1024 * 1024)
				metrics = append(metrics, metric("memoryRequest", "", avg*float64(p), avg, 0, 0, "MiB", false))
			}
			if sampleCtr.MemLimitBytes > 0 {
				avg := float64(sampleCtr.MemLimitBytes) / (1024 * 1024)
				metrics = append(metrics, metric("memoryLimit", "", avg*float64(p), avg, 0, 0, "MiB", false))
			}
			mmin, mmax, mavg, msum := aggGauge(series, memIdx, step0, 60, true)
			metrics = append(metrics, metric("memoryUsage", "", msum, mavg, mmin, mmax, "MiB", true))
			rmin, rmax, ravg, rsum := aggGauge(series, memIdx, step0, 60, false)
			metrics = append(metrics, metric("memoryRSS", "", rsum, ravg, rmin, rmax, "MiB", true))
			var gpus []planner.GPUEntity
			for _, c := range series {
				gpus = append(gpus, gpuByCtr[c.ID]...)
			}
			if len(gpus) > 0 {
				g := gpus[0]
				model := strings.ReplaceAll(g.Model, " ", "-")
				umin, umax, uavg := aggGPU(gpus, gpuIdx, step0, 60, true)
				metrics = append(metrics, metric("acceleratorCoreUsage", model, 0, uavg, umin, umax, "percentage", true))
				mmin, mmax, mavg := aggGPU(gpus, gpuIdx, step0, 60, false)
				metrics = append(metrics, metric("acceleratorMemoryUsage", model, 0, mavg, mmin, mmax, "percentage", true))
				fmin, fmax, favg := aggFB(gpus, gpuIdx, step0, 60)
				metrics = append(metrics, metric("acceleratorFrameBufferUsage", model, 0, favg, fmin, fmax, "MiB", true))
			}
			containers = append(containers, map[string]any{
				"container_image_name": sampleCtr.Image,
				"container_name":       name,
				"metrics":              metrics,
			})
		}
		start := epoch.Add(offset).Add(time.Duration(chunk*24+j) * 15 * time.Minute)
		_ = step
		out = append(out, map[string]any{
			"version":             "v2.0",
			"experiment_name":     experiment,
			"interval_start_time": start.UTC().Format("2006-01-02T15:04:05.000Z"),
			"interval_end_time":   start.Add(15 * time.Minute).UTC().Format("2006-01-02T15:04:05.000Z"),
			"kubernetes_objects": []map[string]any{{
				"type":       strings.ToLower(w.Kind),
				"name":       w.Name,
				"namespace":  ns.Name,
				"containers": containers,
			}},
		})
	}
	return out, nil
}

func findWorkload(plan *planner.Plan, experiment string) (planner.Workload, planner.Namespace, error) {
	ns := map[int]planner.Namespace{}
	for _, n := range plan.Namespaces {
		ns[n.ID] = n
	}
	for _, w := range plan.Workloads {
		n := ns[w.NamespaceID]
		if planner.ExperimentName(plan.Dataset, n.Name, w.Kind, w.Name) == experiment {
			return w, n, nil
		}
	}
	return planner.Workload{}, planner.Namespace{}, fmt.Errorf("unknown experiment %s", experiment)
}

func aggRate(series []planner.Container, idx map[[2]uint32]cpuPoint, step0, n, stepSec int, throttle bool) (min, max, avg, sum float64) {
	min = math.MaxFloat64
	var means []float64
	for _, c := range series {
		var vals []float64
		for s := step0; s < step0+n; s++ {
			cur, ok := idx[[2]uint32{uint32(c.ID), uint32(s)}]
			if !ok {
				continue
			}
			var delta int64
			if prev, ok := idx[[2]uint32{uint32(c.ID), uint32(s - 1)}]; ok && prev.life == cur.life {
				if throttle {
					delta = cur.throt - prev.throt
				} else {
					delta = cur.usage - prev.usage
				}
			} else if throttle {
				delta = cur.throt
			} else {
				delta = cur.usage
			}
			if delta < 0 {
				delta = 0
			}
			v := float64(delta) / 1e6 / float64(stepSec)
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
	if min == math.MaxFloat64 {
		min = 0
	}
	sum = 0
	for _, m := range means {
		sum += m
	}
	if len(means) > 0 {
		avg = sum / float64(len(means))
	}
	return round6(min), round6(max), round6(avg), round6(sum)
}

type gauge struct{ ws, rss int64 }

func aggGauge(series []planner.Container, idx map[[2]uint32]gauge, step0, n int, ws bool) (min, max, avg, sum float64) {
	min = math.MaxFloat64
	var means []float64
	for _, c := range series {
		var vals []float64
		for s := step0; s < step0+n; s++ {
			cur, ok := idx[[2]uint32{uint32(c.ID), uint32(s)}]
			if !ok {
				continue
			}
			var b int64
			if ws {
				b = cur.ws
			} else {
				b = cur.rss
			}
			v := float64(b) / (1024 * 1024)
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
	if min == math.MaxFloat64 {
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

func aggGPU(gpus []planner.GPUEntity, idx map[[2]uint32]samples.GPURow, step0, n int, util bool) (min, max, avg float64) {
	min = math.MaxFloat64
	var vals []float64
	for _, g := range gpus {
		for s := step0; s < step0+n; s++ {
			row, ok := idx[[2]uint32{uint32(g.ID), uint32(s)}]
			if !ok {
				continue
			}
			var v float64
			if util {
				v = float64(row.Util)
			} else {
				v = float64(row.MemCopy)
			}
			vals = append(vals, v)
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
	}
	if min == math.MaxFloat64 {
		min = 0
	}
	return round6(min), round6(max), round6(mean(vals))
}

func aggFB(gpus []planner.GPUEntity, idx map[[2]uint32]samples.GPURow, step0, n int) (min, max, avg float64) {
	min = math.MaxFloat64
	var vals []float64
	for _, g := range gpus {
		for s := step0; s < step0+n; s++ {
			row, ok := idx[[2]uint32{uint32(g.ID), uint32(s)}]
			if !ok {
				continue
			}
			v := float64(row.FBUsed)
			vals = append(vals, v)
			if v < min {
				min = v
			}
			if v > max {
				max = v
			}
		}
	}
	if min == math.MaxFloat64 {
		min = 0
	}
	return round6(min), round6(max), round6(mean(vals))
}

func metric(name, model string, sum, avg, min, max float64, format string, withMin bool) map[string]any {
	info := map[string]any{"format": format, "avg": avg}
	if withMin {
		if name != "cpuThrottle" {
			info["min"] = min
		}
		info["max"] = max
	}
	if name != "acceleratorCoreUsage" && name != "acceleratorMemoryUsage" && name != "acceleratorFrameBufferUsage" {
		info["sum"] = sum
	}
	m := map[string]any{"name": name, "results": map[string]any{"aggregation_info": info}}
	if model != "" {
		m["results"].(map[string]any)["metadata"] = map[string]any{"accelerator_model_name": model}
	}
	return m
}

func mean(v []float64) float64 {
	if len(v) == 0 {
		return 0
	}
	var s float64
	for _, x := range v {
		s += x
	}
	return s / float64(len(v))
}

func round6(v float64) float64 {
	return math.Round(v*1e6) / 1e6
}

// FloorEnd snaps an end time down to 15 minutes. "now" uses the current clock.
func FloorEnd(end string, now time.Time) (time.Time, error) {
	if end == "" || end == "now" {
		return now.UTC().Truncate(15 * time.Minute), nil
	}
	t, err := time.Parse(time.RFC3339, end)
	if err != nil {
		return time.Time{}, err
	}
	return t.UTC().Truncate(15 * time.Minute), nil
}

func Offset(plan *planner.Plan, end time.Time) (offset time.Duration, originalEnd, effectiveStart, effectiveEnd time.Time, err error) {
	epoch, err := time.Parse(time.RFC3339, plan.Epoch)
	if err != nil {
		return 0, time.Time{}, time.Time{}, time.Time{}, err
	}
	originalEnd = epoch.Add(time.Duration(plan.Steps*plan.StepSeconds) * time.Second)
	floorOrig := originalEnd.Truncate(15 * time.Minute)
	offset = end.Sub(floorOrig)
	return offset, originalEnd, epoch.Add(offset), floorOrig.Add(offset), nil
}
