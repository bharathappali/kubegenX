package kruize

import (
	"fmt"
	"time"

	"github.com/bharathappali/kubegenx/internal/planner"
	"github.com/bharathappali/kubegenx/internal/samples"
)

// maxSeriesPerPage keeps one results call to the chunk files of a namespace
// or a workload. A whole dataset is many more series than this.
const maxSeriesPerPage = 100

// ResultPage is one page of container intervals, computed from the chunk
// files that hold those steps.
type ResultPage struct {
	Page      int
	PageSize  int
	Total     int
	Pages     int
	Intervals int
	Series    int
	Start     time.Time
	End       time.Time
	Items     []Bucket
}

type aggSeries struct {
	ns      planner.Namespace
	wl      planner.Workload
	name    string
	sample  planner.Container
	members []planner.Container
	reps    int
}

// AggregatePage returns one 6-hour chunk of aggregation rows for the session
// window that begins at origin. Pages are the dataset chunks that overlap the
// requested time, one chunk per page. A row is one container name over one
// interval inside that chunk. Only the Parquet row groups for those containers
// are decoded.
func AggregatePage(plan *planner.Plan, dataDir string, origin time.Time, interval time.Duration, namespace, workload string, from, to time.Time, page int) (ResultPage, error) {
	if page < 1 {
		page = 1
	}
	step := time.Duration(plan.StepSeconds) * time.Second
	per := int(interval / step)
	if per < 1 {
		per = 1
	}
	cs := plan.ChunkSamples
	if cs < 1 {
		cs = 1
	}
	nBuckets := 0
	if plan.Steps > 0 {
		nBuckets = plan.Steps / per
	}
	type chunkPage struct {
		js []int
	}
	var chunks []chunkPage
	var nInt int
	lastChunk := -1
	for j := 0; j < nBuckets; j++ {
		bStart := origin.Add(time.Duration(j) * interval)
		bEnd := bStart.Add(interval)
		if !bStart.Before(to) || !bEnd.After(from) {
			continue
		}
		nInt++
		ch := (j * per) / cs
		if ch != lastChunk {
			chunks = append(chunks, chunkPage{})
			lastChunk = ch
		}
		chunks[len(chunks)-1].js = append(chunks[len(chunks)-1].js, j)
	}
	series := seriesFor(plan, namespace, workload)
	if len(series) > maxSeriesPerPage {
		return ResultPage{}, fmt.Errorf("pass namespace or workload; a page computes at most %d series", maxSeriesPerPage)
	}
	pages := len(chunks)
	if pages < 1 {
		pages = 1
	}
	total := len(series) * nInt
	out := ResultPage{
		Page: page, Pages: pages, Total: total, Series: len(series), Items: []Bucket{},
	}
	if total == 0 || page > len(chunks) {
		return out, nil
	}
	js := chunks[page-1].js
	out.Intervals = len(js)
	out.PageSize = len(js)
	var items []Bucket
	for _, s := range series {
		rows, err := bucketsFor(plan, dataDir, origin, interval, per, s, js)
		if err != nil {
			return ResultPage{}, err
		}
		items = append(items, rows...)
	}
	if len(items) > 0 {
		out.Start = items[0].Start
		out.End = items[len(items)-1].End
	}
	out.Items = items
	return out, nil
}

func seriesFor(plan *planner.Plan, namespace, workload string) []aggSeries {
	ns := map[int]planner.Namespace{}
	for _, n := range plan.Namespaces {
		ns[n.ID] = n
	}
	byPod := map[int][]planner.Container{}
	for _, c := range plan.Containers {
		byPod[c.PodID] = append(byPod[c.PodID], c)
	}
	var out []aggSeries
	for _, w := range plan.Workloads {
		if w.Kind != "Deployment" && w.Kind != "StatefulSet" {
			continue
		}
		nspace := ns[w.NamespaceID]
		if namespace != "" && nspace.Name != namespace {
			continue
		}
		if workload != "" && w.Name != workload {
			continue
		}
		var pods []planner.Pod
		for _, p := range plan.Pods {
			if p.WorkloadID == w.ID {
				pods = append(pods, p)
			}
		}
		seen := map[string]bool{}
		grouped := map[string][]planner.Container{}
		var names []string
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
			members := grouped[name]
			if len(members) == 0 {
				continue
			}
			out = append(out, aggSeries{
				ns: nspace, wl: w, name: name, sample: members[0], members: members, reps: reps,
			})
		}
	}
	return out
}

func bucketsFor(plan *planner.Plan, dataDir string, origin time.Time, interval time.Duration, per int, s aggSeries, js []int) ([]Bucket, error) {
	if len(js) == 0 {
		return nil, nil
	}
	lo, hi := js[0], js[0]
	for _, j := range js[1:] {
		if j < lo {
			lo = j
		}
		if j > hi {
			hi = j
		}
	}
	stepLo := lo * per
	if stepLo > 0 {
		stepLo--
	}
	stepHi := (hi + 1) * per
	cpuIdx, memIdx, err := readSeries(plan, dataDir, s.wl.ID, s.members, stepLo, stepHi)
	if err != nil {
		return nil, err
	}
	var out []Bucket
	for _, j := range js {
		step0 := j * per
		metrics := map[string]Metric{}
		if s.sample.CPUReqMilli > 0 {
			metrics["cpuRequest"] = constant(float64(s.sample.CPUReqMilli)/1000, s.reps, "cores")
		}
		if s.sample.CPULimitMilli > 0 {
			metrics["cpuLimit"] = constant(float64(s.sample.CPULimitMilli)/1000, s.reps, "cores")
		}
		cmin, cmax, cavg, csum := aggRate(s.members, cpuIdx, step0, per, plan.StepSeconds, false)
		metrics["cpuUsage"] = Metric{Min: cmin, Max: cmax, Avg: cavg, Sum: csum, Unit: "cores"}
		tmin, tmax, tavg, tsum := aggRate(s.members, cpuIdx, step0, per, plan.StepSeconds, true)
		metrics["cpuThrottle"] = Metric{Min: tmin, Max: tmax, Avg: tavg, Sum: tsum, Unit: "cores"}
		if s.sample.MemReqBytes > 0 {
			metrics["memoryRequest"] = constant(float64(s.sample.MemReqBytes), s.reps, "bytes")
		}
		if s.sample.MemLimitBytes > 0 {
			metrics["memoryLimit"] = constant(float64(s.sample.MemLimitBytes), s.reps, "bytes")
		}
		mmin, mmax, mavg, msum := gaugeBytes(s.members, memIdx, step0, per, true)
		metrics["memoryUsage"] = Metric{Min: mmin, Max: mmax, Avg: mavg, Sum: msum, Unit: "bytes"}
		rmin, rmax, ravg, rsum := gaugeBytes(s.members, memIdx, step0, per, false)
		metrics["memoryRSS"] = Metric{Min: rmin, Max: rmax, Avg: ravg, Sum: rsum, Unit: "bytes"}
		bStart := origin.Add(time.Duration(j) * interval)
		out = append(out, Bucket{
			Namespace: s.ns.Name, Kind: s.wl.Kind, Workload: s.wl.Name, Replicas: s.reps, Container: s.name,
			Start: bStart, End: bStart.Add(interval), Metrics: metrics,
		})
	}
	return out, nil
}

func readSeries(plan *planner.Plan, dataDir string, workloadID int, members []planner.Container, stepLo, stepHi int) (map[[2]uint32]cpuPoint, map[[2]uint32]gauge, error) {
	want := map[uint32]bool{}
	ids := make([]uint32, 0, len(members))
	for _, c := range members {
		id := uint32(c.ID)
		if want[id] {
			continue
		}
		want[id] = true
		ids = append(ids, id)
	}
	cs := plan.ChunkSamples
	if cs < 1 {
		cs = 1
	}
	c0 := stepLo / cs
	last := stepHi - 1
	if last < stepLo {
		last = stepLo
	}
	c1 := last / cs
	cpuIdx := map[[2]uint32]cpuPoint{}
	memIdx := map[[2]uint32]gauge{}
	for _, it := range plan.Items {
		if it.Chunk < c0 || it.Chunk > c1 || workloadID < it.WlLo || workloadID >= it.WlHi {
			continue
		}
		cpuRows, err := samples.ReadCPUMatching(samples.ItemFile(dataDir, "cpu", it.Chunk, it.Index), ids)
		if err != nil {
			return nil, nil, err
		}
		for _, r := range cpuRows {
			if !want[r.CtrID] || int(r.StepIdx) < stepLo || int(r.StepIdx) >= stepHi {
				continue
			}
			cpuIdx[[2]uint32{r.CtrID, r.StepIdx}] = cpuPoint{r.LifeSeq, r.UsageUs, r.ThrottledUs}
		}
		memRows, err := samples.ReadMemMatching(samples.ItemFile(dataDir, "memory", it.Chunk, it.Index), ids)
		if err != nil {
			return nil, nil, err
		}
		for _, r := range memRows {
			if !want[r.CtrID] || int(r.StepIdx) < stepLo || int(r.StepIdx) >= stepHi {
				continue
			}
			memIdx[[2]uint32{r.CtrID, r.StepIdx}] = gauge{ws: r.WsBytes, rss: r.RssBytes}
		}
	}
	return cpuIdx, memIdx, nil
}
