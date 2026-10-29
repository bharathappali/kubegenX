package export

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/bharathappali/kubegenx/internal/planner"
	"github.com/bharathappali/kubegenx/internal/samples"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb"
)

const (
	metricCPU      = "container_cpu_usage_seconds_total"
	metricThrottle = "container_cpu_cfs_throttled_seconds_total"
	metricWS       = "container_memory_working_set_bytes"
	metricRSS      = "container_memory_rss"
	recIrate       = "node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate"
	recRate5m      = "node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate5m"
	recRate        = "node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate"
	recWS          = "node_namespace_pod_container:container_memory_working_set_bytes"
	recRSS         = "node_namespace_pod_container:container_memory_rss"
	recOwner       = "namespace_workload_pod:kube_pod_owner:relabel"
	recInfo        = "node_namespace_pod:kube_pod_info:"
)

type Result struct {
	Blocks int
	Bytes  int64
}

const blockSpan = 2 * time.Hour

// Build turns Parquet into 2h TSDB blocks without loading the dataset.
// A scan estimates each item, then goroutines fill 75% of the 16GiB pod.
func Build(ctx context.Context, plan *planner.Plan, datasetDir, scratch, exportName string, start time.Time, external map[string]string, progress func(done, total int, bytes int64, workers int, heap, budget int64)) (Result, error) {
	return buildBatched(ctx, plan, datasetDir, scratch, exportName, start, external, progress)
}

func writeBlock(ctx context.Context, plan *planner.Plan, cpuRows []samples.CPURow, memRows []samples.MemRow, gpuRows []samples.GPURow, scratch, exportName, token string, origin, lo, hi time.Time, destRoot string, external map[string]string, wlLo, wlHi, stepLo, stepHi int, emitNS bool) (int, int64, error) {
	tmp := filepath.Join(scratch, "tsdb-"+exportName+"-"+token)
	if err := os.RemoveAll(tmp); err != nil {
		return 0, 0, err
	}
	if err := os.MkdirAll(tmp, 0o775); err != nil {
		return 0, 0, err
	}
	defer os.RemoveAll(tmp)
	span := hi.Sub(lo).Milliseconds()
	if span < 1 {
		span = 1
	}
	w, err := tsdb.NewBlockWriter(slog.Default(), tmp, span)
	if err != nil {
		return 0, 0, err
	}
	app := w.Appender(ctx)
	b := &builder{
		app: app, start: origin.UnixMilli(), stepSec: plan.StepSeconds, plan: plan,
		winLo: lo.UnixMilli(), winHi: hi.UnixMilli(),
		wlLo: wlLo, wlHi: wlHi, stepLo: stepLo, stepHi: stepHi, emitNS: emitNS,
	}
	if err := b.emit(cpuRows, memRows, gpuRows); err != nil {
		w.Close()
		return 0, 0, err
	}
	if b.appended == 0 {
		w.Close()
		return 0, 0, nil
	}
	if err := app.Commit(); err != nil {
		w.Close()
		return 0, 0, err
	}
	id, err := w.Flush(ctx)
	w.Close()
	if err != nil {
		return 0, 0, err
	}
	blockDir := filepath.Join(tmp, id.String())
	if err := addThanosMeta(blockDir, plan, exportName, external); err != nil {
		return 0, 0, err
	}
	dst := filepath.Join(destRoot, id.String())
	if err := copyTree(blockDir, dst); err != nil {
		return 0, 0, err
	}
	dropCache(blockDir)
	dropCache(dst)
	sz, _ := dirSize(dst)
	return 1, sz, nil
}

type builder struct {
	app      storage.Appender
	start    int64
	stepSec  int
	plan     *planner.Plan
	winLo    int64
	winHi    int64
	wlLo     int
	wlHi     int
	stepLo   int
	stepHi   int
	emitNS   bool
	appended int
	err      error
}

func (b *builder) inShard(workloadID int) bool {
	if b.wlHi <= b.wlLo {
		return true
	}
	return workloadID >= b.wlLo && workloadID < b.wlHi
}

func (b *builder) accept(ts int64) bool {
	if b.winHi <= b.winLo {
		return true
	}
	return ts >= b.winLo && ts < b.winHi
}

func (b *builder) emit(cpuRows []samples.CPURow, memRows []samples.MemRow, gpuRows []samples.GPURow) error {
	ns := map[int]planner.Namespace{}
	for _, n := range b.plan.Namespaces {
		ns[n.ID] = n
	}
	wl := map[int]planner.Workload{}
	for _, w := range b.plan.Workloads {
		wl[w.ID] = w
	}
	pod := map[int]planner.Pod{}
	for _, p := range b.plan.Pods {
		pod[p.ID] = p
	}
	cpuBy := map[uint32][]samples.CPURow{}
	for _, r := range cpuRows {
		cpuBy[r.CtrID] = append(cpuBy[r.CtrID], r)
	}
	memBy := map[[3]uint32]samples.MemRow{}
	for _, r := range memRows {
		memBy[[3]uint32{r.CtrID, uint32(r.LifeSeq), r.StepIdx}] = r
	}
	for _, c := range b.plan.Containers {
		if !b.inShard(c.WorkloadID) {
			continue
		}
		p := pod[c.PodID]
		w := wl[c.WorkloadID]
		n := ns[w.NamespaceID]
		rows := cpuBy[uint32(c.ID)]
		lives := map[uint8][]samples.CPURow{}
		for _, r := range rows {
			lives[r.LifeSeq] = append(lives[r.LifeSeq], r)
		}
		for life, rs := range lives {
			cid := containerID(b.plan.Seed, c.ID, life)
			base := map[string]string{
				"namespace": n.Name, "pod": p.Name, "container": c.Name, "image": c.Image,
				"node": p.Node, "job": "kubelet", "metrics_path": "/metrics/cadvisor",
				"id": cgroupID(p.UID, cid), "name": c.Name, "instance": p.IP + ":10250",
			}
			for k, v := range n.Labels {
				base[k] = v
			}
			b.series(metricCPU, base, rs, func(r samples.CPURow) float64 { return float64(r.UsageUs) / 1e6 })
			b.series(metricThrottle, base, rs, func(r samples.CPURow) float64 { return float64(r.ThrottledUs) / 1e6 })
			b.rates(base, rs)
			memBase := clone(base)
			var memRowsLife []samples.CPURow
			for _, r := range rs {
				if _, ok := memBy[[3]uint32{r.CtrID, uint32(life), r.StepIdx}]; ok {
					memRowsLife = append(memRowsLife, r)
				}
			}
			b.series(metricWS, base, memRowsLife, func(r samples.CPURow) float64 {
				return float64(memBy[[3]uint32{r.CtrID, uint32(r.LifeSeq), r.StepIdx}].WsBytes)
			})
			b.series(metricRSS, base, memRowsLife, func(r samples.CPURow) float64 {
				return float64(memBy[[3]uint32{r.CtrID, uint32(r.LifeSeq), r.StepIdx}].RssBytes)
			})
			rec := map[string]string{"namespace": n.Name, "pod": p.Name, "container": c.Name, "node": p.Node}
			for k, v := range n.Labels {
				rec[k] = v
			}
			b.series(recWS, rec, memRowsLife, func(r samples.CPURow) float64 {
				return float64(memBy[[3]uint32{r.CtrID, uint32(r.LifeSeq), r.StepIdx}].WsBytes)
			})
			b.series(recRSS, rec, memRowsLife, func(r samples.CPURow) float64 {
				return float64(memBy[[3]uint32{r.CtrID, uint32(r.LifeSeq), r.StepIdx}].RssBytes)
			})
			_ = memBase
			b.ksmContainer(n, p, w, c, cid, life, rs)
		}
	}
	b.ksmPods(ns, wl, pod)
	b.gpus(ns, wl, pod, gpuRows)
	return b.err
}

func (b *builder) series(name string, base map[string]string, rows []samples.CPURow, val func(samples.CPURow) float64) {
	if b.err != nil || len(rows) == 0 {
		return
	}
	lbls := withName(name, base)
	sort.Slice(rows, func(i, j int) bool { return rows[i].StepIdx < rows[j].StepIdx })
	var prev int64 = -1
	for _, r := range rows {
		ts := b.start + int64(r.StepIdx)*int64(b.stepSec)*1000
		if ts <= prev || !b.accept(ts) {
			continue
		}
		prev = ts
		if _, err := b.app.Append(0, lbls, ts, val(r)); err != nil {
			b.err = fmt.Errorf("%s: %w", name, err)
			return
		}
		b.appended++
	}
}

func (b *builder) rates(base map[string]string, rows []samples.CPURow) {
	if b.err != nil || len(rows) < 2 {
		return
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].StepIdx < rows[j].StepIdx })
	irate := withName(recIrate, map[string]string{"namespace": base["namespace"], "pod": base["pod"], "container": base["container"], "node": base["node"], "org_id": base["org_id"], "cluster_id": base["cluster_id"]})
	rate5 := withName(recRate5m, map[string]string{"namespace": base["namespace"], "pod": base["pod"], "container": base["container"], "node": base["node"], "org_id": base["org_id"], "cluster_id": base["cluster_id"]})
	rate := withName(recRate, map[string]string{"namespace": base["namespace"], "pod": base["pod"], "container": base["container"], "node": base["node"], "org_id": base["org_id"], "cluster_id": base["cluster_id"]})
	for i := 1; i < len(rows); i++ {
		ts := b.start + int64(rows[i].StepIdx)*int64(b.stepSec)*1000
		if !b.accept(ts) {
			continue
		}
		du := float64(rows[i].UsageUs-rows[i-1].UsageUs) / 1e6
		dt := float64(rows[i].StepIdx-rows[i-1].StepIdx) * float64(b.stepSec)
		if dt <= 0 {
			continue
		}
		iv := du / dt
		k := i
		if k > 20 {
			k = 20
		}
		span := float64(rows[i].StepIdx-rows[i-k].StepIdx) * float64(b.stepSec)
		rv := float64(rows[i].UsageUs-rows[i-k].UsageUs) / 1e6 / span
		if math.IsNaN(iv) || math.IsNaN(rv) {
			continue
		}
		if _, err := b.app.Append(0, irate, ts, iv); err != nil {
			b.err = err
			return
		}
		if _, err := b.app.Append(0, rate5, ts, rv); err != nil {
			b.err = err
			return
		}
		if _, err := b.app.Append(0, rate, ts, rv); err != nil {
			b.err = err
			return
		}
		b.appended += 3
	}
}

func (b *builder) ksmContainer(n planner.Namespace, p planner.Pod, w planner.Workload, c planner.Container, cid string, life uint8, rows []samples.CPURow) {
	if len(rows) == 0 {
		return
	}
	info := map[string]string{
		"namespace": n.Name, "pod": p.Name, "container": c.Name,
		"image": c.Image, "image_id": "sha256:" + c.ImageID, "container_id": cid,
	}
	for k, v := range n.Labels {
		info[k] = v
	}
	b.staticSteps("kube_pod_container_info", info, rows, 1)
	b.staticSteps("kube_pod_container_status_ready", info, rows, 1)
	if c.CPUReqMilli > 0 {
		req := clone(info)
		req["resource"] = "cpu"
		req["unit"] = "core"
		b.staticSteps("kube_pod_container_resource_requests", req, rows, float64(c.CPUReqMilli)/1000)
	}
	if c.CPULimitMilli > 0 {
		lim := clone(info)
		lim["resource"] = "cpu"
		lim["unit"] = "core"
		b.staticSteps("kube_pod_container_resource_limits", lim, rows, float64(c.CPULimitMilli)/1000)
	}
	if c.MemReqBytes > 0 {
		req := clone(info)
		req["resource"] = "memory"
		req["unit"] = "byte"
		b.staticSteps("kube_pod_container_resource_requests", req, rows, float64(c.MemReqBytes))
	}
	if c.MemLimitBytes > 0 {
		lim := clone(info)
		lim["resource"] = "memory"
		lim["unit"] = "byte"
		b.staticSteps("kube_pod_container_resource_limits", lim, rows, float64(c.MemLimitBytes))
	}
	restarts := float64(life)
	b.staticSteps("kube_pod_container_status_restarts_total", map[string]string{"namespace": n.Name, "pod": p.Name, "container": c.Name}, rows, restarts)
	_ = w
}

func (b *builder) staticSteps(name string, base map[string]string, rows []samples.CPURow, v float64) {
	if b.err != nil || len(rows) == 0 {
		return
	}
	lbls := withName(name, base)
	sort.Slice(rows, func(i, j int) bool { return rows[i].StepIdx < rows[j].StepIdx })
	var prev int64 = -1
	for _, r := range rows {
		if r.StepIdx%2 != 0 {
			continue
		}
		ts := b.start + int64(r.StepIdx)*int64(b.stepSec)*1000
		if ts <= prev || !b.accept(ts) {
			continue
		}
		prev = ts
		if _, err := b.app.Append(0, lbls, ts, v); err != nil {
			b.err = err
			return
		}
		b.appended++
	}
}

func (b *builder) ksmPods(ns map[int]planner.Namespace, wl map[int]planner.Workload, pods map[int]planner.Pod) {
	steps := b.plan.Steps
	if b.emitNS || b.wlHi <= b.wlLo {
		for _, n := range b.plan.Namespaces {
			lbl := map[string]string{"namespace": n.Name, "phase": "Active"}
			for k, v := range n.Labels {
				lbl[k] = v
			}
			b.staticRange("kube_namespace_status_phase", lbl, steps, 1)
		}
	}
	for _, p := range pods {
		if !b.inShard(p.WorkloadID) {
			continue
		}
		w := wl[p.WorkloadID]
		n := ns[w.NamespaceID]
		info := map[string]string{
			"namespace": n.Name, "pod": p.Name, "node": p.Node, "uid": p.UID,
			"pod_ip": p.IP, "host_ip": "10.0.0.1",
			"created_by_kind": ownerKind(w), "created_by_name": ownerName(w),
		}
		for k, v := range n.Labels {
			info[k] = v
		}
		b.staticRange("kube_pod_info", info, steps, 1)
		own := map[string]string{
			"namespace": n.Name, "pod": p.Name, "owner_kind": ownerKind(w),
			"owner_name": ownerName(w), "owner_is_controller": "true",
		}
		for k, v := range n.Labels {
			own[k] = v
		}
		b.staticRange("kube_pod_owner", own, steps, 1)
		phase := map[string]string{"namespace": n.Name, "pod": p.Name, "phase": "Running"}
		b.staticRange("kube_pod_status_phase", phase, steps, 1)
		rel := map[string]string{
			"namespace": n.Name, "pod": p.Name, "workload": w.Name,
			"workload_type": kindLower(w.Kind), "node": p.Node,
		}
		for k, v := range n.Labels {
			rel[k] = v
		}
		b.staticRange(recOwner, rel, steps, 1)
		b.staticRange(recInfo, map[string]string{"namespace": n.Name, "pod": p.Name, "node": p.Node}, steps, 1)
	}
	seenRS := map[string]bool{}
	for _, w := range b.plan.Workloads {
		if !b.inShard(w.ID) || w.Kind != "Deployment" {
			continue
		}
		n := ns[w.NamespaceID]
		key := n.Name + "/" + w.Name
		if seenRS[key] {
			continue
		}
		seenRS[key] = true
		rs := map[string]string{
			"namespace": n.Name, "replicaset": w.Name + "-" + w.RSHash,
			"owner_kind": "Deployment", "owner_name": w.Name, "owner_is_controller": "true",
		}
		for k, v := range n.Labels {
			rs[k] = v
		}
		b.staticRange("kube_replicaset_owner", rs, b.plan.Steps, 1)
	}
}

func (b *builder) staticRange(name string, base map[string]string, steps int, v float64) {
	if b.err != nil {
		return
	}
	lbls := withName(name, base)
	lo, hi := 0, steps
	if b.stepHi > b.stepLo {
		lo, hi = b.stepLo, b.stepHi
		if lo < 0 {
			lo = 0
		}
		if hi > steps {
			hi = steps
		}
	}
	if lo%2 != 0 {
		lo++
	}
	for step := lo; step < hi; step += 2 {
		ts := b.start + int64(step)*int64(b.stepSec)*1000
		if !b.accept(ts) {
			continue
		}
		if _, err := b.app.Append(0, lbls, ts, v); err != nil {
			b.err = err
			return
		}
		b.appended++
	}
}

func (b *builder) gpus(ns map[int]planner.Namespace, wl map[int]planner.Workload, pods map[int]planner.Pod, rows []samples.GPURow) {
	by := map[uint32][]samples.GPURow{}
	for _, r := range rows {
		by[r.GPUEntID] = append(by[r.GPUEntID], r)
	}
	ctr := map[int]planner.Container{}
	for _, c := range b.plan.Containers {
		ctr[c.ID] = c
	}
	for _, g := range b.plan.GPUs {
		if !b.inShard(g.WorkloadID) {
			continue
		}
		c := ctr[g.ContainerID]
		p := pods[g.PodID]
		w := wl[g.WorkloadID]
		n := ns[w.NamespaceID]
		base := map[string]string{
			"gpu": strings.TrimPrefix(g.Device, "nvidia"), "UUID": g.UUID, "pci_bus_id": g.PCI,
			"device": g.Device, "modelName": g.Model, "Hostname": g.Hostname,
			"DCGM_FI_DRIVER_VERSION": g.Driver,
			"exported_namespace":     n.Name, "exported_pod": p.Name, "exported_container": c.Name,
			"job": "nvidia-dcgm-exporter", "namespace": "nvidia-gpu-operator",
			"container": "nvidia-dcgm-exporter", "endpoint": "gpu-metrics",
			"service": "nvidia-dcgm-exporter", "instance": "10.128.4.7:9400",
			"pod": "nvidia-dcgm-exporter-abcde",
		}
		if g.MIGProfile != "" {
			base["GPU_I_PROFILE"] = g.MIGProfile
			base["GPU_I_ID"] = "0"
		}
		rs := by[uint32(g.ID)]
		b.gpuSeries("DCGM_FI_DEV_GPU_UTIL", base, rs, func(r samples.GPURow) float64 { return float64(r.Util) })
		b.gpuSeries("DCGM_FI_DEV_MEM_COPY_UTIL", base, rs, func(r samples.GPURow) float64 { return float64(r.MemCopy) })
		b.gpuSeries("DCGM_FI_DEV_FB_USED", base, rs, func(r samples.GPURow) float64 { return float64(r.FBUsed) })
	}
}

func (b *builder) gpuSeries(name string, base map[string]string, rows []samples.GPURow, val func(samples.GPURow) float64) {
	if b.err != nil || len(rows) == 0 {
		return
	}
	lbls := withName(name, base)
	sort.Slice(rows, func(i, j int) bool { return rows[i].StepIdx < rows[j].StepIdx })
	var prev int64 = -1
	for _, r := range rows {
		ts := b.start + int64(r.StepIdx)*int64(b.stepSec)*1000
		if ts <= prev || !b.accept(ts) {
			continue
		}
		prev = ts
		if _, err := b.app.Append(0, lbls, ts, val(r)); err != nil {
			b.err = err
			return
		}
		b.appended++
	}
}

func withName(name string, base map[string]string) labels.Labels {
	m := map[string]string{"__name__": name}
	for k, v := range base {
		if v != "" {
			m[k] = v
		}
	}
	return labels.FromMap(m)
}

func clone(in map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range in {
		out[k] = v
	}
	return out
}

func ownerKind(w planner.Workload) string {
	if w.Kind == "StatefulSet" {
		return "StatefulSet"
	}
	return "ReplicaSet"
}

func ownerName(w planner.Workload) string {
	if w.Kind == "StatefulSet" {
		return w.Name
	}
	return w.Name + "-" + w.RSHash
}

func kindLower(k string) string {
	if k == "StatefulSet" {
		return "statefulset"
	}
	return "deployment"
}

func containerID(seed uint64, ctr int, life uint8) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d|%d|%d", seed, ctr, life)))
	return "cri-o://" + hex.EncodeToString(sum[:])
}

func cgroupID(uid, cid string) string {
	return "/kubepods.slice/pod" + uid + "/" + cid
}

func addThanosMeta(blockDir string, plan *planner.Plan, exportName string, external map[string]string) error {
	path := filepath.Join(blockDir, "meta.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	var meta map[string]any
	if err := json.Unmarshal(raw, &meta); err != nil {
		return err
	}
	lbl := map[string]string{
		"kubegenx_dataset": plan.Dataset,
		"kubegenx_export":  exportName,
		"cluster":          plan.ClusterName,
	}
	for k, v := range external {
		lbl[k] = v
	}
	meta["thanos"] = map[string]any{
		"labels":     lbl,
		"downsample": map[string]any{"resolution": 0},
		"source":     "kubegenx",
	}
	out, err := json.Marshal(meta)
	if err != nil {
		return err
	}
	return os.WriteFile(path, out, 0o644)
}

func copyTree(src, dst string) error {
	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if info.IsDir() {
			return os.MkdirAll(target, 0o775)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o775); err != nil {
			return err
		}
		in, err := os.Open(path)
		if err != nil {
			return err
		}
		defer in.Close()
		out, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
		if err != nil {
			return err
		}
		if _, err := io.Copy(out, in); err != nil {
			out.Close()
			return err
		}
		return out.Close()
	})
}

func dirSize(root string) (int64, error) {
	var n int64
	err := filepath.Walk(root, func(_ string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			n += info.Size()
		}
		return nil
	})
	return n, err
}
