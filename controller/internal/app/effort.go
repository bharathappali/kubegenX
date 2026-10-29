package app

import (
	"context"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"github.com/bharathappali/kubegenx/internal/spec"
)

const (
	workerCPU = "500m"
	workerMem = "1Gi"
	// One 2h block keeps about two cores busy and peaks under 4GiB
	// (decoded parquet plus the TSDB writers). No limit, so a fatter block can burst.
	converterCPU = "2"
	converterMem = "4Gi"
)

func effortPercent(effort string) int {
	switch strings.ToLower(strings.TrimSpace(effort)) {
	case "low":
		return 5
	case "medium":
		return 15
	case "high":
		return 40
	default:
		return 80
	}
}

// workerCount is at least 2, or the share of free worker-sized slots for the effort.
// A slot is one worker's CPU request and one worker's memory request, whichever free pool is smaller.
func workerCount(pct int, freeCPUMilli, workerCPUMilli, freeMem, workerMem int64) int {
	if pct < 1 {
		pct = 80
	}
	if workerCPUMilli < 1 {
		workerCPUMilli = 500
	}
	if workerMem < 1 {
		workerMem = 1 << 30
	}
	cpuSlots := freeCPUMilli / workerCPUMilli
	memSlots := freeMem / workerMem
	slots := cpuSlots
	if memSlots < slots {
		slots = memSlots
	}
	if slots < 0 {
		slots = 0
	}
	n := slots * int64(pct) / 100
	if n < 2 {
		n = 2
	}
	return int(n)
}

func (a *App) sizeWorkers(ctx context.Context, effort string, w spec.Workers) spec.Workers {
	if w.CPU == "" {
		w.CPU = workerCPU
	}
	if w.Memory == "" {
		w.Memory = workerMem
	}
	if w.Threads < 1 {
		w.Threads = 2
	}
	cpuQ := resource.MustParse(w.CPU)
	memQ := resource.MustParse(w.Memory)
	cpu := cpuQ.MilliValue()
	mem := memQ.Value()
	freeCPU, freeMem, err := a.freeCapacity(ctx)
	if err != nil {
		slogErr("free capacity", err)
		w.Count = 2
		return w
	}
	w.Count = workerCount(effortPercent(effort), freeCPU, cpu, freeMem, mem)
	return w
}

func (a *App) freeCapacity(ctx context.Context) (cpuMilli, memBytes int64, err error) {
	nodes, err := a.Kube.CoreV1().Nodes().List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, 0, err
	}
	ready := map[string]struct{}{}
	for _, n := range nodes.Items {
		if n.Spec.Unschedulable || !nodeReady(n) {
			continue
		}
		cpuMilli += n.Status.Allocatable.Cpu().MilliValue()
		memBytes += n.Status.Allocatable.Memory().Value()
		ready[n.Name] = struct{}{}
	}
	pods, err := a.Kube.CoreV1().Pods("").List(ctx, metav1.ListOptions{})
	if err != nil {
		return 0, 0, err
	}
	for _, p := range pods.Items {
		if p.Status.Phase == corev1.PodSucceeded || p.Status.Phase == corev1.PodFailed {
			continue
		}
		if _, ok := ready[p.Spec.NodeName]; !ok {
			continue
		}
		c, m := podRequest(p)
		cpuMilli -= c
		memBytes -= m
	}
	if cpuMilli < 0 {
		cpuMilli = 0
	}
	if memBytes < 0 {
		memBytes = 0
	}
	return cpuMilli, memBytes, nil
}

func nodeReady(n corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}
	return false
}

func podRequest(p corev1.Pod) (cpuMilli, memBytes int64) {
	var sumCPU, sumMem int64
	var initCPU, initMem int64
	for _, c := range p.Spec.Containers {
		sumCPU += c.Resources.Requests.Cpu().MilliValue()
		sumMem += c.Resources.Requests.Memory().Value()
	}
	for _, c := range p.Spec.InitContainers {
		if v := c.Resources.Requests.Cpu().MilliValue(); v > initCPU {
			initCPU = v
		}
		if v := c.Resources.Requests.Memory().Value(); v > initMem {
			initMem = v
		}
	}
	if initCPU > sumCPU {
		sumCPU = initCPU
	}
	if initMem > sumMem {
		sumMem = initMem
	}
	return sumCPU, sumMem
}
