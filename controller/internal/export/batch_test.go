package export

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bharathappali/kubegenx/internal/planner"
	"github.com/bharathappali/kubegenx/internal/samples"
)

func TestBlockJobsSplitsSixHours(t *testing.T) {
	plan := &planner.Plan{
		StepSeconds: 15, Steps: 1440, ChunkSamples: 1440,
		Items: []planner.WorkItem{
			{Index: 0, Chunk: 0, WlLo: 0, WlHi: 10},
			{Index: 1, Chunk: 0, WlLo: 10, WlHi: 20},
		},
	}
	jobs := BlockJobs(plan)
	if len(jobs) != 6 {
		t.Fatalf("jobs %d", len(jobs))
	}
	if jobs[0].ID != "c0000-i000000-s0" || !jobs[0].EmitNS || jobs[3].EmitNS || jobs[3].Index != 1 {
		t.Fatalf("first %+v fourth %+v", jobs[0], jobs[3])
	}
	if jobs[0].StepHi-jobs[0].StepLo != 480 {
		t.Fatalf("span %+v", jobs[0])
	}
}

func TestBatchWritesOneBlockPerSlice(t *testing.T) {
	root := t.TempDir()
	dataset := "tiny"
	plan := &planner.Plan{
		Dataset: dataset, Epoch: "2026-10-05T00:00:00Z", StepSeconds: 15, Steps: 4, ChunkSamples: 4, Chunks: 1,
		Namespaces: []planner.Namespace{{ID: 0, Name: "ns"}},
		Workloads:  []planner.Workload{{ID: 0, NamespaceID: 0, Kind: "Deployment", Name: "api", RSHash: "abc", Replicas: 1}},
		Pods:       []planner.Pod{{ID: 0, WorkloadID: 0, Name: "api-0", UID: "u", Node: "n", IP: "10.0.0.2"}},
		Containers: []planner.Container{{ID: 0, PodID: 0, WorkloadID: 0, Name: "app", Image: "app:1"}},
		Items:      []planner.WorkItem{{ID: "i", Index: 0, WlLo: 0, WlHi: 1, Chunk: 0}},
	}
	if slices := itemSlices(plan, plan.Items[0]); len(slices) != 1 {
		t.Fatalf("slices %+v", slices)
	}
	cpu := []samples.CPURow{
		{CtrID: 0, StepIdx: 0, UsageUs: 100},
		{CtrID: 0, StepIdx: 1, UsageUs: 200},
		{CtrID: 0, StepIdx: 2, UsageUs: 300},
		{CtrID: 0, StepIdx: 3, UsageUs: 400},
	}
	mem := []samples.MemRow{
		{CtrID: 0, StepIdx: 0, WsBytes: 10, RssBytes: 8},
		{CtrID: 0, StepIdx: 3, WsBytes: 13, RssBytes: 11},
	}
	start := time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)
	dest := filepath.Join(root, "tsdb")
	if err := os.MkdirAll(dest, 0o755); err != nil {
		t.Fatal(err)
	}
	n, _, err := writeBlock(context.Background(), plan, cpu, mem, nil, t.TempDir(), "tiny", "s0", start, start, start.Add(time.Minute), dest, nil, 0, 1, 0, 4, true)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("blocks %d", n)
	}
	kept := filterCPU(cpu, 1, 3)
	if len(kept) != 2 || kept[0].StepIdx != 1 || kept[1].StepIdx != 2 {
		t.Fatalf("filter %+v", kept)
	}
}

func TestBatchWorkersStayWithinCPUs(t *testing.T) {
	if jobBudget != podMemory*75/100 || podMemory-jobBudget != podMemory/4 {
		t.Fatalf("budget %d pod %d", jobBudget, podMemory)
	}
	plan := &planner.Plan{
		StepSeconds: 15, Steps: 5760, ChunkSamples: 1440,
		Workloads:  make([]planner.Workload, 25000),
		Containers: make([]planner.Container, 100000),
	}
	decoded := itemBytes(plan)
	items, sliceN, parts := planLayout(decoded, writerBytes, 3, 8)
	peak := decoded + int64(sliceN)*writerBytes
	if items != 4 || sliceN != 1 || parts != 4 {
		t.Fatalf("25k layout items %d slices %d parts %d peak %d", items, sliceN, parts, peak)
	}
	if int64(items)*peak > jobBudget {
		t.Fatalf("over budget items %d peak %d", items, peak)
	}
	if items*sliceN*parts > 16 {
		t.Fatalf("writers %d", items*sliceN*parts)
	}
	// A small item keeps a read in flight per two CPUs and does not trade reads for extra slices.
	smallItems, smallSlices, smallParts := planLayout(64<<20, 64<<20, 3, 8)
	if smallItems != 16 || smallSlices != 1 || smallParts != 1 {
		t.Fatalf("small layout %d %d %d", smallItems, smallSlices, smallParts)
	}
	dir := t.TempDir()
	it := planner.WorkItem{Index: 0, WlLo: 0, WlHi: 1024, Chunk: 0}
	path := samples.ItemFile(dir, "cpu", 0, 0)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, make([]byte, 4096), 0o644); err != nil {
		t.Fatal(err)
	}
	est := estimateItem(plan, dir, it)
	if est.disk != 4096 || est.slices != 3 || est.perSlice != writerBytes {
		t.Fatalf("scan %+v", est)
	}
	if est.charge(1) != est.decoded+est.perSlice {
		t.Fatalf("charge %d", est.charge(1))
	}
	prev := readHeap
	readHeap = func() int64 { return 0 }
	defer func() { readHeap = prev }()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	gate := newMemGate(100, 4)
	go gate.tick(ctx)
	if _, err := gate.acquire(ctx, 40); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.acquire(ctx, 40); err != nil {
		t.Fatal(err)
	}
	if _, err := gate.acquire(ctx, 40); err == nil {
		t.Fatal("third acquire should wait for a free ticket")
	}
	got := splitRange(0, 25000, parts)
	if len(got) == 0 || got[0][0] != 0 || got[len(got)-1][1] != 25000 {
		t.Fatalf("range %+v", got)
	}
	for i := 1; i < len(got); i++ {
		if got[i][0] != got[i-1][1] {
			t.Fatalf("gap %+v", got)
		}
	}
	own := map[uint32]int{7: 10, 8: 40}
	kept := filterCPUPart([]samples.CPURow{
		{CtrID: 7, StepIdx: 1},
		{CtrID: 8, StepIdx: 1},
		{CtrID: 7, StepIdx: 9},
	}, 0, 5, own, 0, 20)
	if len(kept) != 1 || kept[0].CtrID != 7 {
		t.Fatalf("part filter %+v", kept)
	}
}
