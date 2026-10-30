package kruize

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/bharathappali/kubegenx/internal/planner"
	"github.com/bharathappali/kubegenx/internal/samples"
	"github.com/parquet-go/parquet-go"
)

func TestAggregatePageReadsOnlyTheChunkForThatPage(t *testing.T) {
	dir := t.TempDir()
	plan := &planner.Plan{
		StepSeconds: 15, Steps: 8, ChunkSamples: 4, Chunks: 2,
		Namespaces: []planner.Namespace{{ID: 0, Name: "payments", Labels: map[string]string{"org_id": "amazan", "cluster_id": "us-east-1"}}},
		Workloads:  []planner.Workload{{ID: 0, NamespaceID: 0, Kind: "Deployment", Name: "checkout", Replicas: 1}},
		Pods:       []planner.Pod{{ID: 0, WorkloadID: 0, Name: "checkout-0"}},
		Containers: []planner.Container{{ID: 7, PodID: 0, WorkloadID: 0, Name: "app"}},
		Items: []planner.WorkItem{
			{Index: 0, WlLo: 0, WlHi: 1, Chunk: 0},
			{Index: 0, WlLo: 0, WlHi: 1, Chunk: 1},
			{Index: 1, WlLo: 9, WlHi: 10, Chunk: 0},
		},
	}
	writeCPU(t, samples.ItemFile(dir, "cpu", 0, 0), []cpuDisk{
		{CtrID: 7, StepIdx: 0, UsageUs: 15_000_000},
		{CtrID: 7, StepIdx: 1, UsageUs: 30_000_000},
		{CtrID: 7, StepIdx: 2, UsageUs: 45_000_000},
		{CtrID: 7, StepIdx: 3, UsageUs: 60_000_000},
	})
	writeMem(t, samples.ItemFile(dir, "memory", 0, 0), []memDisk{
		{CtrID: 7, StepIdx: 0, WsBytes: 1000, RssBytes: 800},
	})
	writeCPU(t, samples.ItemFile(dir, "cpu", 1, 0), []cpuDisk{
		{CtrID: 7, LifeSeq: 1, StepIdx: 4, UsageUs: 0},
		{CtrID: 7, LifeSeq: 1, StepIdx: 5, UsageUs: 0},
		{CtrID: 7, LifeSeq: 1, StepIdx: 6, UsageUs: 0},
		{CtrID: 7, LifeSeq: 1, StepIdx: 7, UsageUs: 0},
	})
	origin := time.Date(2026, 10, 17, 0, 0, 0, 0, time.UTC)
	end := origin.Add(2 * time.Minute)
	first, err := AggregatePage(plan, dir, origin, time.Minute, "payments", "checkout", origin, end, 1)
	if err != nil {
		t.Fatal(err)
	}
	if first.Intervals != 1 || first.Total != 2 || first.Pages != 2 || len(first.Items) != 1 {
		t.Fatalf("page %+v", first)
	}
	if first.Items[0].Start != origin || first.Items[0].Metrics["cpuUsage"].Avg != 1 {
		t.Fatalf("first bucket %+v", first.Items[0])
	}
	second, err := AggregatePage(plan, dir, origin, time.Minute, "payments", "checkout", origin, end, 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Items) != 1 || second.Items[0].Start != origin.Add(time.Minute) || second.Items[0].Metrics["cpuUsage"].Avg != 0 {
		t.Fatalf("second bucket %+v", second.Items)
	}
}

type cpuDisk struct {
	CtrID       uint32 `parquet:"ctr_id"`
	LifeSeq     int32  `parquet:"life_seq"`
	StepIdx     uint32 `parquet:"step_idx"`
	UsageUs     int64  `parquet:"usage_us"`
	ThrottledUs int64  `parquet:"throttled_us"`
}

type memDisk struct {
	CtrID    uint32 `parquet:"ctr_id"`
	LifeSeq  int32  `parquet:"life_seq"`
	StepIdx  uint32 `parquet:"step_idx"`
	WsBytes  int64  `parquet:"ws_bytes"`
	RssBytes int64  `parquet:"rss_bytes"`
}

func writeCPU(t *testing.T, path string, rows []cpuDisk) {
	t.Helper()
	writeRows(t, path, rows)
}

func writeMem(t *testing.T, path string, rows []memDisk) {
	t.Helper()
	writeRows(t, path, rows)
}

func writeRows[T any](t *testing.T, path string, rows []T) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := parquet.WriteFile(path, rows); err != nil {
		t.Fatal(err)
	}
}
