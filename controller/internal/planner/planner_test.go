package planner

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/bharathappali/kubegenx/internal/spec"
)

func TestEmbedLoads(t *testing.T) {
	if len(mappingJSON) < 1000 {
		t.Fatal("mapping.json missing")
	}
	geo, err := loadCountries()
	if err != nil {
		t.Fatal(err)
	}
	if len(geo) < 100 {
		t.Fatalf("countries %d", len(geo))
	}
}

func TestPlanSmallDeterministic(t *testing.T) {
	tpl := spec.DefaultTemplate()
	tpl.Topology.Mode = "NamespaceMatrix"
	tpl.Topology.Namespaces.Count = 3
	tpl.Topology.WorkloadsPerNamespace = 5
	tpl.Time = spec.Time{Step: "15s", Duration: "24h", Chunk: "6h"}
	tpl.GPU.Enabled = true
	tpl.GPU.WorkloadRatio = 1.0 / 15.0
	a, err := Build("e2e", tpl, "2026-10-05T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	b, err := Build("e2e", tpl, "2026-10-05T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	aj, _ := json.Marshal(a)
	bj, _ := json.Marshal(b)
	if string(aj) != string(bj) {
		t.Fatal("plan not deterministic")
	}
	if len(a.Workloads) != 15 {
		t.Fatalf("workloads %d", len(a.Workloads))
	}
	if len(a.Namespaces) != 3 {
		t.Fatalf("ns %d", len(a.Namespaces))
	}
	byNS := map[int]int{}
	for _, w := range a.Workloads {
		byNS[w.NamespaceID]++
	}
	for id, c := range byNS {
		if c != 5 {
			t.Fatalf("namespace %d has %d workloads, want 5 each: %+v", id, c, byNS)
		}
	}
	if a.Chunks != 4 || a.Steps != 5760 || a.ChunkSamples != 1440 {
		t.Fatalf("time chunks=%d steps=%d cs=%d", a.Chunks, a.Steps, a.ChunkSamples)
	}
	if len(a.Items) != 4 {
		t.Fatalf("items %d", len(a.Items))
	}
	gpus := 0
	for _, w := range a.Workloads {
		if w.GPU {
			gpus++
		}
		if !dns1123(w.Name) || len(w.Name) > 46 {
			t.Fatalf("bad workload name %s", w.Name)
		}
	}
	if gpus != 1 {
		t.Fatalf("gpu workloads %d", gpus)
	}
	for _, n := range a.Namespaces {
		if !dns1123(n.Name) {
			t.Fatalf("bad ns %s", n.Name)
		}
		if n.Labels["org_id"] == "" || n.Labels["cluster_id"] == "" {
			t.Fatalf("labels %+v", n.Labels)
		}
	}
	for _, pod := range a.Pods {
		if !dns1123(pod.Name) || len(pod.Name) > 63 {
			t.Fatalf("bad pod %s", pod.Name)
		}
	}
	if len(a.GPUs) < 1 {
		t.Fatal("no gpu entities")
	}
	if a.GPUs[0].Model != "NVIDIA A100-SXM4-40GB" && a.GPUs[0].Model == "" {
		t.Fatal(a.GPUs[0].Model)
	}
}

func TestPlanHundredThousand(t *testing.T) {
	tpl := spec.DefaultTemplate()
	tpl.Topology.Mode = "TotalWorkloads"
	tpl.Topology.TotalWorkloads = 100000
	tpl.Topology.Namespaces.Count = 0
	tpl.Topology.Namespaces.Distribution = "Zipf"
	tpl.Topology.Namespaces.ZipfS = 1
	tpl.GPU.Enabled = true
	tpl.GPU.WorkloadRatio = 0.15
	tpl.Time = spec.Time{Step: "15s", Duration: "15d", Chunk: "6h"}
	p, err := Build("gifted-golick", tpl, "2026-10-05T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	geo, err := loadCountries()
	if err != nil {
		t.Fatal(err)
	}
	pool := map[string]struct{}{}
	for _, name := range countryPlaceNames(geo) {
		pool[name] = struct{}{}
	}
	if len(p.Workloads) != 100000 {
		t.Fatalf("workloads %d", len(p.Workloads))
	}
	if len(p.Namespaces) != 316 {
		t.Fatalf("namespaces %d", len(p.Namespaces))
	}
	seenNS := map[string]struct{}{}
	byNS := map[int]map[string]struct{}{}
	for _, ns := range p.Namespaces {
		if _, ok := pool[ns.Name]; !ok || len(ns.Name) > 46 {
			t.Fatalf("namespace %s", ns.Name)
		}
		if _, ok := seenNS[ns.Name]; ok {
			t.Fatalf("duplicate namespace %s", ns.Name)
		}
		seenNS[ns.Name] = struct{}{}
	}
	for _, w := range p.Workloads {
		if _, ok := pool[w.Name]; !ok || len(w.Name) > 46 {
			t.Fatalf("workload %s", w.Name)
		}
		if byNS[w.NamespaceID] == nil {
			byNS[w.NamespaceID] = map[string]struct{}{}
		}
		if _, ok := byNS[w.NamespaceID][w.Name]; ok {
			t.Fatalf("duplicate workload %s in namespace %d", w.Name, w.NamespaceID)
		}
		byNS[w.NamespaceID][w.Name] = struct{}{}
	}
	est := int64(len(p.Containers)) * int64(p.Steps) * 16
	reserved := est + est/3
	t.Logf("namespaces=%d workloads=%d pods=%d containers=%d gpus=%d steps=%d chunks=%d items=%d est=%d reserved=%d",
		len(p.Namespaces), len(p.Workloads), len(p.Pods), len(p.Containers), len(p.GPUs), p.Steps, p.Chunks, len(p.Items), est, reserved)
	if reserved > 1500*1024*1024*1024 {
		t.Fatalf("reserved %d exceeds 1500Gi", reserved)
	}
}

func TestNamespacesCapAtCountries(t *testing.T) {
	geo, err := loadCountries()
	if err != nil {
		t.Fatal(err)
	}
	tpl := spec.DefaultTemplate()
	tpl.Topology.Mode = "TotalWorkloads"
	tpl.Topology.TotalWorkloads = len(geo) + 2
	tpl.Topology.Namespaces.Count = len(geo) + 2
	tpl.Topology.Namespaces.Distribution = "Uniform"
	tpl.Time = spec.Time{Step: "15s", Duration: "6h", Chunk: "6h"}
	tpl.GPU.Enabled = false
	p, err := Build("wide", tpl, "2026-10-05T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	wantNS := len(geo) + 2
	if len(p.Namespaces) != wantNS {
		t.Fatalf("namespaces %d", len(p.Namespaces))
	}
	if len(p.Workloads) != wantNS {
		t.Fatalf("workloads %d", len(p.Workloads))
	}
	pool := map[string]struct{}{}
	for _, name := range countryPlaceNames(geo) {
		pool[name] = struct{}{}
	}
	for _, ns := range p.Namespaces {
		if _, ok := pool[ns.Name]; !ok {
			t.Fatalf("namespace %s", ns.Name)
		}
	}
	for _, w := range p.Workloads {
		if _, ok := pool[w.Name]; !ok {
			t.Fatalf("workload %s", w.Name)
		}
	}
}

func TestTotalWorkloadsSkewed(t *testing.T) {
	tpl := spec.DefaultTemplate()
	tpl.Topology.Mode = "TotalWorkloads"
	tpl.Topology.TotalWorkloads = 15
	tpl.Topology.Namespaces.Count = 0
	tpl.Topology.Namespaces.Distribution = "Zipf"
	tpl.Topology.Namespaces.ZipfS = 1
	tpl.Time = spec.Time{Step: "15s", Duration: "6h", Chunk: "6h"}
	p, err := Build("skew", tpl, "2026-10-05T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if len(p.Workloads) != 15 {
		t.Fatalf("workloads %d", len(p.Workloads))
	}
	counts := map[int]int{}
	for _, w := range p.Workloads {
		counts[w.NamespaceID]++
	}
	if len(counts) < 2 {
		t.Fatalf("namespaces %+v", counts)
	}
	min, max, sum := 100, 0, 0
	for _, c := range counts {
		if c < min {
			min = c
		}
		if c > max {
			max = c
		}
		sum += c
	}
	if sum != 15 {
		t.Fatalf("sum %d %+v", sum, counts)
	}
	if max-min < 2 {
		t.Fatalf("distribution too even %+v", counts)
	}
}

func TestBucketMean(t *testing.T) {
	buckets := spec.DefaultTemplate().Topology.Replicas.Buckets
	var sum int
	n := 100000
	for i := 0; i < n; i++ {
		sum += pickReplicas(424242, uint32(i), buckets)
	}
	mean := float64(sum) / float64(n)
	if mean < 3.20 || mean > 3.46 {
		t.Fatalf("mean %f", mean)
	}
}

func TestDuplicateFailsClosed(t *testing.T) {
	p := &Plan{Dataset: "d", Namespaces: []Namespace{{ID: 0, Name: "ns"}, {ID: 1, Name: "other"}},
		Workloads: []Workload{
			{ID: 0, NamespaceID: 0, Kind: "Deployment", Name: "a"},
			{ID: 1, NamespaceID: 1, Kind: "Deployment", Name: "a"},
		}}
	if err := p.checkExperimentNames(); err != nil {
		t.Fatal(err)
	}
	p.Workloads = append(p.Workloads, Workload{ID: 2, NamespaceID: 0, Kind: "StatefulSet", Name: "a"})
	if err := p.checkExperimentNames(); err == nil || !strings.Contains(err.Error(), "DuplicateWorkloadName") {
		t.Fatal(err)
	}
}

func TestReplicasShareRequestsAndLimits(t *testing.T) {
	tpl := spec.DefaultTemplate()
	tpl.Topology.Mode = "NamespaceMatrix"
	tpl.Topology.Namespaces.Count = 1
	tpl.Topology.WorkloadsPerNamespace = 4
	tpl.Topology.Kinds = map[string]float64{"Deployment": 1}
	tpl.Topology.Replicas.Buckets = []spec.Bucket{{Range: [2]int{3, 3}, Share: 100}}
	tpl.Time = spec.Time{Step: "15s", Duration: "1h", Chunk: "1h"}
	tpl.GPU.Enabled = false
	plan, err := Build("share", tpl, "2026-10-15T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	byPod := map[int][]Container{}
	for _, c := range plan.Containers {
		byPod[c.PodID] = append(byPod[c.PodID], c)
	}
	podsOf := map[int][]Pod{}
	for _, p := range plan.Pods {
		podsOf[p.WorkloadID] = append(podsOf[p.WorkloadID], p)
	}
	checked := 0
	for _, w := range plan.Workloads {
		pods := podsOf[w.ID]
		if len(pods) < 2 {
			t.Fatalf("workload %s replicas %d", w.Name, len(pods))
		}
		first := map[string]Container{}
		for _, c := range byPod[pods[0].ID] {
			first[c.Name] = c
		}
		for _, p := range pods[1:] {
			for _, c := range byPod[p.ID] {
				src := first[c.Name]
				if c.CPUReqMilli != src.CPUReqMilli || c.CPULimitMilli != src.CPULimitMilli || c.MemReqBytes != src.MemReqBytes || c.MemLimitBytes != src.MemLimitBytes || c.QoS != src.QoS {
					t.Fatalf("pod %s container %s req/lim %+v template %+v", p.Name, c.Name, c, src)
				}
				checked++
			}
		}
	}
	if checked < 1 {
		t.Fatal("no replica containers compared")
	}
}

func dns1123(s string) bool {
	if s == "" || len(s) > 63 {
		return false
	}
	for i, c := range s {
		ok := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-'
		if !ok {
			return false
		}
		if (i == 0 || i == len(s)-1) && c == '-' {
			return false
		}
	}
	return true
}
