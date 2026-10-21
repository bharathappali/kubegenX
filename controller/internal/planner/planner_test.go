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
	p := &Plan{Dataset: "d", Namespaces: []Namespace{{ID: 0, Name: "ns"}},
		Workloads: []Workload{{ID: 0, NamespaceID: 0, Kind: "Deployment", Name: "a"}, {ID: 1, NamespaceID: 0, Kind: "Deployment", Name: "a"}}}
	if err := p.checkExperimentNames(); err == nil || !strings.Contains(err.Error(), "DuplicateWorkloadName") {
		t.Fatal(err)
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
