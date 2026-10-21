package kruize

import (
	"strings"
	"testing"

	"github.com/bharathappali/kubegenx/internal/planner"
	"github.com/bharathappali/kubegenx/internal/spec"
)

func TestExperimentJSONShape(t *testing.T) {
	tpl := spec.DefaultTemplate()
	tpl.Time.Duration = "1d"
	tpl.GPU.WorkloadRatio = 1.0 / 15.0
	tpl.Topology.Namespaces.Count = 3
	tpl.Topology.WorkloadsPerNamespace = 5
	plan, err := planner.Build("e2e", tpl, "2026-10-05T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	gpus := 0
	for _, w := range plan.Workloads {
		if w.GPU {
			gpus++
		}
	}
	if gpus != 1 {
		t.Fatalf("gpu workloads %d", gpus)
	}
	data, total, pages := Experiments(plan, 1, 100)
	if total != 15 || pages != 1 || len(data) != 15 {
		t.Fatalf("total=%d pages=%d len=%d", total, pages, len(data))
	}
	seen := map[string]bool{}
	for _, exp := range data {
		if _, ok := exp["labels"]; ok {
			t.Fatal("experiment payload includes labels")
		}
		name, _ := exp["experiment_name"].(string)
		parts := strings.Split(name, "|")
		if len(parts) != 4 || parts[0] != "e2e" {
			t.Fatalf("name %q", name)
		}
		if seen[name] {
			t.Fatalf("duplicate %s", name)
		}
		seen[name] = true
	}
}
