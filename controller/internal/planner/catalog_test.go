package planner

import "testing"

func TestCatalogKeepsOrgClusterPodsAndContainers(t *testing.T) {
	plan := &Plan{
		Namespaces: []Namespace{{
			ID: 0, Name: "payments",
			Labels: map[string]string{"org_id": "amazan", "cluster_id": "us-east-1"},
		}},
		Workloads: []Workload{{ID: 0, NamespaceID: 0, Kind: "Deployment", Name: "checkout"}},
		Pods:      []Pod{{ID: 0, WorkloadID: 0, Name: "checkout-a"}, {ID: 1, WorkloadID: 0, Name: "checkout-b"}},
		Containers: []Container{
			{ID: 0, PodID: 0, WorkloadID: 0, Name: "app"},
			{ID: 1, PodID: 1, WorkloadID: 0, Name: "app"},
			{ID: 2, PodID: 0, WorkloadID: 0, Name: "sidecar"},
		},
	}
	got := Catalog(plan)
	if len(got) != 1 || got[0].OrgID != "amazan" || got[0].ClusterID != "us-east-1" || got[0].Namespace != "payments" {
		t.Fatalf("namespace %+v", got)
	}
	if len(got[0].Workloads) != 1 {
		t.Fatalf("workloads %+v", got[0].Workloads)
	}
	w := got[0].Workloads[0]
	if w.Kind != "Deployment" || w.Name != "checkout" || len(w.Pods) != 2 || len(w.Containers) != 2 || w.Containers[0] != "app" || w.Containers[1] != "sidecar" {
		t.Fatalf("workload %+v", w)
	}
}
