package planner

// CatalogNamespace is the on-disk index of one namespace.
// It is written beside the plan and is not derived from Parquet.
type CatalogNamespace struct {
	OrgID     string            `json:"orgId"`
	ClusterID string            `json:"clusterId"`
	Namespace string            `json:"namespace"`
	Workloads []CatalogWorkload `json:"workloads"`
}

// CatalogWorkload is one workload inside a namespace, with the pods and
// container names that belong to it.
type CatalogWorkload struct {
	Kind       string   `json:"kind"`
	Name       string   `json:"name"`
	Pods       []string `json:"pods"`
	Containers []string `json:"containers"`
}

// Catalog is the namespace → workload map stored at
// datasets/<name>/plan/catalog.json when the dataset is created.
func Catalog(plan *Plan) []CatalogNamespace {
	if plan == nil {
		return []CatalogNamespace{}
	}
	pods := map[int][]string{}
	for _, p := range plan.Pods {
		pods[p.WorkloadID] = append(pods[p.WorkloadID], p.Name)
	}
	ctrs := map[int][]string{}
	seen := map[int]map[string]bool{}
	for _, c := range plan.Containers {
		if seen[c.WorkloadID] == nil {
			seen[c.WorkloadID] = map[string]bool{}
		}
		if seen[c.WorkloadID][c.Name] {
			continue
		}
		seen[c.WorkloadID][c.Name] = true
		ctrs[c.WorkloadID] = append(ctrs[c.WorkloadID], c.Name)
	}
	byNS := map[int][]CatalogWorkload{}
	for _, w := range plan.Workloads {
		podNames := pods[w.ID]
		if podNames == nil {
			podNames = []string{}
		}
		names := ctrs[w.ID]
		if names == nil {
			names = []string{}
		}
		byNS[w.NamespaceID] = append(byNS[w.NamespaceID], CatalogWorkload{
			Kind: w.Kind, Name: w.Name, Pods: podNames, Containers: names,
		})
	}
	out := make([]CatalogNamespace, 0, len(plan.Namespaces))
	for _, n := range plan.Namespaces {
		wls := byNS[n.ID]
		if wls == nil {
			wls = []CatalogWorkload{}
		}
		out = append(out, CatalogNamespace{
			OrgID: n.Labels["org_id"], ClusterID: n.Labels["cluster_id"],
			Namespace: n.Name, Workloads: wls,
		})
	}
	return out
}
