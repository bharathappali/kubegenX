package planner

import (
	"strings"
	"testing"

	"github.com/bharathappali/kubegenx/internal/spec"
)

func TestCatalogSizes(t *testing.T) {
	if len(orgCatalog) != 100 {
		t.Fatalf("orgs %d", len(orgCatalog))
	}
	seen := map[string]struct{}{}
	for _, name := range orgCatalog {
		if _, ok := seen[name]; ok {
			t.Fatalf("duplicate org %s", name)
		}
		seen[name] = struct{}{}
		if name != strings.ToLower(name) || strings.Contains(name, " ") {
			t.Fatalf("org name %s", name)
		}
	}
	pool := clusterCatalog()
	if len(pool) != 10000 {
		t.Fatalf("clusters %d", len(pool))
	}
	if pool[0] != "us-east-1" || pool[499] != "us-east-500" || pool[500] != "us-west-1" {
		t.Fatalf("cluster names %s %s %s", pool[0], pool[499], pool[500])
	}
	if pool[len(pool)-1] != "mx-central-500" {
		t.Fatalf("last cluster %s", pool[len(pool)-1])
	}
}

func TestOrgRangeFollowsWorkloadCount(t *testing.T) {
	lo, hi := orgRange(5000)
	if lo != 2 || hi != 3 {
		t.Fatalf("5k orgs %d-%d", lo, hi)
	}
	lo, hi = orgRange(20000)
	if lo != 7 || hi != 10 {
		t.Fatalf("20k orgs %d-%d", lo, hi)
	}
	lo, hi = orgRange(100)
	if lo != 1 || hi != 2 {
		t.Fatalf("small orgs %d-%d", lo, hi)
	}
}

func TestTenancyLinksClusterToOneOrg(t *testing.T) {
	counts := make([]int, 80)
	for i := range counts {
		counts[i] = 200
	}
	orgs, clusters := assignTenancy(424242, counts)
	owner := map[string]string{}
	perOrg := map[string]int{}
	for i := range orgs {
		if orgs[i] == "" || clusters[i] == "" {
			t.Fatalf("empty label at %d", i)
		}
		if !strings.Contains(clusters[i], "-") {
			t.Fatalf("cluster %s", clusters[i])
		}
		if prev, ok := owner[clusters[i]]; ok && prev != orgs[i] {
			t.Fatalf("cluster %s on %s and %s", clusters[i], prev, orgs[i])
		}
		owner[clusters[i]] = orgs[i]
		perOrg[orgs[i]]++
	}
	if len(perOrg) < 7 || len(perOrg) > 10 {
		t.Fatalf("orgs %d", len(perOrg))
	}
	maxN, minN := 0, len(counts)
	for _, n := range perOrg {
		if n > maxN {
			maxN = n
		}
		if n < minN {
			minN = n
		}
	}
	if maxN < minN*2 {
		t.Fatalf("org spread max %d min %d", maxN, minN)
	}
	againO, againC := assignTenancy(424242, counts)
	for i := range orgs {
		if againO[i] != orgs[i] || againC[i] != clusters[i] {
			t.Fatal("tenancy is not stable for the same seed")
		}
	}
}

func TestSmallPlanUsesFewOrgs(t *testing.T) {
	tpl := spec.DefaultTemplate()
	p, err := Build("demo", tpl, "2026-10-05T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]struct{}{}
	for _, n := range p.Namespaces {
		if n.Labels["org_id"] == "" || n.Labels["cluster_id"] == "" {
			t.Fatalf("labels %+v", n.Labels)
		}
		seen[n.Labels["org_id"]] = struct{}{}
	}
	if len(seen) > 2 {
		t.Fatalf("orgs %d", len(seen))
	}
	mapped := OrgMapOf(p)
	if mapped.CatalogOrgs != 100 || mapped.CatalogClusters != 10000 {
		t.Fatalf("catalog %+v", mapped)
	}
	if len(mapped.Orgs) != len(seen) {
		t.Fatalf("map orgs %d", len(mapped.Orgs))
	}
}
