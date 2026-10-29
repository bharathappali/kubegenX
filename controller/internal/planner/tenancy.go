package planner

import (
	"math"
	"sort"
	"sync"

	"github.com/bharathappali/kubegenx/internal/philox"
)

// orgCatalog spells company names the way they are pronounced.
// amazon → amazan, google → googal, facebook → pacebuk, barclays → barklayz.
var orgCatalog = []string{
	"amazan", "googal", "pacebuk", "mykrosoft", "netflik",
	"aple", "tezla", "orakel", "salzfors", "adobi",
	"envidia", "intal", "wolmart", "targit", "kostko",
	"homdepot", "yoober", "lift", "airbneeb", "spotifai",
	"paypel", "veeza", "masterkard", "jaypeemorgan", "goldmun",
	"morganstanlee", "sitee", "welzfargo", "bankofamerika", "aychsbeesee",
	"barklayz", "doychbank", "beenpeepariba", "santandair", "sawftbank",
	"samzung", "sonee", "panasonik", "tosheeba", "hitachee",
	"seemens", "filips", "bosh", "beemveh", "mersaydeez",
	"folksvagen", "toyouta", "handa", "neesan", "hyunday",
	"valvo", "eekeeya", "neslay", "yoonileever", "pepsiko",
	"kokakola", "starbuks", "makdonaldz", "nykee", "adeedas",
	"diznee", "wornur", "komkast", "vurizon", "ayentee",
	"teemobeel", "vodafon", "orinj", "telefonika", "esaypee",
	"eyebeem", "del", "aychpee", "lenova", "sisko",
	"brodkom", "kwolkam", "ayemdee", "ayesemel", "teesemsee",
	"shopifai", "strype", "skwair", "twileeo", "snoflayk",
	"daytabriks", "palanteer", "kloudflair", "akamaye", "redhet",
	"kanonikal", "sooza", "doker", "hashikor", "mongodbee",
	"elastik", "konfluent", "daytadog", "splank", "servisnow",
}

// regionStems are the prefixes of cluster ids. The number after the stem is what varies.
var regionStems = []string{
	"us-east", "us-west", "us-central", "us-south",
	"eu-west", "eu-central", "eu-north", "eu-south",
	"ap-southeast", "ap-northeast", "ap-south", "ap-east",
	"sa-east", "ca-central", "ca-west",
	"af-south", "me-south", "me-central",
	"il-central", "mx-central",
}

const (
	catalogOrgs     = 100
	catalogClusters = 10000
	clustersPerStem = catalogClusters / 20
)

var (
	clusterOnce sync.Once
	clusterPool []string
)

func clusterCatalog() []string {
	clusterOnce.Do(func() {
		clusterPool = make([]string, 0, catalogClusters)
		for _, stem := range regionStems {
			for n := 1; n <= clustersPerStem; n++ {
				clusterPool = append(clusterPool, stem+"-"+itoa(n))
			}
		}
	})
	return clusterPool
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	var b [8]byte
	i := len(b)
	for n > 0 {
		i--
		b[i] = byte('0' + n%10)
		n /= 10
	}
	return string(b[i:])
}

// orgRange is how many orgs a dataset of this many workloads should draw.
// About 2–3 at 5k workloads, 7–10 past 15k, and only 1–2 for a small set.
func orgRange(workloads int) (int, int) {
	if workloads <= 0 {
		return 1, 1
	}
	if workloads < 1000 {
		return 1, 2
	}
	mid := 2.5 * math.Pow(float64(workloads)/5000, 1.15)
	lo := int(math.Floor(mid - 0.5))
	hi := int(math.Ceil(mid + 0.5))
	if lo < 1 {
		lo = 1
	}
	if hi < lo {
		hi = lo
	}
	if workloads >= 5000 && workloads < 7000 {
		if lo < 2 {
			lo = 2
		}
		if hi > 3 {
			hi = 3
		}
	}
	if workloads > 15000 {
		return 7, 10
	}
	if hi > 10 {
		hi = 10
	}
	if lo > hi {
		lo = hi
	}
	return lo, hi
}

func orgCount(workloads int, seed uint64) int {
	lo, hi := orgRange(workloads)
	if hi <= lo {
		return lo
	}
	return lo + int(philox.U32(seed, 30, uint32(workloads), 0, 0, 0)%uint32(hi-lo+1))
}

// clusterCount grows faster than the org count and stays under the namespace count,
// so a small dataset is not spread across thousands of clusters.
func clusterCount(workloads, namespaces, orgs int, seed uint64) int {
	if namespaces < 1 {
		return 0
	}
	if orgs < 1 {
		orgs = 1
	}
	if orgs > namespaces {
		orgs = namespaces
	}
	mid := float64(orgs) * math.Pow(float64(workloads)/1000, 0.7)
	if mid < float64(orgs) {
		mid = float64(orgs)
	}
	if mid > float64(namespaces) {
		mid = float64(namespaces)
	}
	center := int(math.Round(mid))
	span := int(math.Max(1, math.Round(mid*0.15)))
	lo, hi := center-span, center+span
	if lo < orgs {
		lo = orgs
	}
	if hi > namespaces {
		hi = namespaces
	}
	if hi < lo {
		hi = lo
	}
	if hi == lo {
		return lo
	}
	return lo + int(philox.U32(seed, 31, uint32(workloads), uint32(orgs), 0, 0)%uint32(hi-lo+1))
}

func expShares(n int) []float64 {
	shares := make([]float64, n)
	if n == 0 {
		return shares
	}
	const ratio = 1.75
	sum := 0.0
	for i := 0; i < n; i++ {
		shares[i] = math.Pow(ratio, float64(n-1-i))
		sum += shares[i]
	}
	for i := range shares {
		shares[i] /= sum
	}
	return shares
}

// splitByShare gives every part at least one item when total allows it,
// then hands the rest out by share so the first part is the largest.
func splitByShare(total int, shares []float64) []int {
	n := len(shares)
	out := make([]int, n)
	if n == 0 || total <= 0 {
		return out
	}
	if total < n {
		for i := 0; i < total; i++ {
			out[i] = 1
		}
		return out
	}
	left := total
	for i := range out {
		out[i] = 1
		left--
	}
	floors := make([]int, n)
	type frac struct {
		i    int
		part float64
	}
	rems := make([]frac, n)
	used := 0
	for i, s := range shares {
		v := s * float64(left)
		floors[i] = int(math.Floor(v))
		used += floors[i]
		rems[i] = frac{i, v - float64(floors[i])}
		out[i] += floors[i]
	}
	sort.SliceStable(rems, func(a, b int) bool {
		if rems[a].part == rems[b].part {
			return rems[a].i < rems[b].i
		}
		return rems[a].part > rems[b].part
	})
	extra := left - used
	for k := 0; k < extra && k < n; k++ {
		out[rems[k].i]++
	}
	return out
}

func weightedPick(seed uint64, feature, entity uint32, weights []float64) int {
	if len(weights) == 0 {
		return 0
	}
	sum := 0.0
	for _, w := range weights {
		sum += w
	}
	if sum <= 0 {
		return 0
	}
	total := 0
	ws := make([]int, len(weights))
	for i, w := range weights {
		v := int(math.Round(w / sum * 10000))
		if v < 1 {
			v = 1
		}
		ws[i] = v
		total += v
	}
	r := int(philox.U32(seed, feature, entity, 0, 0, 0) % uint32(total))
	acc := 0
	for i, v := range ws {
		acc += v
		if r < acc {
			return i
		}
	}
	return len(weights) - 1
}

// assignTenancy returns the org id and cluster id for each namespace.
// A cluster belongs to one org. Bigger orgs own more clusters, and the
// busiest namespaces land on those orgs.
func assignTenancy(seed uint64, counts []int) (orgs, clusters []string) {
	ns := len(counts)
	orgs = make([]string, ns)
	clusters = make([]string, ns)
	if ns == 0 {
		return orgs, clusters
	}
	total := 0
	for _, c := range counts {
		if c > 0 {
			total += c
		}
	}
	nOrg := orgCount(total, seed)
	if nOrg > ns {
		nOrg = ns
	}
	if nOrg > len(orgCatalog) {
		nOrg = len(orgCatalog)
	}
	nCl := clusterCount(total, ns, nOrg, seed)
	orgNames, err := pickNames(seed, 40, 0, orgCatalog, nOrg)
	if err != nil {
		orgNames = orgCatalog[:nOrg]
	}
	clNames, err := pickNames(seed, 41, 0, clusterCatalog(), nCl)
	if err != nil {
		clNames = clusterCatalog()[:nCl]
	}
	shares := expShares(nOrg)
	quota := splitByShare(nCl, shares)
	type slot struct {
		org   int
		index int
	}
	slots := make([]slot, 0, nCl)
	next := 0
	byOrg := make([][]int, nOrg)
	for o := 0; o < nOrg; o++ {
		for c := 0; c < quota[o]; c++ {
			slots = append(slots, slot{org: o, index: next})
			byOrg[o] = append(byOrg[o], len(slots)-1)
			next++
		}
	}
	order := make([]int, ns)
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(a, b int) bool {
		if counts[order[a]] == counts[order[b]] {
			return order[a] < order[b]
		}
		return counts[order[a]] > counts[order[b]]
	})
	placed := make([]bool, ns)
	limit := nCl
	if limit > len(order) {
		limit = len(order)
	}
	for i := 0; i < limit; i++ {
		ni := order[i]
		s := slots[i]
		orgs[ni] = orgNames[s.org]
		clusters[ni] = clNames[s.index]
		placed[ni] = true
	}
	slotW := make([]float64, len(slots))
	for i, s := range slots {
		rank := 0
		for k, id := range byOrg[s.org] {
			if id == i {
				rank = k
				break
			}
		}
		slotW[i] = shares[s.org] * math.Pow(1.9, float64(len(byOrg[s.org])-1-rank))
	}
	for _, ni := range order {
		if placed[ni] {
			continue
		}
		pick := weightedPick(seed, 42, uint32(ni), slotW)
		s := slots[pick]
		orgs[ni] = orgNames[s.org]
		clusters[ni] = clNames[s.index]
	}
	return orgs, clusters
}

// OrgMap is the org-to-cluster mapping that a plan actually contains.
type OrgMap struct {
	CatalogOrgs     int      `json:"catalogOrgs"`
	CatalogClusters int      `json:"catalogClusters"`
	Orgs            []OrgRow `json:"orgs"`
}

type OrgRow struct {
	OrgID      string       `json:"orgId"`
	Namespaces int          `json:"namespaces"`
	Workloads  int          `json:"workloads"`
	Clusters   []ClusterRow `json:"clusters"`
}

type ClusterRow struct {
	ClusterID  string `json:"clusterId"`
	Namespaces int    `json:"namespaces"`
	Workloads  int    `json:"workloads"`
}

func OrgMapOf(plan *Plan) OrgMap {
	type acc struct {
		ns int
		wl int
	}
	workloads := map[int]int{}
	if plan != nil {
		for _, w := range plan.Workloads {
			workloads[w.NamespaceID]++
		}
	}
	orgOrder := []string{}
	orgs := map[string]*OrgRow{}
	seen := map[string]map[string]*acc{}
	if plan != nil {
		for _, n := range plan.Namespaces {
			orgID := n.Labels["org_id"]
			clID := n.Labels["cluster_id"]
			row := orgs[orgID]
			if row == nil {
				row = &OrgRow{OrgID: orgID}
				orgs[orgID] = row
				orgOrder = append(orgOrder, orgID)
				seen[orgID] = map[string]*acc{}
			}
			wl := workloads[n.ID]
			row.Namespaces++
			row.Workloads += wl
			bucket := seen[orgID][clID]
			if bucket == nil {
				bucket = &acc{}
				seen[orgID][clID] = bucket
			}
			bucket.ns++
			bucket.wl += wl
		}
	}
	out := make([]OrgRow, 0, len(orgOrder))
	for _, id := range orgOrder {
		row := *orgs[id]
		cls := make([]ClusterRow, 0, len(seen[id]))
		for cl, bucket := range seen[id] {
			cls = append(cls, ClusterRow{ClusterID: cl, Namespaces: bucket.ns, Workloads: bucket.wl})
		}
		sort.Slice(cls, func(i, j int) bool {
			if cls[i].Workloads == cls[j].Workloads {
				return cls[i].ClusterID < cls[j].ClusterID
			}
			return cls[i].Workloads > cls[j].Workloads
		})
		row.Clusters = cls
		out = append(out, row)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Workloads == out[j].Workloads {
			return out[i].OrgID < out[j].OrgID
		}
		return out[i].Workloads > out[j].Workloads
	})
	return OrgMap{CatalogOrgs: len(orgCatalog), CatalogClusters: len(clusterCatalog()), Orgs: out}
}
