package planner

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"unicode"

	"github.com/bharathappali/kubegenx/internal/generate"
	"github.com/bharathappali/kubegenx/internal/philox"
	"github.com/bharathappali/kubegenx/internal/spec"
	"golang.org/x/text/unicode/norm"
)

const podAlphabet = "bcdfghjklmnpqrstvwxz2456789"

var statelessRoles = []string{"api", "web", "worker", "gateway", "auth", "frontend", "backend", "scheduler", "notifier", "search", "payments", "catalog"}
var statefulRoles = []string{"postgres", "mysql", "redis", "kafka", "etcd", "mongo", "zookeeper", "elasticsearch", "cassandra", "rabbitmq"}
var sidecarNames = []string{"envoy-proxy", "log-shipper", "metrics-agent", "istio-proxy", "config-reloader"}

type Plan struct {
	Dataset      string      `json:"dataset"`
	Seed         uint64      `json:"seed"`
	Epoch        string      `json:"epoch"`
	StepSeconds  int         `json:"stepSeconds"`
	Steps        int         `json:"steps"`
	ChunkSamples int         `json:"chunkSamples"`
	Chunks       int         `json:"chunks"`
	Digest       string      `json:"digest"`
	Namespaces   []Namespace `json:"namespaces"`
	Workloads    []Workload  `json:"workloads"`
	Pods         []Pod       `json:"pods"`
	Containers   []Container `json:"containers"`
	GPUs         []GPUEntity `json:"gpus"`
	Nodes        []Node      `json:"nodes"`
	Items        []WorkItem  `json:"items"`
	LabelKeys    []string    `json:"labelKeys"`
	ClusterName  string      `json:"clusterName"`
	Kruize       bool        `json:"kruize"`
}

type Namespace struct {
	ID     int               `json:"id"`
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
}

type Workload struct {
	ID          int    `json:"id"`
	NamespaceID int    `json:"namespaceId"`
	Kind        string `json:"kind"`
	Name        string `json:"name"`
	RSHash      string `json:"rsHash"`
	Replicas    int    `json:"replicas"`
	GPU         bool   `json:"gpu"`
}

type Pod struct {
	ID         int    `json:"id"`
	WorkloadID int    `json:"workloadId"`
	Name       string `json:"name"`
	UID        string `json:"uid"`
	Node       string `json:"node"`
	IP         string `json:"ip"`
	Ordinal    int    `json:"ordinal"`
}

type Container struct {
	ID            int    `json:"id"`
	PodID         int    `json:"podId"`
	WorkloadID    int    `json:"workloadId"`
	Name          string `json:"name"`
	Image         string `json:"image"`
	ImageID       string `json:"imageId"`
	CPUMeanMilli  uint32 `json:"cpuMeanMilli"`
	CPULimitMilli uint32 `json:"cpuLimitMilli"`
	CPUReqMilli   uint32 `json:"cpuReqMilli"`
	MemMeanBytes  int64  `json:"memMeanBytes"`
	MemLimitBytes int64  `json:"memLimitBytes"`
	MemReqBytes   int64  `json:"memReqBytes"`
	RestartAt     int    `json:"restartAt"`
	QoS           string `json:"qos"`
	Sidecar       bool   `json:"sidecar"`
}

type GPUEntity struct {
	ID          int    `json:"id"`
	ContainerID int    `json:"containerId"`
	PodID       int    `json:"podId"`
	WorkloadID  int    `json:"workloadId"`
	Model       string `json:"model"`
	UUID        string `json:"uuid"`
	Device      string `json:"device"`
	Hostname    string `json:"hostname"`
	PCI         string `json:"pci"`
	FBTotalMiB  uint32 `json:"fbTotalMiB"`
	UtilLo      uint32 `json:"utilLo"`
	UtilHi      uint32 `json:"utilHi"`
	Driver      string `json:"driver"`
	MIGProfile  string `json:"migProfile,omitempty"`
}

type Node struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	GPU  bool   `json:"gpu"`
	Zone string `json:"zone"`
}

type WorkItem struct {
	ID    string `json:"id"`
	Index int    `json:"index"`
	WlLo  int    `json:"wlLo"`
	WlHi  int    `json:"wlHi"`
	Chunk int    `json:"chunk"`
}

type country struct {
	continent string
	name      string
	cities    []string
}

// Build expands a template into a deterministic plan.
func Build(dataset string, tpl spec.Template, epoch string) (*Plan, error) {
	if err := validate(tpl); err != nil {
		return nil, err
	}
	step, err := spec.ParseDuration(or(tpl.Time.Step, "15s"))
	if err != nil {
		return nil, err
	}
	dur, err := spec.ParseDuration(or(tpl.Time.Duration, "24h"))
	if err != nil {
		return nil, err
	}
	chunk, err := spec.ParseDuration(or(tpl.Time.Chunk, "6h"))
	if err != nil {
		return nil, err
	}
	if step <= 0 || chunk < step || dur < step {
		return nil, fmt.Errorf("invalid time range")
	}
	stepSec := int(step.Seconds())
	chunkSamples := int(chunk / step)
	steps := int(dur / step)
	chunks := steps / chunkSamples
	if chunks < 1 {
		chunks = 1
		steps = chunkSamples
	}
	// Drop a partial trailing chunk so every file is full.
	steps = chunks * chunkSamples

	geo, err := loadCountries()
	if err != nil {
		return nil, err
	}
	nsN, wlPer, err := dimensions(tpl)
	if err != nil {
		return nil, err
	}
	p := &Plan{
		Dataset:      dataset,
		Seed:         tpl.Seed,
		Epoch:        epoch,
		StepSeconds:  stepSec,
		Steps:        steps,
		ChunkSamples: chunkSamples,
		Chunks:       chunks,
		ClusterName:  "kgx-" + dataset,
		Kruize:       true,
	}
	if err := p.assignNamespaces(geo, nsN, tpl); err != nil {
		return nil, err
	}
	if err := p.assignWorkloads(geo, wlPer, tpl); err != nil {
		return nil, err
	}
	p.assignNodes(tpl)
	p.assignPodsAndContainers(tpl)
	p.assignGPUs(tpl)
	p.assignItems(tpl)
	if err := p.checkExperimentNames(); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(p)
	if err != nil {
		return nil, err
	}
	sum := sha256.Sum256(raw)
	p.Digest = hex.EncodeToString(sum[:])
	// Digest covers the body without itself; re-marshal is stable because
	// Digest was empty during the first marshal and we set it after.
	return p, nil
}

func (p *Plan) checkExperimentNames() error {
	seen := map[string]struct{}{}
	ns := map[int]string{}
	for _, n := range p.Namespaces {
		ns[n.ID] = n.Name
	}
	for _, w := range p.Workloads {
		kind := strings.ToLower(w.Kind)
		name := fmt.Sprintf("%s|%s|%s|%s", p.Dataset, ns[w.NamespaceID], kind, w.Name)
		if _, ok := seen[name]; ok {
			return fmt.Errorf("Failed(DuplicateWorkloadName): %s", name)
		}
		if len(name) > 255 {
			return fmt.Errorf("experiment name longer than 255: %s", name)
		}
		seen[name] = struct{}{}
	}
	return nil
}

func (p *Plan) assignNamespaces(geo []country, n int, tpl spec.Template) error {
	order := shuffle(tpl.Seed, len(geo), 1)
	// Prefer countries that can hold the workloads, stable by city-count among the shuffle.
	sort.SliceStable(order, func(i, j int) bool {
		return len(geo[order[i]].cities) > len(geo[order[j]].cities)
	})
	used := map[string]int{}
	for len(p.Namespaces) < n {
		if len(order) == 0 {
			return fmt.Errorf("not enough countries")
		}
		c := geo[order[0]]
		order = order[1:]
		base := dns(c.continent + "-" + c.name)
		used[base]++
		name := base
		if used[base] > 1 {
			name = fmt.Sprintf("%s-%d", base, used[base])
		}
		labels := map[string]string{}
		keys := tpl.WorkloadLabels.Keys
		if len(keys) == 0 {
			keys = spec.DefaultTemplate().WorkloadLabels.Keys
		}
		var keyNames []string
		for _, k := range keys {
			labels[k.Key] = weightedValue(tpl.Seed, uint32(len(p.Namespaces)), k)
			keyNames = append(keyNames, k.Key)
		}
		p.LabelKeys = keyNames
		p.Namespaces = append(p.Namespaces, Namespace{ID: len(p.Namespaces), Name: name, Labels: labels})
	}
	return nil
}

func (p *Plan) assignWorkloads(geo []country, per int, tpl spec.Template) error {
	byName := map[string]country{}
	for _, c := range geo {
		byName[dns(c.continent+"-"+c.name)] = c
	}
	depShare := 80
	if tpl.Topology.Kinds != nil {
		if v, ok := tpl.Topology.Kinds["Deployment"]; ok {
			depShare = int(math.Round(v * 100))
		}
	}
	buckets := tpl.Topology.Replicas.Buckets
	if len(buckets) == 0 {
		buckets = spec.DefaultTemplate().Topology.Replicas.Buckets
	}
	stsBuckets := tpl.Topology.Replicas.StatefulSetBuckets
	if len(stsBuckets) == 0 {
		stsBuckets = spec.DefaultTemplate().Topology.Replicas.StatefulSetBuckets
	}
	gpuN := gpuCount(len(p.Namespaces)*per, tpl.GPU)
	gpuSet := map[int]bool{}
	if gpuN > 0 {
		perm := shuffle(tpl.Seed, len(p.Namespaces)*per, 21)
		for i := 0; i < gpuN; i++ {
			gpuSet[perm[i]] = true
		}
	}
	wlID := 0
	for _, ns := range p.Namespaces {
		base := ns.Name
		if i := strings.LastIndex(base, "-"); i > 0 {
			if _, err := strconv.Atoi(base[i+1:]); err == nil && usedGeneration(base) {
				base = base[:i]
			}
		}
		c, ok := byName[base]
		if !ok {
			// generation suffix stripped incorrectly; fall back to any country
			c = geo[ns.ID%len(geo)]
		}
		perm := shuffle(tpl.Seed, max(per, len(c.cities)), uint32(1000+ns.ID))
		seen := map[string]int{}
		for j := 0; j < per; j++ {
			kind := "Deployment"
			if int(philox.U32(tpl.Seed, 12, uint32(wlID), 0, 0, 0)%100) >= depShare {
				kind = "StatefulSet"
			}
			roles := statelessRoles
			bks := buckets
			if kind == "StatefulSet" {
				roles = statefulRoles
				bks = stsBuckets
			}
			city := c.cities[perm[j%len(perm)]%len(c.cities)]
			role := roles[(perm[j%len(perm)]/max(1, len(c.cities)))%len(roles)]
			name := dns(city + "-" + role)
			if len(name) > 46 {
				name = trimDNS(name, 46)
			}
			seen[name]++
			if seen[name] > 1 {
				name = trimDNS(fmt.Sprintf("%s-%s", name, base36(uint32(seen[name]))), 46)
			}
			reps := pickReplicas(tpl.Seed, uint32(wlID), bks)
			rs := encodeN(philox.U32(tpl.Seed, 13, uint32(wlID), 0, 0, 1), 10)
			p.Workloads = append(p.Workloads, Workload{
				ID: wlID, NamespaceID: ns.ID, Kind: kind, Name: name, RSHash: rs, Replicas: reps, GPU: gpuSet[wlID],
			})
			wlID++
		}
	}
	return nil
}

func usedGeneration(name string) bool {
	i := strings.LastIndex(name, "-")
	if i < 0 {
		return false
	}
	_, err := strconv.Atoi(name[i+1:])
	return err == nil && len(name[i+1:]) <= 3
}

func (p *Plan) assignNodes(tpl spec.Template) {
	zone := "us-east-2"
	// One general node plus one GPU node when needed. More open if maxPods is hit.
	p.Nodes = append(p.Nodes, Node{ID: 0, Name: nodeName(0, zone), Zone: zone})
	needGPU := false
	for _, w := range p.Workloads {
		if w.GPU {
			needGPU = true
			break
		}
	}
	if needGPU {
		p.Nodes = append(p.Nodes, Node{ID: 1, Name: nodeName(1, zone), GPU: true, Zone: zone})
	}
	_ = tpl
}

func (p *Plan) assignPodsAndContainers(tpl spec.Template) {
	maxPods := tpl.Nodes.MaxPodsPerNode
	if maxPods <= 0 {
		maxPods = 250
	}
	cpuNodes := []int{}
	gpuNodes := []int{}
	for _, n := range p.Nodes {
		if n.GPU {
			gpuNodes = append(gpuNodes, n.ID)
		} else {
			cpuNodes = append(cpuNodes, n.ID)
		}
	}
	load := map[int]int{}
	nsOf := map[int]int{}
	for _, w := range p.Workloads {
		nsOf[w.ID] = w.NamespaceID
	}
	_ = nsOf
	for _, w := range p.Workloads {
		nCont := pickContainerCount(tpl.Seed, uint32(w.ID), tpl.Topology.ContainersPerPod)
		names := containerNames(tpl.Seed, uint32(w.ID), w.Name, nCont)
		for r := 0; r < w.Replicas; r++ {
			var podName string
			if w.Kind == "StatefulSet" {
				podName = fmt.Sprintf("%s-%d", w.Name, r)
			} else {
				podName = fmt.Sprintf("%s-%s-%s", w.Name, w.RSHash, encodeN(philox.U32(tpl.Seed, 14, uint32(w.ID), uint32(r), 0, 0), 5))
			}
			if len(podName) > 63 {
				podName = trimDNS(podName, 63)
			}
			pool := cpuNodes
			if w.GPU && len(gpuNodes) > 0 {
				pool = gpuNodes
			}
			nodeID := pool[0]
			for _, id := range pool {
				if load[id] < maxPods {
					nodeID = id
					break
				}
			}
			load[nodeID]++
			if load[nodeID] >= maxPods {
				id := len(p.Nodes)
				p.Nodes = append(p.Nodes, Node{ID: id, Name: nodeName(id, "us-east-2"), GPU: w.GPU, Zone: "us-east-2"})
				if w.GPU {
					gpuNodes = append(gpuNodes, id)
				} else {
					cpuNodes = append(cpuNodes, id)
				}
			}
			podID := len(p.Pods)
			uid := uuidFrom(tpl.Seed, uint32(podID))
			ip := fmt.Sprintf("10.%d.%d.%d", (podID>>16)&255, (podID>>8)&255, podID&255)
			if ip == "10.0.0.0" {
				ip = "10.0.0.1"
			}
			p.Pods = append(p.Pods, Pod{ID: podID, WorkloadID: w.ID, Name: podName, UID: uid, Node: p.Nodes[nodeID].Name, IP: ip, Ordinal: r})
			for i, cn := range names {
				cid := len(p.Containers)
				imageRole := cn
				image := fmt.Sprintf("quay.io/kubegenx/%s:2.4.1", imageRole)
				sum := sha256.Sum256([]byte(fmt.Sprintf("%d|%s", tpl.Seed, image)))
				bandCPU, bandMem := pickBands(tpl, uint32(cid), i > 0)
				lo, hi := generate.CPUBandMilli(bandCPU)
				mean := generate.PickMilli(tpl.Seed, uint32(cid), lo, hi)
				mlo, mhi := generate.MemBandBytes(bandMem)
				mem := generate.PickBytes(tpl.Seed, uint32(cid), mlo, mhi)
				sizing := pickSizing(tpl, uint32(cid))
				req, lim, mreq, mlim, qos := applySizing(sizing, mean, mem)
				restart := -1
				if pickAnomaly(tpl, uint32(cid)) == "oomLoop" {
					restart = p.Steps / 3
					if restart < 1 {
						restart = 1
					}
				}
				p.Containers = append(p.Containers, Container{
					ID: cid, PodID: podID, WorkloadID: w.ID, Name: cn, Image: image,
					ImageID: hex.EncodeToString(sum[:]), CPUMeanMilli: mean, CPULimitMilli: lim, CPUReqMilli: req,
					MemMeanBytes: mem, MemLimitBytes: mlim, MemReqBytes: mreq, RestartAt: restart, QoS: qos, Sidecar: i > 0,
				})
			}
		}
	}
}

func (p *Plan) assignGPUs(tpl spec.Template) {
	if !tpl.GPU.Enabled {
		return
	}
	gpuPods := map[int]bool{}
	for _, w := range p.Workloads {
		if w.GPU {
			gpuPods[w.ID] = true
		}
	}
	nodeOfPod := map[int]string{}
	for _, pod := range p.Pods {
		nodeOfPod[pod.ID] = pod.Node
	}
	dev := 0
	for _, c := range p.Containers {
		if !gpuPods[c.WorkloadID] || c.Sidecar {
			continue
		}
		// One GPU on the primary container of each replica.
		id := len(p.GPUs)
		fb := uint32(40960)
		model := "NVIDIA A100-SXM4-40GB"
		mig := ""
		if tpl.GPU.MigRatio > 0 && philox.U32(tpl.Seed, 15, uint32(c.ID), 0, 0, 0)%100 < uint32(tpl.GPU.MigRatio*100) {
			mig = "3g.20gb"
			fb = 20480
		}
		p.GPUs = append(p.GPUs, GPUEntity{
			ID: id, ContainerID: c.ID, PodID: c.PodID, WorkloadID: c.WorkloadID,
			Model: model, UUID: gpuUUID(tpl.Seed, uint32(id)), Device: fmt.Sprintf("nvidia%d", dev%8),
			Hostname: nodeOfPod[c.PodID], PCI: fmt.Sprintf("00000000:%02X:00.0", 0x10+dev),
			FBTotalMiB: fb, UtilLo: 30, UtilHi: 70, Driver: "550.90.07", MIGProfile: mig,
		})
		dev++
	}
}

func (p *Plan) assignItems(tpl spec.Template) {
	wpi := tpl.WorkItem.WorkloadsPerItem
	if wpi <= 0 {
		wpi = 1024
	}
	for chunk := 0; chunk < p.Chunks; chunk++ {
		for lo := 0; lo < len(p.Workloads); lo += wpi {
			hi := lo + wpi
			if hi > len(p.Workloads) {
				hi = len(p.Workloads)
			}
			idx := lo / wpi
			p.Items = append(p.Items, WorkItem{
				ID:    fmt.Sprintf("i-%06d-c%04d", idx, chunk),
				Index: idx,
				WlLo:  lo,
				WlHi:  hi,
				Chunk: chunk,
			})
		}
	}
}

func gpuCount(n int, g spec.GPU) int {
	if !g.Enabled || n == 0 || g.WorkloadRatio <= 0 {
		return 0
	}
	k := int(math.Round(g.WorkloadRatio * float64(n)))
	if k < 1 {
		k = 1
	}
	if k > n {
		k = n
	}
	return k
}

func pickReplicas(seed uint64, entity uint32, buckets []spec.Bucket) int {
	r := philox.U32(seed, 9, entity, 0, 0, 3) % 100
	var acc uint32
	for _, b := range buckets {
		acc += uint32(b.Share)
		if r < acc {
			span := b.Range[1] - b.Range[0] + 1
			if span < 1 {
				span = 1
			}
			return b.Range[0] + int(philox.U32(seed, 9, entity, 0, 0, 4)%uint32(span))
		}
	}
	return 1
}

func pickContainerCount(seed uint64, wl uint32, weights map[string]float64) int {
	if len(weights) == 0 {
		weights = map[string]float64{"1": 0.75, "2": 0.18, "3": 0.07}
	}
	type pair struct {
		n     int
		share int
	}
	var ps []pair
	total := 0
	for k, v := range weights {
		n, _ := strconv.Atoi(k)
		if n <= 0 {
			continue
		}
		s := int(math.Round(v * 100))
		if s <= 0 {
			continue
		}
		ps = append(ps, pair{n, s})
		total += s
	}
	sort.Slice(ps, func(i, j int) bool { return ps[i].n < ps[j].n })
	if total == 0 {
		return 1
	}
	r := int(philox.U32(seed, 16, wl, 0, 0, 0) % uint32(total))
	acc := 0
	for _, p := range ps {
		acc += p.share
		if r < acc {
			return p.n
		}
	}
	return 1
}

func containerNames(seed uint64, wl uint32, workloadName string, n int) []string {
	role := workloadName
	if i := strings.LastIndex(workloadName, "-"); i > 0 {
		role = workloadName[i+1:]
	}
	// Role may include a city prefix already stripped once; if the role isn't known, use "app".
	known := false
	for _, r := range append(append([]string{}, statelessRoles...), statefulRoles...) {
		if r == role {
			known = true
			break
		}
	}
	if !known {
		role = "app"
	}
	names := []string{role}
	if n <= 1 {
		return names
	}
	perm := shuffle(seed, len(sidecarNames), wl+17)
	for i := 0; i < n-1 && i < len(perm); i++ {
		names = append(names, sidecarNames[perm[i]])
	}
	return names
}

func pickBands(tpl spec.Template, entity uint32, sidecar bool) (cpu, mem string) {
	if sidecar {
		return "low", "low"
	}
	cpu = weightedKey(tpl.Seed, entity, 18, tpl.Profiles.CPU, "medium")
	mem = weightedKey(tpl.Seed, entity, 19, tpl.Profiles.Memory, "medium")
	return cpu, mem
}

func pickSizing(tpl spec.Template, entity uint32) string {
	return weightedKey(tpl.Seed, entity, 20, tpl.Profiles.Sizing, "rightSized")
}

func pickAnomaly(tpl spec.Template, entity uint32) string {
	m := tpl.Profiles.Anomalies
	if len(m) == 0 {
		return "none"
	}
	total := 0
	keys := make([]string, 0, len(m))
	for k, v := range m {
		s := int(math.Round(v * 100))
		if s > 0 {
			total += s
			keys = append(keys, k)
		}
	}
	sort.Strings(keys)
	if total <= 0 || total > 100 {
		// Treat shares as weights even if they don't sum to 100.
		if total == 0 {
			return "none"
		}
	}
	none := 100 - total
	if none < 0 {
		none = 0
	}
	r := int(philox.U32(tpl.Seed, 22, entity, 0, 0, 0) % uint32(total+none))
	if r >= total {
		return "none"
	}
	acc := 0
	for _, k := range keys {
		acc += int(math.Round(m[k] * 100))
		if r < acc {
			return k
		}
	}
	return "none"
}

func applySizing(policy string, cpuMean uint32, memMean int64) (cpuReq, cpuLim uint32, memReq, memLim int64, qos string) {
	switch policy {
	case "over", "overProvisioned":
		cpuReq = cpuMean * 5
		cpuLim = cpuReq * 3 / 2
		memReq = memMean * 5
		memLim = memReq * 3 / 2
		qos = "Burstable"
	case "under", "underProvisioned":
		cpuReq = cpuMean / 2
		if cpuReq < 1 {
			cpuReq = 1
		}
		cpuLim = cpuMean
		memReq = memMean / 2
		memLim = memMean
		qos = "Burstable"
	case "noLimits":
		cpuReq = cpuMean * 12 / 10
		memReq = memMean * 12 / 10
		qos = "Burstable"
	case "bestEffort":
		qos = "BestEffort"
	case "guaranteed":
		cpuReq = cpuMean
		cpuLim = cpuMean
		memReq = memMean
		memLim = memMean
		qos = "Guaranteed"
	default:
		cpuReq = cpuMean * 12 / 10
		cpuLim = cpuMean * 2
		memReq = memMean * 12 / 10
		memLim = memMean * 2
		qos = "Burstable"
	}
	return cpuReq, cpuLim, memReq, memLim, qos
}

func weightedKey(seed uint64, entity, salt uint32, m map[string]float64, def string) string {
	if len(m) == 0 {
		return def
	}
	keys := make([]string, 0, len(m))
	total := 0
	for k, v := range m {
		s := int(math.Round(v * 100))
		if s > 0 {
			keys = append(keys, k)
			total += s
		}
	}
	sort.Strings(keys)
	if total == 0 {
		return def
	}
	r := int(philox.U32(seed, salt, entity, 0, 0, 0) % uint32(total))
	acc := 0
	for _, k := range keys {
		acc += int(math.Round(m[k] * 100))
		if r < acc {
			return k
		}
	}
	return keys[0]
}

func weightedValue(seed uint64, ns uint32, k spec.LabelKey) string {
	total := 0
	ws := make([]int, len(k.Values))
	for i, v := range k.Values {
		w := v.Weight
		if w <= 0 {
			w = 1
		}
		ws[i] = w
		total += w
	}
	if total == 0 {
		return "default"
	}
	h := uint32(0)
	for _, c := range k.Key {
		h = h*33 + uint32(c)
	}
	r := int(philox.U32(seed, 11, ns, h, 0, 0) % uint32(total))
	acc := 0
	for i, v := range k.Values {
		acc += ws[i]
		if r < acc {
			return v.Value
		}
	}
	return k.Values[0].Value
}

func dimensions(tpl spec.Template) (ns, per int, err error) {
	mode := tpl.Topology.Mode
	if mode == "" {
		mode = "NamespaceMatrix"
	}
	switch mode {
	case "NamespaceMatrix":
		ns = tpl.Topology.Namespaces.Count
		per = tpl.Topology.WorkloadsPerNamespace
		if ns < 1 || per < 1 {
			return 0, 0, fmt.Errorf("namespace matrix needs namespaces and workloadsPerNamespace")
		}
	case "TotalWorkloads":
		total := tpl.Topology.TotalWorkloads
		if total < 1 {
			return 0, 0, fmt.Errorf("totalWorkloads required")
		}
		ns = tpl.Topology.Namespaces.Count
		if ns < 1 {
			ns = int(math.Ceil(math.Sqrt(float64(total))))
			if ns > 2000 {
				ns = 2000
			}
			if ns < 1 {
				ns = 1
			}
		}
		per = (total + ns - 1) / ns
	default:
		return 0, 0, fmt.Errorf("unsupported topology mode %s", mode)
	}
	return ns, per, nil
}

func validate(tpl spec.Template) error {
	var sum int
	for _, b := range tpl.Topology.Replicas.Buckets {
		if b.Range[0] < 1 || b.Range[1] > 10 || b.Range[0] > b.Range[1] {
			return fmt.Errorf("replica range out of 1..10")
		}
		sum += b.Share
	}
	if len(tpl.Topology.Replicas.Buckets) > 0 && sum != 100 {
		return fmt.Errorf("replica shares must sum to 100")
	}
	return nil
}

func loadCountries() ([]country, error) {
	var raw map[string]map[string][]string
	if err := json.Unmarshal(mappingJSON, &raw); err != nil {
		return nil, err
	}
	var out []country
	continents := make([]string, 0, len(raw))
	for c := range raw {
		continents = append(continents, c)
	}
	sort.Strings(continents)
	for _, cont := range continents {
		countries := make([]string, 0, len(raw[cont]))
		for name := range raw[cont] {
			countries = append(countries, name)
		}
		sort.Strings(countries)
		for _, name := range countries {
			seen := map[string]struct{}{}
			var cities []string
			for _, city := range raw[cont][name] {
				n := dns(city)
				if n == "" {
					continue
				}
				if _, ok := seen[n]; ok {
					continue
				}
				seen[n] = struct{}{}
				cities = append(cities, n)
			}
			if len(cities) == 0 {
				continue
			}
			out = append(out, country{continent: dns(cont), name: dns(name), cities: cities})
		}
	}
	return out, nil
}

func shuffle(seed uint64, n int, salt uint32) []int {
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	for i := n - 1; i > 0; i-- {
		j := int(philox.U32(seed, 8, salt, uint32(i), 0, 0) % uint32(i+1))
		idx[i], idx[j] = idx[j], idx[i]
	}
	return idx
}

func dns(s string) string {
	s = norm.NFKD.String(s)
	var b strings.Builder
	prevHyphen := false
	for _, r := range s {
		if r > 127 {
			continue
		}
		r = unicode.ToLower(r)
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
			prevHyphen = false
			continue
		}
		if !prevHyphen && b.Len() > 0 {
			b.WriteByte('-')
			prevHyphen = true
		}
	}
	out := strings.Trim(b.String(), "-")
	for strings.Contains(out, "--") {
		out = strings.ReplaceAll(out, "--", "-")
	}
	return out
}

func trimDNS(s string, n int) string {
	if len(s) <= n {
		return s
	}
	s = s[:n]
	s = strings.TrimRight(s, "-")
	return s
}

func encodeN(v uint32, n int) string {
	out := make([]byte, n)
	for i := n - 1; i >= 0; i-- {
		out[i] = podAlphabet[v%uint32(len(podAlphabet))]
		v /= uint32(len(podAlphabet))
	}
	return string(out)
}

func base36(v uint32) string {
	const alpha = "0123456789abcdefghijklmnopqrstuvwxyz"
	if v == 0 {
		return "0"
	}
	var b [8]byte
	i := len(b)
	for v > 0 && i > 0 {
		i--
		b[i] = alpha[v%36]
		v /= 36
	}
	s := string(b[i:])
	if len(s) > 3 {
		return s[len(s)-3:]
	}
	return s
}

func nodeName(id int, zone string) string {
	a := (id / 65536) % 256
	b := (id / 256) % 256
	c := id % 256
	suffix := "compute.internal"
	if zone == "us-east-1" {
		suffix = "ec2.internal"
	}
	return fmt.Sprintf("ip-10-%d-%d-%d.%s.%s", a, b, c, zone, suffix)
}

func uuidFrom(seed uint64, id uint32) string {
	a := philox.U32(seed, 30, id, 1, 0, 0)
	b := philox.U32(seed, 30, id, 2, 0, 0)
	c := philox.U32(seed, 30, id, 3, 0, 0)
	d := philox.U32(seed, 30, id, 4, 0, 0)
	return fmt.Sprintf("%08x-%04x-4%03x-%04x-%08x%04x", a, uint16(b>>16), uint16(b)&0x0fff, (uint16(c>>16)&0x3fff)|0x8000, c, uint16(d))
}

func gpuUUID(seed uint64, id uint32) string {
	return "GPU-" + uuidFrom(seed, 100000+id)
}

func or(a, b string) string {
	if a == "" {
		return b
	}
	return a
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// ExperimentName is the Kruize name for a workload.
func ExperimentName(dataset, namespace, kind, workload string) string {
	return dataset + "|" + namespace + "|" + strings.ToLower(kind) + "|" + workload
}

// NamespaceByID indexes namespaces.
func (p *Plan) NamespaceByID(id int) Namespace {
	for _, n := range p.Namespaces {
		if n.ID == id {
			return n
		}
	}
	return Namespace{}
}
