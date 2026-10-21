// Package spec is the template / dataset / export document model.
package spec

import "time"

type Template struct {
	Seed           uint64         `json:"seed" yaml:"seed"`
	Topology       Topology       `json:"topology" yaml:"topology"`
	Profiles       Profiles       `json:"profiles" yaml:"profiles"`
	GPU            GPU            `json:"gpu" yaml:"gpu"`
	Nodes          Nodes          `json:"nodes" yaml:"nodes"`
	Time           Time           `json:"time" yaml:"time"`
	Features       []string       `json:"features" yaml:"features"`
	WorkloadLabels WorkloadLabels `json:"workloadLabels" yaml:"workloadLabels"`
	Workers        Workers        `json:"workers" yaml:"workers"`
	WorkItem       WorkItem       `json:"workItem" yaml:"workItem"`
}

type Topology struct {
	Mode                  string             `json:"mode" yaml:"mode"`
	TotalWorkloads        int                `json:"totalWorkloads" yaml:"totalWorkloads"`
	Namespaces            NamespaceSpec      `json:"namespaces" yaml:"namespaces"`
	WorkloadsPerNamespace int                `json:"workloadsPerNamespace" yaml:"workloadsPerNamespace"`
	Kinds                 map[string]float64 `json:"kinds" yaml:"kinds"`
	Replicas              Replicas           `json:"replicas" yaml:"replicas"`
	ContainersPerPod      map[string]float64 `json:"containersPerPod" yaml:"containersPerPod"`
}

type NamespaceSpec struct {
	Count        int     `json:"count" yaml:"count"`
	Distribution string  `json:"distribution" yaml:"distribution"`
	ZipfS        float64 `json:"zipfS" yaml:"zipfS"`
}

type Replicas struct {
	Buckets            []Bucket `json:"buckets" yaml:"buckets"`
	StatefulSetBuckets []Bucket `json:"statefulSetBuckets" yaml:"statefulSetBuckets"`
}

type Bucket struct {
	Range [2]int `json:"range" yaml:"range"`
	Share int    `json:"share" yaml:"share"`
}

type Profiles struct {
	CPU        map[string]float64 `json:"cpu" yaml:"cpu"`
	Memory     map[string]float64 `json:"memory" yaml:"memory"`
	Sizing     map[string]float64 `json:"sizing" yaml:"sizing"`
	Behaviours map[string]float64 `json:"behaviours" yaml:"behaviours"`
	Anomalies  map[string]float64 `json:"anomalies" yaml:"anomalies"`
}

type GPU struct {
	Enabled       bool     `json:"enabled" yaml:"enabled"`
	WorkloadRatio float64  `json:"workloadRatio" yaml:"workloadRatio"`
	NodePools     []string `json:"nodePools" yaml:"nodePools"`
	MigRatio      float64  `json:"migRatio" yaml:"migRatio"`
	LabelProfile  string   `json:"labelProfile" yaml:"labelProfile"`
}

type Nodes struct {
	Pools          []NodePool `json:"pools" yaml:"pools"`
	MaxPodsPerNode int        `json:"maxPodsPerNode" yaml:"maxPodsPerNode"`
}

type NodePool struct {
	InstanceType string  `json:"instanceType" yaml:"instanceType"`
	Weight       float64 `json:"weight" yaml:"weight"`
}

type Time struct {
	Step     string `json:"step" yaml:"step"`
	Duration string `json:"duration" yaml:"duration"`
	Chunk    string `json:"chunk" yaml:"chunk"`
	Epoch    string `json:"epoch" yaml:"epoch"`
}

type LabelValue struct {
	Value  string `json:"value" yaml:"value"`
	Weight int    `json:"weight" yaml:"weight"`
}

type LabelKey struct {
	Key    string       `json:"key" yaml:"key"`
	Values []LabelValue `json:"values" yaml:"values"`
}

type WorkloadLabels struct {
	Scope   string     `json:"scope" yaml:"scope"`
	Keys    []LabelKey `json:"keys" yaml:"keys"`
	CarryOn []string   `json:"carryOn" yaml:"carryOn"`
}

type Workers struct {
	Count   int    `json:"count" yaml:"count"`
	CPU     string `json:"cpu" yaml:"cpu"`
	Memory  string `json:"memory" yaml:"memory"`
	Threads int    `json:"threads" yaml:"threads"`
}

type WorkItem struct {
	WorkloadsPerItem int `json:"workloadsPerItem" yaml:"workloadsPerItem"`
}

type Dataset struct {
	TemplateRef      TemplateRef    `json:"templateRef" yaml:"templateRef"`
	TemplateSnapshot Template       `json:"templateSnapshot" yaml:"templateSnapshot"`
	Epoch            string         `json:"epoch" yaml:"epoch"`
	Step             string         `json:"step" yaml:"step"`
	Duration         string         `json:"duration" yaml:"duration"`
	Chunk            string         `json:"chunk" yaml:"chunk"`
	Workers          Workers        `json:"workers" yaml:"workers"`
	WorkItem         WorkItem       `json:"workItem" yaml:"workItem"`
	Exporters        DatasetExports `json:"exporters" yaml:"exporters"`
	ClusterName      string         `json:"clusterName" yaml:"clusterName"`
}

type TemplateRef struct {
	Name     string `json:"name" yaml:"name"`
	SpecHash string `json:"specHash" yaml:"specHash"`
}

type DatasetExports struct {
	Kruize KruizeExport `json:"kruize" yaml:"kruize"`
}

type KruizeExport struct {
	Enabled            bool   `json:"enabled" yaml:"enabled"`
	ClusterName        string `json:"clusterName" yaml:"clusterName"`
	PerformanceProfile string `json:"performanceProfile" yaml:"performanceProfile"`
}

type Export struct {
	Type           string            `json:"type" yaml:"type"`
	Dataset        string            `json:"dataset" yaml:"dataset"`
	StartTime      string            `json:"startTime" yaml:"startTime"`
	EndTime        string            `json:"endTime" yaml:"endTime"`
	BlockDuration  string            `json:"blockDuration" yaml:"blockDuration"`
	ExternalLabels map[string]string `json:"externalLabels" yaml:"externalLabels"`
	ScratchMode    string            `json:"scratchMode" yaml:"scratchMode"`
	DropLabels     []string          `json:"dropLabels" yaml:"dropLabels"`
}

// ParseDuration accepts Go durations plus Nd for days.
func ParseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, nil
	}
	if len(s) > 1 && s[len(s)-1] == 'd' {
		var n int
		for _, c := range s[:len(s)-1] {
			if c < '0' || c > '9' {
				return 0, errDuration(s)
			}
			n = n*10 + int(c-'0')
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, errDuration(s)
	}
	return d, nil
}

type durationError string

func (e durationError) Error() string { return string(e) }

func errDuration(s string) error { return durationError("bad duration " + s) }

func DefaultTemplate() Template {
	return Template{
		Seed: 424242,
		Topology: Topology{
			Mode: "NamespaceMatrix",
			Namespaces: NamespaceSpec{
				Count:        3,
				Distribution: "Uniform",
			},
			WorkloadsPerNamespace: 5,
			Kinds:                 map[string]float64{"Deployment": 0.8, "StatefulSet": 0.2},
			Replicas: Replicas{
				Buckets: []Bucket{
					{Range: [2]int{1, 3}, Share: 70},
					{Range: [2]int{4, 7}, Share: 22},
					{Range: [2]int{8, 10}, Share: 8},
				},
				StatefulSetBuckets: []Bucket{
					{Range: [2]int{1, 3}, Share: 85},
					{Range: [2]int{4, 7}, Share: 15},
				},
			},
			ContainersPerPod: map[string]float64{"1": 0.75, "2": 0.18, "3": 0.07},
		},
		Profiles: Profiles{
			CPU:        map[string]float64{"idle": 0.15, "low": 0.40, "medium": 0.30, "high": 0.15},
			Memory:     map[string]float64{"low": 0.35, "medium": 0.45, "high": 0.20},
			Sizing:     map[string]float64{"rightSized": 0.45, "over": 0.30, "under": 0.10, "noLimits": 0.10, "bestEffort": 0.05},
			Behaviours: map[string]float64{"steady": 0.30, "diurnal": 0.40, "bursty": 0.15, "batchOnOff": 0.10, "idleWindows": 0.05},
			Anomalies:  map[string]float64{"throttleAggressive": 0.08, "memoryLeak": 0.03, "oomLoop": 0.01, "spikes": 0.10},
		},
		GPU: GPU{Enabled: true, WorkloadRatio: 0.05, LabelProfile: "kruize", NodePools: []string{"p4d.24xlarge"}, MigRatio: 0.3},
		Nodes: Nodes{
			Pools: []NodePool{
				{InstanceType: "m7i.4xlarge", Weight: 0.5},
				{InstanceType: "c7i.4xlarge", Weight: 0.2},
				{InstanceType: "r7i.4xlarge", Weight: 0.3},
			},
			MaxPodsPerNode: 250,
		},
		Time:     Time{Step: "15s", Duration: "15d", Chunk: "6h"},
		Features: []string{"cpu", "memory", "gpu", "ksm"},
		WorkloadLabels: WorkloadLabels{
			Scope: "namespace",
			Keys: []LabelKey{
				{Key: "org_id", Values: []LabelValue{{Value: "acme-retail", Weight: 3}, {Value: "globex", Weight: 1}}},
				{Key: "cluster_id", Values: []LabelValue{{Value: "ocp-east-1", Weight: 2}, {Value: "ocp-west-2", Weight: 1}}},
			},
			CarryOn: []string{"namespace", "ksm-pod", "ksm-container", "recorded", "cadvisor"},
		},
		Workers:  Workers{Count: 2, CPU: "1", Memory: "2Gi", Threads: 2},
		WorkItem: WorkItem{WorkloadsPerItem: 1024},
	}
}
