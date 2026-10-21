package app

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bharathappali/kubegenx/internal/export"
	"github.com/bharathappali/kubegenx/internal/planner"
	"github.com/bharathappali/kubegenx/internal/spec"
	"github.com/bharathappali/kubegenx/internal/store"
	"github.com/bharathappali/kubegenx/internal/verify"
	"gopkg.in/yaml.v3"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
)

type App struct {
	NS          string
	Data        string
	Scratch     string
	Store       *store.Store
	Kube        kubernetes.Interface
	Dyn         dynamic.Interface
	WorkerImage string
	ThanosImage string
	Hub         *Hub

	mu         sync.Mutex
	statusMu   sync.Mutex
	exporting  map[string]bool
	lastStatus map[string]time.Time
}

func (a *App) Loop(ctx context.Context) {
	if a.exporting == nil {
		a.exporting = map[string]bool{}
	}
	if a.lastStatus == nil {
		a.lastStatus = map[string]time.Time{}
	}
	factory := informers.NewSharedInformerFactoryWithOptions(a.Kube, 15*time.Second, informers.WithNamespace(a.NS))
	inf := factory.Core().V1().ConfigMaps().Informer()
	inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { a.onCM(obj) },
		UpdateFunc: func(_, obj any) { a.onCM(obj) },
	})
	factory.Start(ctx.Done())
	cache.WaitForCacheSync(ctx.Done(), inf.HasSynced)
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				a.poll()
			}
		}
	}()
	<-ctx.Done()
}

func (a *App) onCM(obj any) {
	cm, ok := obj.(*corev1.ConfigMap)
	if !ok {
		return
	}
	if cm.Labels["app.kubernetes.io/part-of"] != "kubegenx" && cm.Labels["kubegenx.io/kind"] == "" {
		return
	}
	a.reconcile(cm)
}

func (a *App) poll() {
	c, cancel := ctx()
	defer cancel()
	list, err := a.Kube.CoreV1().ConfigMaps(a.NS).List(c, metav1.ListOptions{LabelSelector: "kubegenx.io/kind in (dataset,export)"})
	if err != nil {
		slogErr("list configmaps", err)
		return
	}
	for i := range list.Items {
		a.reconcile(&list.Items[i])
	}
}

func (a *App) reconcile(cm *corev1.ConfigMap) {
	a.mu.Lock()
	defer a.mu.Unlock()
	switch cm.Labels["kubegenx.io/kind"] {
	case "dataset":
		a.reconcileDataset(cm)
	case "export":
		a.reconcileExport(cm)
	}
}

func (a *App) reconcileDataset(cm *corev1.ConfigMap) {
	name := cm.Labels["kubegenx.io/name"]
	if name == "" {
		return
	}
	if cm.DeletionTimestamp != nil {
		a.cleanupDataset(cm, name)
		return
	}
	var ds spec.Dataset
	if err := yaml.Unmarshal([]byte(cm.Data["spec.yaml"]), &ds); err != nil {
		slogErr("dataset spec", err)
		return
	}
	phase, _ := a.Store.Phase(name)
	if phase == "" {
		if err := a.planDataset(cm, name, ds); err != nil {
			slogErr("plan", err)
			_ = a.Store.UpsertDataset(name, "Failed", "", 0, 0)
			a.writeStatus(cm, name, map[string]any{"phase": "Failed", "error": err.Error()})
		}
		return
	}
	if phase == "Generating" {
		p, err := a.Store.Progress(name)
		if err != nil {
			return
		}
		if p.Total > 0 && p.Done+p.Failed == p.Total && p.Leased == 0 {
			if p.Failed > 0 {
				_ = a.Store.SetPhase(name, "Failed")
				a.writeStatus(cm, name, statusFrom(p, "Failed", "workers finished with failures"))
				return
			}
			if _, err := verify.Dataset(a.Data, name); err != nil {
				_ = a.Store.SetPhase(name, "Failed")
				a.writeStatus(cm, name, statusFrom(p, "Failed", err.Error()))
				return
			}
			a.deleteJob(name)
			_ = a.Store.SetPhase(name, "Ready")
			st := statusFrom(p, "Ready", "")
			st["workers"] = "released"
			a.writeStatus(cm, name, st)
			a.publishProgress(name, "Ready")
			return
		}
		a.writeStatusThrottled(cm, name, statusFrom(p, "Generating", ""))
	}
}

func (a *App) planDataset(cm *corev1.ConfigMap, name string, ds spec.Dataset) error {
	tpl := ds.TemplateSnapshot
	if tpl.Time.Step == "" {
		tpl = spec.DefaultTemplate()
	}
	epoch := ds.Epoch
	if epoch == "" {
		epoch = "2026-10-05T00:00:00Z"
	}
	plan, err := planner.Build(name, tpl, epoch)
	if err != nil {
		return err
	}
	if ds.Exporters.Kruize.ClusterName != "" {
		plan.ClusterName = ds.Exporters.Kruize.ClusterName
	}
	est := int64(len(plan.Containers)) * int64(plan.Steps) * 16
	if est < 1 {
		est = 1
	}
	reserved := est + est/3
	sum, err := a.Store.ReservedSum()
	if err != nil {
		return err
	}
	if sum+reserved > store.CapacityBytes {
		return fmt.Errorf("InsufficientCapacity")
	}
	dir := filepath.Join(a.Data, "datasets", name, "plan")
	if err := os.MkdirAll(dir, 0o775); err != nil {
		return err
	}
	raw, err := json.MarshalIndent(plan, "", "  ")
	if err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(dir, "generation.json"), raw, 0o664); err != nil {
		return err
	}
	man, _ := json.MarshalIndent(map[string]any{
		"dataset": name, "digest": plan.Digest, "items": len(plan.Items),
		"workloads": len(plan.Workloads), "containers": len(plan.Containers), "chunks": plan.Chunks,
	}, "", "  ")
	_ = os.WriteFile(filepath.Join(dir, "manifest.json"), man, 0o664)
	if err := a.Store.UpsertDataset(name, "Generating", plan.Digest, est, reserved); err != nil {
		return err
	}
	var items []store.Item
	for _, it := range plan.Items {
		items = append(items, store.Item{ID: it.ID, WlLo: it.WlLo, WlHi: it.WlHi, Chunk: it.Chunk, ItemIndex: it.Index})
	}
	if err := a.Store.InsertItems(name, items); err != nil {
		return err
	}
	workers := ds.Workers
	if workers.Count == 0 {
		workers = tpl.Workers
	}
	if workers.Count == 0 {
		workers.Count = 2
	}
	if err := a.ensureJob(cm, name, workers); err != nil {
		return err
	}
	p, _ := a.Store.Progress(name)
	a.writeStatus(cm, name, statusFrom(p, "Generating", ""))
	return nil
}

func statusFrom(p store.Progress, phase, errMsg string) map[string]any {
	pct := 0.0
	if p.Total > 0 {
		pct = float64(p.Done) / float64(p.Total) * 100
	}
	m := map[string]any{
		"phase":   phase,
		"items":   map[string]int{"total": p.Total, "done": p.Done, "failed": p.Failed, "leased": p.Leased},
		"percent": pct, "bytes": p.Bytes, "volume": "kubegenx-data",
	}
	if errMsg != "" {
		m["error"] = errMsg
	}
	return m
}

func (a *App) publish(dataset string) {
	a.publishProgress(dataset, "")
}

func (a *App) publishProgress(dataset, phase string) {
	if a.Hub == nil {
		return
	}
	p, err := a.Store.Progress(dataset)
	if err != nil {
		return
	}
	if phase == "" {
		phase, _ = a.Store.Phase(dataset)
	}
	pct := 0.0
	if p.Total > 0 {
		pct = float64(p.Done) / float64(p.Total) * 100
	}
	a.Hub.Broadcast("dataset:"+dataset, map[string]any{
		"v": 1, "type": "progress", "topic": "dataset:" + dataset,
		"phase": phase,
		"kpi":   map[string]any{"itemsDone": p.Done, "itemsTotal": p.Total, "bytes": p.Bytes, "percent": pct},
	})
}

func (a *App) writeStatus(parent *corev1.ConfigMap, name string, doc map[string]any) {
	a.statusMu.Lock()
	defer a.statusMu.Unlock()
	if a.lastStatus == nil {
		a.lastStatus = map[string]time.Time{}
	}
	raw, _ := json.Marshal(doc)
	cmName := parent.Name + "-status"
	c, cancel := ctx()
	defer cancel()
	existing, err := a.Kube.CoreV1().ConfigMaps(a.NS).Get(c, cmName, metav1.GetOptions{})
	if errors.IsNotFound(err) {
		cm := &corev1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{
				Name: cmName, Namespace: a.NS,
				Labels: map[string]string{
					"app.kubernetes.io/part-of": "kubegenx", "app.kubernetes.io/managed-by": "kubegenx",
					"kubegenx.io/kind": "status", "kubegenx.io/name": name, "kubegenx.io/dataset": name,
				},
				OwnerReferences: []metav1.OwnerReference{ownerRef(parent)},
			},
			Data: map[string]string{"status.json": string(raw)},
		}
		_, err = a.Kube.CoreV1().ConfigMaps(a.NS).Create(c, cm, metav1.CreateOptions{})
		slogErr("create status", err)
		return
	}
	if err != nil {
		return
	}
	if existing.Data == nil {
		existing.Data = map[string]string{}
	}
	existing.Data["status.json"] = string(raw)
	_, err = a.Kube.CoreV1().ConfigMaps(a.NS).Update(c, existing, metav1.UpdateOptions{})
	slogErr("update status", err)
	a.lastStatus[name] = time.Now()
}

func (a *App) writeStatusThrottled(parent *corev1.ConfigMap, name string, doc map[string]any) {
	a.statusMu.Lock()
	recent := time.Since(a.lastStatus[name]) < 5*time.Second
	a.statusMu.Unlock()
	if recent {
		return
	}
	a.writeStatus(parent, name, doc)
}

func (a *App) cleanupDataset(cm *corev1.ConfigMap, name string) {
	a.deleteJob(name)
	c, cancel := ctx()
	defer cancel()
	list, err := a.Kube.CoreV1().ConfigMaps(a.NS).List(c, metav1.ListOptions{LabelSelector: "kubegenx.io/kind=export,kubegenx.io/dataset=" + name})
	if err == nil {
		for i := range list.Items {
			_ = a.Kube.CoreV1().ConfigMaps(a.NS).Delete(c, list.Items[i].Name, metav1.DeleteOptions{})
		}
	}
	_ = os.RemoveAll(filepath.Join(a.Data, "datasets", name))
	_ = os.RemoveAll(filepath.Join(a.Data, "exports", name))
	_ = a.Store.DeleteDataset(name)
	a.stripFinalizer(cm)
}

func (a *App) stripFinalizer(cm *corev1.ConfigMap) {
	var next []string
	for _, f := range cm.Finalizers {
		if f != "kubegenx.io/cleanup" {
			next = append(next, f)
		}
	}
	if len(next) == len(cm.Finalizers) {
		return
	}
	c, cancel := ctx()
	defer cancel()
	fresh, err := a.Kube.CoreV1().ConfigMaps(a.NS).Get(c, cm.Name, metav1.GetOptions{})
	if err != nil {
		return
	}
	fresh.Finalizers = next
	_, err = a.Kube.CoreV1().ConfigMaps(a.NS).Update(c, fresh, metav1.UpdateOptions{})
	slogErr("strip finalizer", err)
}

func (a *App) reconcileExport(cm *corev1.ConfigMap) {
	name := cm.Labels["kubegenx.io/name"]
	if name == "" {
		return
	}
	if cm.DeletionTimestamp != nil {
		a.cleanupExport(cm, name)
		return
	}
	row, err := a.Store.GetExport(name)
	if err != nil || row.Phase == "" || row.Phase == "Failed" && false {
		// start once
	}
	phase := row.Phase
	if phase == "Serving" || phase == "Building" || phase == "Failed" {
		return
	}
	for _, busy := range a.exporting {
		if busy {
			return
		}
	}
	if a.exporting[name] {
		return
	}
	a.exporting[name] = true
	go a.buildExport(cm.Name, name)
}

func (a *App) buildExport(cmName, name string) {
	defer func() {
		a.mu.Lock()
		delete(a.exporting, name)
		a.mu.Unlock()
	}()
	c, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
	defer cancel()
	cm, err := a.Kube.CoreV1().ConfigMaps(a.NS).Get(c, cmName, metav1.GetOptions{})
	if err != nil {
		slogErr("get export", err)
		return
	}
	var ex spec.Export
	if err := yaml.Unmarshal([]byte(cm.Data["spec.yaml"]), &ex); err != nil {
		slogErr("export spec", err)
		return
	}
	dsName := ex.Dataset
	if dsName == "" {
		dsName = cm.Labels["kubegenx.io/dataset"]
	}
	raw, err := os.ReadFile(filepath.Join(a.Data, "datasets", dsName, "plan", "generation.json"))
	if err != nil {
		_ = a.Store.UpsertExport(store.ExportRow{Name: name, Dataset: dsName, Phase: "Failed"})
		a.writeStatus(cm, name, map[string]any{"phase": "Failed", "error": err.Error()})
		return
	}
	var plan planner.Plan
	if err := json.Unmarshal(raw, &plan); err != nil {
		return
	}
	start, err := time.Parse(time.RFC3339, ex.StartTime)
	if err != nil {
		start = time.Now().UTC().Truncate(15 * time.Minute)
	}
	_ = a.Store.UpsertExport(store.ExportRow{Name: name, Dataset: dsName, Phase: "Building", StartTime: start.UTC().Format(time.RFC3339)})
	a.writeStatus(cm, name, map[string]any{"phase": "Building", "startTime": start.UTC().Format(time.RFC3339)})
	res, err := export.Build(c, &plan, filepath.Join(a.Data, "datasets", dsName), a.Scratch, name, start, ex.ExternalLabels)
	if err != nil {
		slogErr("export build", err)
		_ = a.Store.UpsertExport(store.ExportRow{Name: name, Dataset: dsName, Phase: "Failed", StartTime: start.UTC().Format(time.RFC3339)})
		a.writeStatus(cm, name, map[string]any{"phase": "Failed", "error": err.Error()})
		return
	}
	url, err := a.ensureThanos(cm, name, dsName)
	if err != nil {
		slogErr("thanos", err)
		_ = a.Store.UpsertExport(store.ExportRow{Name: name, Dataset: dsName, Phase: "Failed", Blocks: res.Blocks, Bytes: res.Bytes})
		a.writeStatus(cm, name, map[string]any{"phase": "Failed", "error": err.Error(), "blocks": res.Blocks})
		return
	}
	_ = a.Store.UpsertExport(store.ExportRow{Name: name, Dataset: dsName, Phase: "Serving", StartTime: start.UTC().Format(time.RFC3339), Blocks: res.Blocks, Bytes: res.Bytes, URL: url})
	a.writeStatus(cm, name, map[string]any{"phase": "Serving", "url": url, "blocks": res.Blocks, "bytes": res.Bytes, "startTime": start.UTC().Format(time.RFC3339)})
}

func (a *App) cleanupExport(cm *corev1.ConfigMap, name string) {
	ds := cm.Labels["kubegenx.io/dataset"]
	a.deleteThanos(name)
	if ds != "" {
		_ = os.RemoveAll(filepath.Join(a.Data, "exports", ds, name))
	}
	_ = a.Store.DeleteExport(name)
	a.stripFinalizer(cm)
}

func (a *App) ensureJob(parent *corev1.ConfigMap, dataset string, w spec.Workers) error {
	name := "kgx-ds-" + dataset + "-workers"
	c, cancel := ctx()
	defer cancel()
	if _, err := a.Kube.BatchV1().Jobs(a.NS).Get(c, name, metav1.GetOptions{}); err == nil {
		return nil
	} else if !errors.IsNotFound(err) {
		return err
	}
	n := int32(w.Count)
	if n < 1 {
		n = 1
	}
	cpu := w.CPU
	if cpu == "" {
		cpu = "500m"
	}
	mem := w.Memory
	if mem == "" {
		mem = "1Gi"
	}
	ttl := int32(3600)
	backoff := int32(3)
	auto := false
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: a.NS,
			Labels:          partLabels(map[string]string{"kubegenx.io/dataset": dataset}),
			OwnerReferences: []metav1.OwnerReference{ownerRef(parent)},
		},
		Spec: batchv1.JobSpec{
			Parallelism:             &n,
			Completions:             &n,
			TTLSecondsAfterFinished: &ttl,
			BackoffLimit:            &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: partLabels(map[string]string{"kubegenx.io/dataset": dataset})},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyOnFailure,
					ServiceAccountName:           "kubegenx-worker",
					AutomountServiceAccountToken: &auto,
					SecurityContext:              podSC(),
					Containers: []corev1.Container{{
						Name:            "worker",
						Image:           a.WorkerImage,
						ImagePullPolicy: corev1.PullAlways,
						Env: []corev1.EnvVar{
							{Name: "KUBEGENX_DATASET", Value: dataset},
							{Name: "KUBEGENX_CONTROLLER", Value: "kubegenx-controller." + a.NS + ".svc:9091"},
							{Name: "KUBEGENX_DATA", Value: "/data"},
						},
						Resources:       res(cpu, mem),
						SecurityContext: containerSC(),
						VolumeMounts:    []corev1.VolumeMount{{Name: "data", MountPath: "/data"}},
					}},
					Volumes: []corev1.Volume{{
						Name:         "data",
						VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "kubegenx-data"}},
					}},
				},
			},
		},
	}
	_, err := a.Kube.BatchV1().Jobs(a.NS).Create(c, job, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (a *App) deleteJob(dataset string) {
	c, cancel := ctx()
	defer cancel()
	prop := metav1.DeletePropagationBackground
	err := a.Kube.BatchV1().Jobs(a.NS).Delete(c, "kgx-ds-"+dataset+"-workers", metav1.DeleteOptions{PropagationPolicy: &prop})
	if err != nil && !errors.IsNotFound(err) {
		slogErr("delete job", err)
	}
}

func (a *App) ensureThanos(parent *corev1.ConfigMap, exportName, dataset string) (string, error) {
	c, cancel := ctx()
	defer cancel()
	objName := "kgx-ex-" + exportName
	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{Name: objName + "-objstore", Namespace: a.NS, Labels: partLabels(nil), OwnerReferences: []metav1.OwnerReference{ownerRef(parent)}},
		StringData: map[string]string{"objstore.yml": fmt.Sprintf("type: FILESYSTEM\nconfig:\n  directory: /data/exports/%s/%s/tsdb\n", dataset, exportName)},
	}
	if _, err := a.Kube.CoreV1().Secrets(a.NS).Create(c, secret, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
		return "", err
	}
	replicas := int32(1)
	auto := false
	dep := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{Name: objName, Namespace: a.NS, Labels: partLabels(map[string]string{"kubegenx.io/export": exportName}), OwnerReferences: []metav1.OwnerReference{ownerRef(parent)}},
		Spec: appsv1.DeploymentSpec{
			Replicas: &replicas,
			Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": objName}},
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: partLabels(map[string]string{"app": objName, "kubegenx.io/export": exportName})},
				Spec: corev1.PodSpec{
					ServiceAccountName:           "kubegenx-thanos",
					AutomountServiceAccountToken: &auto,
					SecurityContext:              podSC(),
					Containers: []corev1.Container{
						{
							Name: "store", Image: a.ThanosImage, ImagePullPolicy: corev1.PullIfNotPresent,
							Args: []string{"store", "--objstore.config-file=/etc/thanos/objstore.yml", "--data-dir=/cache",
								"--grpc-address=0.0.0.0:10901", "--http-address=0.0.0.0:10902",
								"--store.enable-index-header-lazy-reader", "--sync-block-duration=30s"},
							SecurityContext: containerSC(),
							Resources:       res("500m", "1Gi"),
							VolumeMounts: []corev1.VolumeMount{
								{Name: "data", MountPath: "/data", ReadOnly: true},
								{Name: "objstore", MountPath: "/etc/thanos", ReadOnly: true},
								{Name: "cache", MountPath: "/cache"},
							},
						},
						{
							Name: "query", Image: a.ThanosImage, ImagePullPolicy: corev1.PullIfNotPresent,
							Args:            []string{"query", "--endpoint=127.0.0.1:10901", "--http-address=0.0.0.0:9090", "--grpc-address=127.0.0.1:10911"},
							Ports:           []corev1.ContainerPort{{Name: "http", ContainerPort: 9090}},
							SecurityContext: containerSC(),
							Resources:       res("200m", "512Mi"),
						},
					},
					Volumes: []corev1.Volume{
						{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "kubegenx-data"}}},
						{Name: "objstore", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: objName + "-objstore"}}},
						{Name: "cache", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}
	if _, err := a.Kube.AppsV1().Deployments(a.NS).Create(c, dep, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
		return "", err
	}
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{Name: objName + "-query", Namespace: a.NS, Labels: partLabels(nil), OwnerReferences: []metav1.OwnerReference{ownerRef(parent)}},
		Spec: corev1.ServiceSpec{
			Selector: map[string]string{"app": objName},
			Ports:    []corev1.ServicePort{{Name: "http", Port: 9090, TargetPort: intstr.FromInt32(9090)}},
		},
	}
	if _, err := a.Kube.CoreV1().Services(a.NS).Create(c, svc, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
		return "", err
	}
	routeName := objName + "-query"
	route := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "route.openshift.io/v1",
		"kind":       "Route",
		"metadata": map[string]any{
			"name": routeName, "namespace": a.NS,
			"labels": map[string]any{"app.kubernetes.io/part-of": "kubegenx", "app.kubernetes.io/managed-by": "kubegenx"},
		},
		"spec": map[string]any{
			"to":   map[string]any{"kind": "Service", "name": objName + "-query"},
			"port": map[string]any{"targetPort": "http"},
			"tls":  map[string]any{"termination": "edge", "insecureEdgeTerminationPolicy": "Redirect"},
		},
	}}
	gvr := schema.GroupVersionResource{Group: "route.openshift.io", Version: "v1", Resource: "routes"}
	if _, err := a.Dyn.Resource(gvr).Namespace(a.NS).Create(c, route, metav1.CreateOptions{}); err != nil && !errors.IsAlreadyExists(err) {
		return "", err
	}
	var host string
	for i := 0; i < 20; i++ {
		got, err := a.Dyn.Resource(gvr).Namespace(a.NS).Get(c, routeName, metav1.GetOptions{})
		if err == nil {
			host, _, _ = unstructured.NestedString(got.Object, "spec", "host")
			if host != "" {
				break
			}
		}
		time.Sleep(time.Second)
	}
	if host == "" {
		return "", fmt.Errorf("route host not assigned")
	}
	return "https://" + host, nil
}

func (a *App) deleteThanos(exportName string) {
	c, cancel := ctx()
	defer cancel()
	objName := "kgx-ex-" + exportName
	prop := metav1.DeletePropagationBackground
	_ = a.Kube.AppsV1().Deployments(a.NS).Delete(c, objName, metav1.DeleteOptions{PropagationPolicy: &prop})
	_ = a.Kube.CoreV1().Services(a.NS).Delete(c, objName+"-query", metav1.DeleteOptions{})
	_ = a.Kube.CoreV1().Secrets(a.NS).Delete(c, objName+"-objstore", metav1.DeleteOptions{})
	gvr := schema.GroupVersionResource{Group: "route.openshift.io", Version: "v1", Resource: "routes"}
	_ = a.Dyn.Resource(gvr).Namespace(a.NS).Delete(c, objName+"-query", metav1.DeleteOptions{})
}

func partLabels(extra map[string]string) map[string]string {
	m := map[string]string{"app.kubernetes.io/part-of": "kubegenx", "app.kubernetes.io/managed-by": "kubegenx"}
	for k, v := range extra {
		m[k] = v
	}
	return m
}

func ownerRef(cm *corev1.ConfigMap) metav1.OwnerReference {
	yes := true
	return metav1.OwnerReference{APIVersion: "v1", Kind: "ConfigMap", Name: cm.Name, UID: cm.UID, Controller: &yes}
}

func podSC() *corev1.PodSecurityContext {
	sc := &corev1.PodSecurityContext{RunAsNonRoot: boolPtr(true), SeccompProfile: &corev1.SeccompProfile{Type: corev1.SeccompProfileTypeRuntimeDefault}}
	if uid, err := strconv.ParseInt(os.Getenv("KUBEGENX_POD_UID"), 10, 64); err == nil && uid > 0 {
		sc.RunAsUser = &uid
		sc.FSGroup = &uid
	}
	return sc
}

func containerSC() *corev1.SecurityContext {
	return &corev1.SecurityContext{
		RunAsNonRoot: boolPtr(true), AllowPrivilegeEscalation: boolPtr(false),
		Capabilities: &corev1.Capabilities{Drop: []corev1.Capability{"ALL"}},
	}
}

func res(cpu, mem string) corev1.ResourceRequirements {
	return corev1.ResourceRequirements{Requests: corev1.ResourceList{
		corev1.ResourceCPU: resource.MustParse(cpu), corev1.ResourceMemory: resource.MustParse(mem),
	}}
}

func loadPlan(data, dataset string) (*planner.Plan, error) {
	raw, err := os.ReadFile(filepath.Join(data, "datasets", dataset, "plan", "generation.json"))
	if err != nil {
		return nil, err
	}
	var p planner.Plan
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	return &p, nil
}

func cmName(kind, name string) string {
	switch kind {
	case "template":
		return "kgx-tpl-" + name
	case "dataset":
		return "kgx-ds-" + name
	case "export":
		return "kgx-ex-" + name
	default:
		return "kgx-" + name
	}
}

func dnsOK(s string) bool {
	if s == "" || len(s) > 40 {
		return false
	}
	for i, c := range s {
		ok := (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') || c == '-'
		if !ok || ((i == 0 || i == len(s)-1) && c == '-') {
			return false
		}
	}
	return !strings.Contains(s, "--")
}
