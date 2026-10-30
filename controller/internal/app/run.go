package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bharathappali/kubegenx/internal/aggregate"
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
	NS             string
	Data           string
	Scratch        string
	Store          *store.Store
	Kube           kubernetes.Interface
	Dyn            dynamic.Interface
	WorkerImage    string
	ConverterImage string
	ThanosImage    string
	Hub            *Hub

	mu           sync.Mutex
	statusMu     sync.Mutex
	exporting    map[string]bool
	activeExport string
	lastStatus   map[string]time.Time
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
	a.expireGeneric()
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

func (a *App) expireGeneric() {
	rows, err := a.Store.ListExports()
	if err != nil {
		return
	}
	cutoff := time.Now().Add(-24 * time.Hour).Unix()
	c, cancel := ctx()
	defer cancel()
	for _, row := range rows {
		if row.Type != "generic" || row.CreatedAt == 0 || row.CreatedAt > cutoff {
			continue
		}
		err := a.Kube.CoreV1().ConfigMaps(a.NS).Delete(c, cmName("export", row.Name), metav1.DeleteOptions{})
		if err != nil && !errors.IsNotFound(err) {
			slogErr("expire session", err)
		}
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
	if phase == "" || phase == "Queued" {
		if a.exportsPending() {
			if phase != "Queued" {
				_ = a.Store.UpsertDataset(name, "Queued", "", 0, 0)
			}
			a.writeStatus(cm, name, map[string]any{"phase": "Queued"})
			return
		}
		if err := a.planDataset(cm, name, ds); err != nil {
			slogErr("plan", err)
			_ = a.Store.UpsertDataset(name, "Failed", "", 0, 0)
			a.writeStatus(cm, name, map[string]any{"phase": "Failed", "error": err.Error()})
		}
		return
	}
	if phase == "Generating" {
		_ = a.Store.MarkGenerationStart(name)
		p, err := a.Store.Progress(name)
		if err != nil {
			return
		}
		if p.Total > 0 && p.Done+p.Failed == p.Total && p.Leased == 0 {
			if p.Failed > 0 {
				_ = a.Store.SetPhase(name, "Failed")
				_ = a.Store.MarkGenerationFinish(name)
				a.writeStatus(cm, name, statusFrom(p, "Failed", "workers finished with failures"))
				return
			}
			if _, err := verify.Dataset(a.Data, name); err != nil {
				_ = a.Store.SetPhase(name, "Failed")
				_ = a.Store.MarkGenerationFinish(name)
				a.writeStatus(cm, name, statusFrom(p, "Failed", err.Error()))
				return
			}
			a.deleteJob(name)
			_ = a.Store.SetPhase(name, "Ready")
			_ = a.Store.MarkGenerationFinish(name)
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
		epoch = defaultEpoch(time.Now(), tpl)
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
	seconds := plan.Steps * plan.StepSeconds
	entries := plan.Steps * (len(plan.Containers)*4 + len(plan.GPUs)*3)
	man, _ := json.MarshalIndent(map[string]any{
		"dataset": name, "digest": plan.Digest, "items": len(plan.Items),
		"workloads": len(plan.Workloads), "containers": len(plan.Containers), "chunks": plan.Chunks,
		"namespaces": len(plan.Namespaces), "gpus": len(plan.GPUs),
		"samples": plan.Steps, "entries": entries,
		"days": float64(seconds) / 86400, "stepSeconds": plan.StepSeconds,
		"chunkSeconds": plan.ChunkSamples * plan.StepSeconds, "epoch": plan.Epoch,
		"namespaceRanges": namespaceRanges(plan),
	}, "", "  ")
	_ = os.WriteFile(filepath.Join(dir, "manifest.json"), man, 0o664)
	if rawWL, err := json.Marshal(workloadBriefs(plan)); err == nil {
		_ = os.WriteFile(filepath.Join(dir, "workloads.json"), rawWL, 0o664)
	}
	if err := writeCatalog(dir, plan); err != nil {
		return err
	}
	if err := a.Store.UpsertDataset(name, "Generating", plan.Digest, est, reserved); err != nil {
		return err
	}
	_ = a.Store.MarkGenerationStart(name)
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

func namespaceRanges(plan *planner.Plan) []map[string]any {
	type span struct{ lo, hi int }
	got := map[int]*span{}
	for _, w := range plan.Workloads {
		s := got[w.NamespaceID]
		if s == nil {
			s = &span{lo: w.ID, hi: w.ID + 1}
			got[w.NamespaceID] = s
			continue
		}
		if w.ID < s.lo {
			s.lo = w.ID
		}
		if w.ID+1 > s.hi {
			s.hi = w.ID + 1
		}
	}
	out := make([]map[string]any, 0, len(plan.Namespaces))
	for _, ns := range plan.Namespaces {
		s := got[ns.ID]
		lo, hi := 0, 0
		if s != nil {
			lo, hi = s.lo, s.hi
		}
		out = append(out, map[string]any{"id": ns.ID, "name": ns.Name, "wlLo": lo, "wlHi": hi, "count": hi - lo})
	}
	return out
}

func workloadBriefs(plan *planner.Plan) []map[string]any {
	ns := map[int]string{}
	for _, n := range plan.Namespaces {
		ns[n.ID] = n.Name
	}
	out := make([]map[string]any, 0, len(plan.Workloads))
	for _, w := range plan.Workloads {
		out = append(out, map[string]any{
			"id": w.ID, "namespaceId": w.NamespaceID, "namespace": ns[w.NamespaceID],
			"name": w.Name, "kind": w.Kind, "replicas": w.Replicas, "gpu": w.GPU,
		})
	}
	return out
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

func (a *App) progressDoc(dataset, phase string) map[string]any {
	p, _ := a.Store.Progress(dataset)
	if phase == "" {
		phase, _ = a.Store.Phase(dataset)
	}
	doc := statusFrom(p, phase, "")
	doc["name"] = dataset
	if started, finished, err := a.Store.GenerationTimes(dataset); err == nil {
		if started > 0 {
			doc["startedAt"] = started
		}
		if finished > 0 {
			doc["finishedAt"] = finished
		}
	}
	cells, err := a.Store.Cells(dataset)
	if err != nil || cells == nil {
		cells = []store.Cell{}
	}
	doc["cells"] = cells
	pct := 0.0
	if p.Total > 0 {
		pct = float64(p.Done) / float64(p.Total) * 100
	}
	doc["type"] = "progress"
	doc["v"] = 1
	doc["topic"] = "dataset:" + dataset
	doc["kpi"] = map[string]any{"itemsDone": p.Done, "itemsTotal": p.Total, "bytes": p.Bytes, "percent": pct, "itemsLeased": p.Leased, "itemsFailed": p.Failed}
	return doc
}

func (a *App) publishProgress(dataset, phase string) {
	if a.Hub == nil {
		return
	}
	a.Hub.Broadcast("dataset:"+dataset, a.progressDoc(dataset, phase))
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
	if err != nil {
		return
	}
	if row.Phase != "Queued" && row.Phase != "Building" {
		return
	}
	a.kickExportLocked()
}

// exportsPending is true while any parquet conversion is queued or running.
// New datasets stay queued until that work finishes.
func (a *App) exportsPending() bool {
	rows, err := a.Store.ListExports()
	if err != nil {
		return true
	}
	for _, row := range rows {
		if row.Phase == "Queued" || row.Phase == "Building" {
			return true
		}
	}
	return false
}

// kickExportLocked starts the single parquet conversion. A Building export
// resumes first. Every other dataset and export stays queued until it returns.
// Caller holds a.mu.
func (a *App) kickExportLocked() {
	if a.activeExport != "" {
		return
	}
	if a.datasetsBusy() {
		return
	}
	rows, err := a.Store.ListExports()
	if err != nil {
		slogErr("list exports", err)
		return
	}
	var pick *store.ExportRow
	for i := range rows {
		row := rows[i]
		if row.Phase != "Queued" && row.Phase != "Building" {
			continue
		}
		if a.exporting != nil && a.exporting[row.Name] {
			continue
		}
		if pick == nil || aheadOfExport(row, *pick) {
			cp := row
			pick = &cp
		}
	}
	if pick == nil {
		return
	}
	if a.exporting == nil {
		a.exporting = map[string]bool{}
	}
	a.exporting[pick.Name] = true
	a.activeExport = pick.Name
	slog.Info("export start", "name", pick.Name, "phase", pick.Phase)
	go a.buildExport(cmName("export", pick.Name), pick.Name)
}

func aheadOfExport(cand, cur store.ExportRow) bool {
	if cand.Phase == "Building" && cur.Phase != "Building" {
		return true
	}
	if cur.Phase == "Building" && cand.Phase != "Building" {
		return false
	}
	if cand.CreatedAt != cur.CreatedAt {
		return cand.CreatedAt < cur.CreatedAt
	}
	return cand.Name < cur.Name
}

func (a *App) buildExport(cmName, name string) {
	defer func() {
		a.mu.Lock()
		delete(a.exporting, name)
		if a.activeExport == name {
			a.activeExport = ""
		}
		a.kickExportLocked()
		a.mu.Unlock()
	}()
	c, cancel := context.WithTimeout(context.Background(), 3*time.Hour)
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
		_ = a.Store.TouchExport(name, "Failed", 0, 0, 0, "", 0)
		a.writeStatus(cm, name, map[string]any{"phase": "Failed", "error": err.Error()})
		return
	}
	var plan planner.Plan
	if err := json.Unmarshal(raw, &plan); err != nil {
		return
	}
	raw = nil
	start, err := time.Parse(time.RFC3339, ex.StartTime)
	if err != nil {
		start = time.Now().UTC().Truncate(15 * time.Minute)
	}
	end := start.Add(time.Duration(plan.Steps*plan.StepSeconds) * time.Second)
	if ex.EndTime != "" {
		if parsed, err := time.Parse(time.RFC3339, ex.EndTime); err == nil {
			end = parsed
		}
	}
	started := time.Now().Unix()
	if ex.Type == "generic" {
		a.buildGeneric(cm, name, dsName, &plan, ex, start, end, started)
		return
	}
	if row, err := a.Store.GetExport(name); err == nil && row.StartedAt > 0 && row.Phase == "Building" {
		started = row.StartedAt
	}
	a.runConverters(c, cm, name, dsName, &plan, ex, start, end, started)
}

func (a *App) buildGeneric(cm *corev1.ConfigMap, name, dsName string, plan *planner.Plan, ex spec.Export, start, end time.Time, started int64) {
	step := time.Duration(plan.StepSeconds) * time.Second
	interval, err := aggregate.ParseInterval(ex.Interval, step)
	if err != nil {
		_ = a.Store.TouchExport(name, "Failed", 0, 0, 0, "", started)
		a.writeStatus(cm, name, map[string]any{"phase": "Failed", "error": err.Error()})
		return
	}
	n := 0
	if interval > 0 {
		n = plan.Steps / int(interval/step)
	}
	pages := plan.Chunks
	if pages < 1 {
		cs := plan.ChunkSamples
		if cs < 1 {
			cs = 1
		}
		pages = (plan.Steps + cs - 1) / cs
	}
	if pages < 1 {
		pages = 1
	}
	_ = a.ensureCatalog(dsName, plan)
	_ = a.Store.TouchExport(name, "Serving", 0, 0, 0, "", started)
	a.writeStatus(cm, name, map[string]any{
		"phase": "Serving", "sessionId": ex.SessionID,
		"startTime": start.UTC().Format(time.RFC3339), "endTime": end.UTC().Format(time.RFC3339),
		"interval": ex.Interval, "intervals": n, "pages": pages,
	})
}

func writeCatalog(dir string, plan *planner.Plan) error {
	raw, err := json.MarshalIndent(planner.Catalog(plan), "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dir, "catalog.json"), raw, 0o664)
}

func (a *App) ensureCatalog(dataset string, plan *planner.Plan) error {
	dir := filepath.Join(a.Data, "datasets", dataset, "plan")
	path := filepath.Join(dir, "catalog.json")
	if _, err := os.Stat(path); err == nil {
		return nil
	}
	if err := os.MkdirAll(dir, 0o775); err != nil {
		return err
	}
	return writeCatalog(dir, plan)
}

func (a *App) cleanupExport(cm *corev1.ConfigMap, name string) {
	ds := cm.Labels["kubegenx.io/dataset"]
	a.deleteConverterJob(name)
	_ = a.Store.DeleteBlocks(name)
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
							Name: "store", Image: a.ThanosImage, ImagePullPolicy: corev1.PullAlways,
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
							Name: "query", Image: a.ThanosImage, ImagePullPolicy: corev1.PullAlways,
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

func withQueryWindow(raw string, start, end time.Time) string {
	if raw == "" || !end.After(start) {
		return raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	dur := end.Sub(start)
	endStamp := end.UTC().Format("2006-01-02 15:04:05")
	u.Path = "/graph"
	q := url.Values{}
	q.Set("g0.expr", "count by (namespace) (kube_pod_info)")
	q.Set("g0.tab", "0")
	q.Set("g0.range_input", promRange(dur))
	q.Set("g0.end_input", endStamp)
	q.Set("g0.moment_input", endStamp)
	u.RawQuery = q.Encode()
	return u.String()
}

func promRange(d time.Duration) string {
	switch {
	case d%(24*time.Hour) == 0:
		return fmt.Sprintf("%dd", int(d/(24*time.Hour)))
	case d%time.Hour == 0:
		return fmt.Sprintf("%dh", int(d/time.Hour))
	default:
		return fmt.Sprintf("%ds", int(d/time.Second))
	}
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
