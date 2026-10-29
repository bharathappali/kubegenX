package app

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/bharathappali/kubegenx/internal/aggregate"
	"github.com/bharathappali/kubegenx/internal/planner"
	"github.com/bharathappali/kubegenx/internal/spec"
	"github.com/bharathappali/kubegenx/internal/store"
	"github.com/bharathappali/kubegenx/internal/verify"
	"gopkg.in/yaml.v3"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (a *App) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /api/v1/health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, 200, map[string]any{"ok": true})
	})
	mux.HandleFunc("GET /api/v1/templates", a.listTemplates)
	mux.HandleFunc("POST /api/v1/templates", a.createTemplate)
	mux.HandleFunc("GET /api/v1/datasets", a.listDatasets)
	mux.HandleFunc("POST /api/v1/datasets", a.createDataset)
	mux.HandleFunc("GET /api/v1/datasets/{ds}", a.getDataset)
	mux.HandleFunc("GET /api/v1/datasets/{ds}/progress", a.getDataset)
	mux.HandleFunc("GET /api/v1/datasets/{ds}/layout", a.getLayout)
	mux.HandleFunc("GET /api/v1/datasets/{ds}/workloads", a.getWorkloads)
	mux.HandleFunc("GET /api/v1/datasets/{ds}/labels", a.getLabels)
	mux.HandleFunc("GET /api/v1/datasets/{ds}/orgs", a.getOrgs)
	mux.HandleFunc("GET /api/v1/datasets/{ds}/verify", a.getVerify)
	mux.HandleFunc("DELETE /api/v1/datasets/{ds}", a.deleteDataset)
	mux.HandleFunc("DELETE /api/v1/datasets/{ds}/parquet", a.deleteParquet)
	mux.HandleFunc("GET /api/v1/exports", a.listExports)
	mux.HandleFunc("POST /api/v1/exports", a.createExport)
	mux.HandleFunc("GET /api/v1/exports/{name}", a.getExport)
	mux.HandleFunc("DELETE /api/v1/exports/{name}", a.deleteExport)
	mux.HandleFunc("POST /api/v1/exports/{name}/serve", a.serveExport)
	mux.HandleFunc("GET /api/v1/exports/{name}/blocks", a.listBlocks)
	mux.HandleFunc("POST /api/v1/exports/{name}/blocks/lease", a.leaseBlocks)
	mux.HandleFunc("POST /api/v1/exports/{name}/blocks/heartbeat", a.heartbeatBlocks)
	mux.HandleFunc("POST /api/v1/exports/{name}/blocks/{id}/complete", a.completeBlock)
	mux.HandleFunc("POST /api/v1/exports/{name}/blocks/{id}/fail", a.failBlock)
	mux.HandleFunc("GET /api/v1/storage", a.getStorage)
	mux.HandleFunc("GET /api/v1/datasets/{ds}/sessions/{id}/namespaces", a.sessionNamespaces)
	mux.HandleFunc("GET /api/v1/datasets/{ds}/sessions/{id}/namespaces/{ns}/workloads", a.sessionWorkloads)
	mux.HandleFunc("GET /api/v1/datasets/{ds}/sessions/{id}/results", a.sessionResults)
	mux.HandleFunc("GET /api/v1/ws", a.ws)
	mux.Handle("/", a.ui())
	return mux
}

func (a *App) ui() http.Handler {
	sub, err := fsSub()
	if err != nil {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "text/html")
			_, _ = w.Write([]byte("<!doctype html><title>KubeGenX</title><h1>KubeGenX</h1>"))
		})
	}
	return http.FileServer(http.FS(sub))
}

func (a *App) listTemplates(w http.ResponseWriter, r *http.Request) {
	items, err := a.listKind("template")
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"items": items})
}

func (a *App) createTemplate(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string        `json:"name"`
		Spec spec.Template `json:"spec"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if !dnsOK(body.Name) {
		writeErr(w, 400, "invalid name")
		return
	}
	if body.Spec.Seed == 0 && body.Spec.Time.Step == "" {
		body.Spec = spec.DefaultTemplate()
	}
	raw, _ := yaml.Marshal(body.Spec)
	if err := a.putCM("template", body.Name, "", map[string]string{"spec.yaml": string(raw)}, false); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 201, map[string]any{"name": body.Name})
}

func (a *App) createDataset(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name     string       `json:"name"`
		Template string       `json:"template"`
		Epoch    string       `json:"epoch"`
		Effort   string       `json:"effort"`
		Workers  spec.Workers `json:"workers"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if !dnsOK(body.Name) {
		writeErr(w, 400, "invalid name")
		return
	}
	c, cancel := ctx()
	defer cancel()
	tplCM, err := a.Kube.CoreV1().ConfigMaps(a.NS).Get(c, cmName("template", body.Template), metav1.GetOptions{})
	if err != nil {
		writeErr(w, 404, "template not found")
		return
	}
	var tpl spec.Template
	if err := yaml.Unmarshal([]byte(tplCM.Data["spec.yaml"]), &tpl); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if body.Workers.CPU != "" {
		tpl.Workers.CPU = body.Workers.CPU
	}
	if body.Workers.Memory != "" {
		tpl.Workers.Memory = body.Workers.Memory
	}
	if body.Workers.Threads > 0 {
		tpl.Workers.Threads = body.Workers.Threads
	}
	tpl.Workers = a.sizeWorkers(c, body.Effort, tpl.Workers)
	epoch := body.Epoch
	if epoch == "" {
		epoch = defaultEpoch(time.Now(), tpl)
	}
	ds := spec.Dataset{
		TemplateRef:      spec.TemplateRef{Name: body.Template},
		TemplateSnapshot: tpl,
		Epoch:            epoch,
		Workers:          tpl.Workers,
		Exporters:        spec.DatasetExports{Kruize: spec.KruizeExport{Enabled: true, ClusterName: "kgx-" + body.Name, PerformanceProfile: "resource-optimization-openshift"}},
	}
	raw, _ := yaml.Marshal(ds)
	if err := a.putCM("dataset", body.Name, body.Template, map[string]string{"spec.yaml": string(raw)}, true); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 201, map[string]any{"name": body.Name})
}

func (a *App) listDatasets(w http.ResponseWriter, r *http.Request) {
	items, err := a.listKind("dataset")
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"items": items})
}

func (a *App) getDataset(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("ds")
	writeJSON(w, 200, a.progressDoc(name, ""))
}

func (a *App) getLayout(w http.ResponseWriter, r *http.Request) {
	man, err := readManifest(a.Data, r.PathValue("ds"))
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	ranges, _ := man["namespaceRanges"].([]any)
	if ranges == nil {
		ranges = []any{}
	}
	writeJSON(w, 200, map[string]any{
		"chunks": man["chunks"], "chunkSeconds": man["chunkSeconds"], "stepSeconds": man["stepSeconds"],
		"epoch": man["epoch"], "workloads": man["workloads"], "namespaces": ranges,
	})
}

func (a *App) getWorkloads(w http.ResponseWriter, r *http.Request) {
	raw, err := os.ReadFile(filepath.Join(a.Data, "datasets", r.PathValue("ds"), "plan", "workloads.json"))
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	var all []map[string]any
	if err := json.Unmarshal(raw, &all); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	offset, _ := strconv.Atoi(r.URL.Query().Get("offset"))
	limit, _ := strconv.Atoi(r.URL.Query().Get("limit"))
	if offset < 0 {
		offset = 0
	}
	if limit < 1 {
		limit = 40
	}
	if limit > 100 {
		limit = 100
	}
	if offset > len(all) {
		offset = len(all)
	}
	end := offset + limit
	if end > len(all) {
		end = len(all)
	}
	items := all[offset:end]
	if items == nil {
		items = []map[string]any{}
	}
	writeJSON(w, 200, map[string]any{"total": len(all), "offset": offset, "items": items})
}

func readManifest(data, name string) (map[string]any, error) {
	raw, err := os.ReadFile(filepath.Join(data, "datasets", name, "plan", "manifest.json"))
	if err != nil {
		return nil, err
	}
	var man map[string]any
	if err := json.Unmarshal(raw, &man); err != nil {
		return nil, err
	}
	return man, nil
}

func (a *App) getLabels(w http.ResponseWriter, r *http.Request) {
	plan, err := loadPlan(a.Data, r.PathValue("ds"))
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	type combo struct {
		Labels     map[string]string `json:"labels"`
		Namespaces int               `json:"namespaces"`
	}
	counts := map[string]*combo{}
	var order []string
	for _, n := range plan.Namespaces {
		key := n.Labels["org_id"] + "|" + n.Labels["cluster_id"]
		c, ok := counts[key]
		if !ok {
			c = &combo{Labels: n.Labels, Namespaces: 0}
			counts[key] = c
			order = append(order, key)
		}
		c.Namespaces++
	}
	var combos []combo
	for _, k := range order {
		combos = append(combos, *counts[k])
	}
	writeJSON(w, 200, map[string]any{"keys": plan.LabelKeys, "combinations": combos, "totalNamespaces": len(plan.Namespaces)})
}

func (a *App) getOrgs(w http.ResponseWriter, r *http.Request) {
	plan, err := loadPlan(a.Data, r.PathValue("ds"))
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	writeJSON(w, 200, planner.OrgMapOf(plan))
}

func (a *App) getVerify(w http.ResponseWriter, r *http.Request) {
	rep, err := verify.Dataset(a.Data, r.PathValue("ds"))
	if err != nil {
		writeJSON(w, 409, map[string]any{"error": err.Error(), "report": rep})
		return
	}
	writeJSON(w, 200, rep)
}

func (a *App) deleteDataset(w http.ResponseWriter, r *http.Request) {
	c, cancel := ctx()
	defer cancel()
	err := a.Kube.CoreV1().ConfigMaps(a.NS).Delete(c, cmName("dataset", r.PathValue("ds")), metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{"deleted": r.PathValue("ds")})
}

func (a *App) deleteParquet(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("ds")
	live, err := a.Store.LiveSessions(name)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if len(live) > 0 {
		ids := []string{}
		for _, s := range live {
			ids = append(ids, s.ID)
		}
		writeJSON(w, 409, map[string]any{"reason": "KruizeSessionsDependOnParquet", "sessions": ids})
		return
	}
	if err := os.RemoveAll(filepath.Join(a.Data, "datasets", name, "samples")); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	_ = a.Store.MarkParquetDeleted(name)
	writeJSON(w, 200, map[string]any{"deleted": "parquet"})
}

func (a *App) listExports(w http.ResponseWriter, r *http.Request) {
	rows, err := a.Store.ListExports()
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if rows == nil {
		rows = []store.ExportRow{}
	}
	writeJSON(w, 200, map[string]any{"items": rows})
}

func (a *App) createExport(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Dataset  string `json:"dataset"`
		Type     string `json:"type"`
		Anchor   string `json:"anchor"`
		Time     string `json:"time"`
		Interval string `json:"interval"`
		Effort   string `json:"effort"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if body.Type == "" {
		body.Type = "thanos"
	}
	if body.Type != "thanos" && body.Type != "generic" {
		writeErr(w, 400, "type must be thanos or generic")
		return
	}
	if body.Dataset == "" {
		writeErr(w, 400, "dataset is required")
		return
	}
	plan, err := loadPlan(a.Data, body.Dataset)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	start, end, err := aggregate.Window(plan, body.Anchor, body.Time)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if body.Anchor == "" {
		body.Anchor = "start"
	}
	existing, err := a.Store.ListExports()
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	name := body.Dataset
	sessionID := ""
	if body.Type == "thanos" {
		for _, row := range existing {
			if row.Dataset == body.Dataset && (row.Type == "" || row.Type == "thanos") {
				writeErr(w, 409, "dataset already has a Thanos export")
				return
			}
		}
		if !dnsOK(name) {
			writeErr(w, 400, "invalid name")
			return
		}
	} else {
		step := time.Duration(plan.StepSeconds) * time.Second
		if _, err := aggregate.ParseInterval(body.Interval, step); err != nil {
			writeErr(w, 400, err.Error())
			return
		}
		name = newSessionID()
		sessionID = name
	}
	if body.Effort == "" {
		body.Effort = "xhigh"
	}
	ex := spec.Export{
		Type: body.Type, Dataset: body.Dataset, Effort: body.Effort, Anchor: body.Anchor,
		StartTime: start.Format(time.RFC3339), EndTime: end.Format(time.RFC3339),
		Interval: body.Interval, SessionID: sessionID, BlockDuration: "2h",
		ExternalLabels: map[string]string{"kubegenx_export": name},
	}
	raw, _ := yaml.Marshal(ex)
	row := store.ExportRow{
		Name: name, Dataset: body.Dataset, Phase: "Configured", Type: body.Type, SessionID: sessionID,
		Anchor: body.Anchor, StartTime: ex.StartTime, EndTime: ex.EndTime, Interval: body.Interval,
	}
	if err := a.Store.SaveExport(row); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if err := a.putCM("export", name, body.Dataset, map[string]string{"spec.yaml": string(raw), "_type": body.Type}, true); err != nil {
		_ = a.Store.DeleteExport(name)
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 201, map[string]any{"name": name, "sessionId": sessionID, "startTime": ex.StartTime, "endTime": ex.EndTime, "type": body.Type})
}

func newSessionID() string {
	s := strings.ToLower(strconv.FormatInt(time.Now().UnixNano(), 36))
	if len(s) > 12 {
		s = s[len(s)-12:]
	}
	return "g" + s
}

func (a *App) getExport(w http.ResponseWriter, r *http.Request) {
	row, err := a.Store.GetExport(r.PathValue("name"))
	if err != nil {
		writeErr(w, 404, "not found")
		return
	}
	writeJSON(w, 200, row)
}

func (a *App) deleteExport(w http.ResponseWriter, r *http.Request) {
	c, cancel := ctx()
	defer cancel()
	err := a.Kube.CoreV1().ConfigMaps(a.NS).Delete(c, cmName("export", r.PathValue("name")), metav1.DeleteOptions{})
	if err != nil && !errors.IsNotFound(err) {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 202, map[string]any{"deleted": r.PathValue("name")})
}

func (a *App) getStorage(w http.ResponseWriter, r *http.Request) {
	used, datasets, exports, err := a.Store.Storage()
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if datasets == nil {
		datasets = []store.StorageDataset{}
	}
	if exports == nil {
		exports = []store.ExportRow{}
	}
	writeJSON(w, 200, map[string]any{
		"volume": "kubegenx-data", "capacityBytes": store.CapacityBytes, "usedBytes": used,
		"datasets": datasets, "exports": exports,
	})
}

func (a *App) ws(w http.ResponseWriter, r *http.Request) {
	c, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	topics := strings.Split(r.URL.Query().Get("topics"), ",")
	a.Hub.Add(c, topics)
	defer func() {
		a.Hub.Remove(c)
		_ = c.Close()
	}()
	for _, t := range topics {
		if strings.HasPrefix(t, "dataset:") {
			a.publishProgress(strings.TrimPrefix(t, "dataset:"), "")
		}
	}
	for {
		if _, _, err := c.ReadMessage(); err != nil {
			return
		}
	}
}

func (a *App) putCM(kind, name, dataset string, data map[string]string, finalizer bool) error {
	labels := map[string]string{
		"app.kubernetes.io/part-of": "kubegenx", "app.kubernetes.io/managed-by": "kubegenx",
		"kubegenx.io/kind": kind, "kubegenx.io/name": name,
	}
	if dataset != "" {
		labels["kubegenx.io/dataset"] = dataset
	}
	if kind == "export" {
		typ := data["_type"]
		if typ == "" {
			typ = "thanos"
		}
		delete(data, "_type")
		labels["kubegenx.io/export-type"] = typ
	}
	cm := &corev1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: cmName(kind, name), Namespace: a.NS, Labels: labels}, Data: data}
	if finalizer {
		cm.Finalizers = []string{"kubegenx.io/cleanup"}
	}
	c, cancel := ctx()
	defer cancel()
	_, err := a.Kube.CoreV1().ConfigMaps(a.NS).Create(c, cm, metav1.CreateOptions{})
	return err
}

func (a *App) listKind(kind string) ([]map[string]any, error) {
	c, cancel := ctx()
	defer cancel()
	list, err := a.Kube.CoreV1().ConfigMaps(a.NS).List(c, metav1.ListOptions{LabelSelector: "kubegenx.io/kind=" + kind})
	if err != nil {
		return nil, err
	}
	var items []map[string]any
	for _, cm := range list.Items {
		name := cm.Labels["kubegenx.io/name"]
		item := map[string]any{"name": name}
		if kind == "dataset" {
			phase, _ := a.Store.Phase(name)
			p, _ := a.Store.Progress(name)
			item["phase"] = phase
			item["progress"] = statusFrom(p, phase, "")
			if man, err := readManifest(a.Data, name); err == nil {
				item["namespaces"] = man["namespaces"]
				item["workloads"] = man["workloads"]
				item["samples"] = man["samples"]
				item["entries"] = man["entries"]
				item["days"] = man["days"]
				item["stepSeconds"] = man["stepSeconds"]
			}
		}
		items = append(items, item)
	}
	if items == nil {
		items = []map[string]any{}
	}
	return items, nil
}
