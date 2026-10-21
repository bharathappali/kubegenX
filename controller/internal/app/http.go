package app

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/bharathappali/kubegenx/internal/kruize"
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
	mux.HandleFunc("GET /api/v1/datasets/{ds}/labels", a.getLabels)
	mux.HandleFunc("GET /api/v1/datasets/{ds}/verify", a.getVerify)
	mux.HandleFunc("DELETE /api/v1/datasets/{ds}", a.deleteDataset)
	mux.HandleFunc("DELETE /api/v1/datasets/{ds}/parquet", a.deleteParquet)
	mux.HandleFunc("GET /api/v1/exports", a.listExports)
	mux.HandleFunc("POST /api/v1/exports", a.createExport)
	mux.HandleFunc("GET /api/v1/exports/{name}", a.getExport)
	mux.HandleFunc("DELETE /api/v1/exports/{name}", a.deleteExport)
	mux.HandleFunc("GET /api/v1/storage", a.getStorage)
	mux.HandleFunc("POST /api/v1/datasets/{ds}/exporters/kruize/sessions", a.createSession)
	mux.HandleFunc("GET /api/v1/datasets/{ds}/exporters/kruize/sessions", a.listSessions)
	mux.HandleFunc("GET /api/v1/datasets/{ds}/exporters/kruize/sessions/{sid}", a.getSession)
	mux.HandleFunc("DELETE /api/v1/datasets/{ds}/exporters/kruize/sessions/{sid}", a.deleteSession)
	mux.HandleFunc("GET /api/v1/datasets/{ds}/exporters/kruize/sessions/{sid}/experiments", a.getExperiments)
	mux.HandleFunc("GET /api/v1/datasets/{ds}/exporters/kruize/sessions/{sid}/experiments/{exp}/results", a.getResults)
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
	if body.Workers.Count > 0 {
		tpl.Workers = body.Workers
	}
	ds := spec.Dataset{
		TemplateRef:      spec.TemplateRef{Name: body.Template},
		TemplateSnapshot: tpl,
		Epoch:            body.Epoch,
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
	phase, _ := a.Store.Phase(name)
	p, _ := a.Store.Progress(name)
	doc := statusFrom(p, phase, "")
	doc["name"] = name
	writeJSON(w, 200, doc)
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
		Name      string `json:"name"`
		Dataset   string `json:"dataset"`
		StartTime string `json:"startTime"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if !dnsOK(body.Name) {
		writeErr(w, 400, "invalid name")
		return
	}
	ex := spec.Export{Type: "thanos", Dataset: body.Dataset, StartTime: body.StartTime, BlockDuration: "24h", ExternalLabels: map[string]string{"kubegenx_export": body.Name}}
	raw, _ := yaml.Marshal(ex)
	if err := a.putCM("export", body.Name, body.Dataset, map[string]string{"spec.yaml": string(raw)}, true); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 201, map[string]any{"name": body.Name})
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

func (a *App) createSession(w http.ResponseWriter, r *http.Request) {
	ds := r.PathValue("ds")
	var body struct {
		EndTime string `json:"endTime"`
		Name    string `json:"name"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil && err != io.EOF {
		writeErr(w, 400, err.Error())
		return
	}
	deleted, _ := a.Store.ParquetDeleted(ds)
	if deleted {
		writeJSON(w, 410, map[string]any{"error": "Gone"})
		return
	}
	plan, err := loadPlan(a.Data, ds)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	end, err := kruize.FloorEnd(body.EndTime, time.Now())
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	offset, originalEnd, effStart, effEnd, err := kruize.Offset(plan, end)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	id := "s-" + strconv.FormatInt(time.Now().UnixNano(), 36)
	now := time.Now().UTC()
	sess := store.Session{
		ID: id, Dataset: ds, Name: body.Name, EndTime: end.Format(time.RFC3339),
		OffsetSeconds: int64(offset.Seconds()), CreatedAt: now, ExpiresAt: now.Add(72 * time.Hour), LastAccess: now,
	}
	if err := a.Store.CreateSession(sess); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	pages := (len(plan.Workloads) + 99) / 100
	writeJSON(w, 201, map[string]any{
		"sessionId": id, "name": body.Name, "endTime": end.Format(time.RFC3339),
		"offsetSeconds":  int64(offset.Seconds()),
		"originalRange":  map[string]string{"start": plan.Epoch, "end": originalEnd.Format(time.RFC3339)},
		"effectiveRange": map[string]string{"start": effStart.Format(time.RFC3339), "end": effEnd.Format(time.RFC3339)},
		"totals": map[string]int{
			"experiments": len(plan.Workloads), "experimentPages": pages,
			"resultsPagesPerExperiment": plan.Chunks, "intervalsPerPage": 24,
		},
		"createdAt": now.Format(time.RFC3339), "expiresAt": sess.ExpiresAt.Format(time.RFC3339),
	})
}

func (a *App) listSessions(w http.ResponseWriter, r *http.Request) {
	items, err := a.Store.ListSessions(r.PathValue("ds"))
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if items == nil {
		items = []store.Session{}
	}
	writeJSON(w, 200, map[string]any{"items": items})
}

func (a *App) getSession(w http.ResponseWriter, r *http.Request) {
	sess, err := a.Store.GetSession(r.PathValue("sid"))
	if err != nil || sess.Dataset != r.PathValue("ds") {
		writeErr(w, 404, "not found")
		return
	}
	writeJSON(w, 200, sess)
}

func (a *App) deleteSession(w http.ResponseWriter, r *http.Request) {
	if err := a.Store.CloseSession(r.PathValue("sid")); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"closed": r.PathValue("sid")})
}

func (a *App) getExperiments(w http.ResponseWriter, r *http.Request) {
	sess, plan, ok := a.sessionPlan(w, r)
	if !ok {
		return
	}
	_ = a.Store.TouchSession(sess.ID)
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
	data, total, pages := kruize.Experiments(plan, page, pageSize)
	if page < 1 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 100
	}
	writeJSON(w, 200, map[string]any{
		"metadata": map[string]any{"dataset": plan.Dataset, "sessionId": sess.ID, "total": total, "page": page, "pageSize": pageSize, "totalPages": pages, "order": "wl_id"},
		"data":     data,
	})
}

func (a *App) getResults(w http.ResponseWriter, r *http.Request) {
	sess, plan, ok := a.sessionPlan(w, r)
	if !ok {
		return
	}
	_ = a.Store.TouchSession(sess.ID)
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	exp := r.PathValue("exp")
	data, err := kruize.Results(plan, filepath.Join(a.Data, "datasets", plan.Dataset), exp, page, time.Duration(sess.OffsetSeconds)*time.Second)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{
		"metadata": map[string]any{"dataset": plan.Dataset, "sessionId": sess.ID, "experiment": exp, "page": page, "chunk": page, "totalPages": plan.Chunks},
		"data":     data,
	})
}

func (a *App) sessionPlan(w http.ResponseWriter, r *http.Request) (store.Session, *planner.Plan, bool) {
	sess, err := a.Store.GetSession(r.PathValue("sid"))
	if err != nil || sess.Closed || sess.Dataset != r.PathValue("ds") {
		writeErr(w, 404, "session not found")
		return store.Session{}, nil, false
	}
	plan, err := loadPlan(a.Data, sess.Dataset)
	if err != nil {
		writeErr(w, 404, err.Error())
		return store.Session{}, nil, false
	}
	return sess, plan, true
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
		labels["kubegenx.io/export-type"] = "thanos"
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
		}
		items = append(items, item)
	}
	if items == nil {
		items = []map[string]any{}
	}
	return items, nil
}
