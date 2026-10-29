package app

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"

	"github.com/bharathappali/kubegenx/internal/kruize"
)

func (a *App) serveExport(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("name")
	row, err := a.Store.GetExport(name)
	if err != nil {
		writeErr(w, 404, "not found")
		return
	}
	if row.Phase == "Building" || row.Phase == "Queued" || row.Phase == "Serving" {
		writeErr(w, 409, "export is already "+row.Phase)
		return
	}
	a.mu.Lock()
	if a.exporting != nil && a.exporting[name] {
		a.mu.Unlock()
		writeErr(w, 409, "export is already running")
		return
	}
	a.mu.Unlock()
	row.Phase = "Queued"
	row.StartedAt = time.Now().Unix()
	if err := a.Store.SaveExport(row); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	a.mu.Lock()
	a.kickExportLocked()
	a.mu.Unlock()
	writeJSON(w, 202, map[string]any{"name": name, "phase": "Queued"})
}

func (a *App) servingSession(w http.ResponseWriter, r *http.Request) (string, bool) {
	id := r.PathValue("id")
	row, err := a.Store.GetExport(id)
	if err != nil || row.Dataset != r.PathValue("ds") || row.Type != "generic" {
		writeErr(w, 404, "session not found")
		return "", false
	}
	if row.Phase != "Serving" {
		writeErr(w, 409, "session is not serving")
		return "", false
	}
	return id, true
}

func (a *App) sessionNamespaces(w http.ResponseWriter, r *http.Request) {
	id, ok := a.servingSession(w, r)
	if !ok {
		return
	}
	plan, err := loadPlan(a.Data, r.PathValue("ds"))
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	_ = id
	names := make([]string, 0, len(plan.Namespaces))
	for _, n := range plan.Namespaces {
		names = append(names, n.Name)
	}
	writeJSON(w, 200, map[string]any{"sessionId": id, "namespaces": names})
}

func (a *App) sessionWorkloads(w http.ResponseWriter, r *http.Request) {
	id, ok := a.servingSession(w, r)
	if !ok {
		return
	}
	plan, err := loadPlan(a.Data, r.PathValue("ds"))
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	want := r.PathValue("ns")
	nsID := -1
	for _, n := range plan.Namespaces {
		if n.Name == want {
			nsID = n.ID
			break
		}
	}
	if nsID < 0 {
		writeErr(w, 404, "namespace not found")
		return
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
	var workloads []map[string]any
	for _, wl := range plan.Workloads {
		if wl.NamespaceID != nsID || (wl.Kind != "Deployment" && wl.Kind != "StatefulSet") {
			continue
		}
		names := ctrs[wl.ID]
		if names == nil {
			names = []string{}
		}
		workloads = append(workloads, map[string]any{
			"name": wl.Name, "kind": wl.Kind, "replicas": wl.Replicas, "containers": names,
		})
	}
	if workloads == nil {
		workloads = []map[string]any{}
	}
	writeJSON(w, 200, map[string]any{"sessionId": id, "namespace": want, "workloads": workloads})
}

func (a *App) sessionResults(w http.ResponseWriter, r *http.Request) {
	id, ok := a.servingSession(w, r)
	if !ok {
		return
	}
	row, err := a.Store.GetExport(id)
	if err != nil {
		writeErr(w, 404, "session not found")
		return
	}
	raw, err := os.ReadFile(filepath.Join(a.Data, "exports", row.Dataset, id, "buckets.json"))
	if err != nil {
		writeErr(w, 404, "results are not ready")
		return
	}
	var doc struct {
		Start time.Time       `json:"start"`
		End   time.Time       `json:"end"`
		Rows  []kruize.Bucket `json:"rows"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	q := r.URL.Query()
	from, to := doc.Start, doc.End
	if s := q.Get("start"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			writeErr(w, 400, "start must be RFC3339")
			return
		}
		from = t
	}
	if s := q.Get("end"); s != "" {
		t, err := time.Parse(time.RFC3339, s)
		if err != nil {
			writeErr(w, 400, "end must be RFC3339")
			return
		}
		to = t
	}
	if from.Before(doc.Start) || to.After(doc.End) {
		writeErr(w, 400, "start and end must sit inside the session window")
		return
	}
	ns, wl := q.Get("namespace"), q.Get("workload")
	var matched []kruize.Bucket
	for _, b := range doc.Rows {
		if !b.Start.Before(to) || !b.End.After(from) {
			continue
		}
		if ns != "" && b.Namespace != ns {
			continue
		}
		if wl != "" && b.Workload != wl {
			continue
		}
		matched = append(matched, b)
	}
	sort.Slice(matched, func(i, j int) bool {
		a, b := matched[i], matched[j]
		if a.Namespace != b.Namespace {
			return a.Namespace < b.Namespace
		}
		if a.Workload != b.Workload {
			return a.Workload < b.Workload
		}
		if a.Container != b.Container {
			return a.Container < b.Container
		}
		return a.Start.Before(b.Start)
	})
	if matched == nil {
		matched = []kruize.Bucket{}
	}
	page, _ := strconv.Atoi(q.Get("page"))
	if page < 1 {
		page = 1
	}
	const pageSize = 200
	total := len(matched)
	pages := (total + pageSize - 1) / pageSize
	if pages == 0 {
		pages = 1
	}
	i := (page - 1) * pageSize
	if i > total {
		i = total
	}
	j := i + pageSize
	if j > total {
		j = total
	}
	writeJSON(w, 200, map[string]any{
		"sessionId": id, "page": page, "pageSize": pageSize, "total": total, "pages": pages,
		"items": matched[i:j],
	})
}
