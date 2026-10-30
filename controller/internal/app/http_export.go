package app

import (
	"net/http"
	"path/filepath"
	"strconv"
	"time"

	"github.com/bharathappali/kubegenx/internal/aggregate"
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
	_ = a.ensureCatalog(r.PathValue("ds"), plan)
	all := make([]map[string]any, 0, len(plan.Namespaces))
	for _, n := range plan.Namespaces {
		all = append(all, map[string]any{
			"namespace": n.Name, "orgId": n.Labels["org_id"], "clusterId": n.Labels["cluster_id"],
		})
	}
	items, page, total, pages := pageOf(all, queryPage(r))
	names := make([]string, 0, len(items))
	for _, item := range items {
		names = append(names, item["namespace"].(string))
	}
	writeJSON(w, 200, map[string]any{
		"sessionId": id, "page": page, "pageSize": metaPageSize, "total": total, "pages": pages,
		"namespaces": names, "items": items,
	})
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
	var nsLabels map[string]string
	nsID := -1
	for _, n := range plan.Namespaces {
		if n.Name == want {
			nsID = n.ID
			nsLabels = n.Labels
			break
		}
	}
	if nsID < 0 {
		writeErr(w, 404, "namespace not found")
		return
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
	var all []map[string]any
	for _, wl := range plan.Workloads {
		if wl.NamespaceID != nsID || (wl.Kind != "Deployment" && wl.Kind != "StatefulSet") {
			continue
		}
		names := ctrs[wl.ID]
		if names == nil {
			names = []string{}
		}
		podNames := pods[wl.ID]
		if podNames == nil {
			podNames = []string{}
		}
		all = append(all, map[string]any{
			"name": wl.Name, "kind": wl.Kind, "replicas": wl.Replicas,
			"containers": names, "pods": podNames,
			"orgId": nsLabels["org_id"], "clusterId": nsLabels["cluster_id"],
		})
	}
	items, page, total, pages := pageOf(all, queryPage(r))
	writeJSON(w, 200, map[string]any{
		"sessionId": id, "namespace": want, "page": page, "pageSize": metaPageSize, "total": total, "pages": pages,
		"workloads": items,
	})
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
	plan, err := loadPlan(a.Data, row.Dataset)
	if err != nil {
		writeErr(w, 404, err.Error())
		return
	}
	origin, err := time.Parse(time.RFC3339, row.StartTime)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	end, err := time.Parse(time.RFC3339, row.EndTime)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	step := time.Duration(plan.StepSeconds) * time.Second
	interval, err := aggregate.ParseInterval(row.Interval, step)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	q := r.URL.Query()
	from, to := origin, end
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
	if from.Before(origin) || to.After(end) {
		writeErr(w, 400, "start and end must sit inside the session window")
		return
	}
	page := queryPage(r)
	got, err := kruize.AggregatePage(plan, filepath.Join(a.Data, "datasets", row.Dataset), origin, interval, q.Get("namespace"), q.Get("workload"), from, to, page)
	if err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if got.Items == nil {
		got.Items = []kruize.Bucket{}
	}
	body := map[string]any{
		"sessionId": id, "page": got.Page, "pageSize": got.PageSize, "total": got.Total, "pages": got.Pages,
		"intervals": got.Intervals, "series": got.Series, "items": got.Items,
	}
	if !got.Start.IsZero() {
		body["start"] = got.Start
		body["end"] = got.End
	}
	writeJSON(w, 200, body)
}

const metaPageSize = 100

func queryPage(r *http.Request) int {
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	return page
}

func pageOf[T any](all []T, page int) (items []T, pageOut, total, pages int) {
	total = len(all)
	pages = 1
	if total > 0 {
		pages = (total + metaPageSize - 1) / metaPageSize
	}
	if page > pages {
		page = pages
	}
	i := (page - 1) * metaPageSize
	if i > total {
		i = total
	}
	j := i + metaPageSize
	if j > total {
		j = total
	}
	items = all[i:j]
	if items == nil {
		items = []T{}
	}
	return items, page, total, pages
}
