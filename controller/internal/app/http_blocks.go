package app

import (
	"encoding/json"
	"net/http"
	"time"

	"github.com/bharathappali/kubegenx/internal/store"
)

func (a *App) listBlocks(w http.ResponseWriter, r *http.Request) {
	items, err := a.Store.ListBlocks(r.PathValue("name"))
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if items == nil {
		items = []store.Block{}
	}
	writeJSON(w, 200, map[string]any{"items": items})
}

func (a *App) leaseBlocks(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkerID string `json:"workerId"`
		N        int    `json:"n"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if body.WorkerID == "" {
		body.WorkerID = "converter"
	}
	blocks, drained, err := a.Store.LeaseBlocks(r.PathValue("name"), body.WorkerID, body.N, 20*time.Minute)
	if err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	if blocks == nil {
		blocks = []store.Block{}
	}
	writeJSON(w, 200, map[string]any{"blocks": blocks, "drained": drained})
}

func (a *App) heartbeatBlocks(w http.ResponseWriter, r *http.Request) {
	var body struct {
		WorkerID string `json:"workerId"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if err := a.Store.HeartbeatBlocks(r.PathValue("name"), body.WorkerID, 20*time.Minute); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *App) completeBlock(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Bytes int64 `json:"bytes"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if err := a.Store.CompleteBlock(r.PathValue("name"), r.PathValue("id"), body.Bytes); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}

func (a *App) failBlock(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Retryable bool `json:"retryable"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
		writeErr(w, 400, err.Error())
		return
	}
	if err := a.Store.FailBlock(r.PathValue("name"), r.PathValue("id"), body.Retryable); err != nil {
		writeErr(w, 500, err.Error())
		return
	}
	writeJSON(w, 200, map[string]any{"ok": true})
}
