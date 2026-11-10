// Command converter claims 2h blocks from the controller and writes TSDB
// blocks on the shared data volume.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/bharathappali/kubegenx/internal/export"
	"github.com/bharathappali/kubegenx/internal/planner"
)

type leaseResp struct {
	Blocks  []exportBlock `json:"blocks"`
	Drained bool          `json:"drained"`
}

type exportBlock struct {
	ID     string `json:"id"`
	Chunk  int    `json:"chunk"`
	Index  int    `json:"index"`
	WlLo   int    `json:"wlLo"`
	WlHi   int    `json:"wlHi"`
	StepLo int    `json:"stepLo"`
	StepHi int    `json:"stepHi"`
	EmitNS bool   `json:"emitNS"`
}

func main() {
	exportName := os.Getenv("KUBEGENX_EXPORT")
	dataset := os.Getenv("KUBEGENX_DATASET")
	base := os.Getenv("KUBEGENX_CONTROLLER")
	data := env("KUBEGENX_DATA", "/data")
	scratch := env("KUBEGENX_SCRATCH", "/scratch")
	workerID := env("HOSTNAME", "converter")
	mode := os.Getenv("KUBEGENX_MODE")
	switch mode {
	case "compact-sync-in", "compact-sync-out", "compact-sync-in-window", "compact-sync-out-window":
		if exportName == "" || dataset == "" {
			slog.Error("missing KUBEGENX_EXPORT or KUBEGENX_DATASET")
			os.Exit(1)
		}
		dataTSDB := export.TSDBDir(data, dataset, exportName)
		scratchTSDB := filepath.Join(scratch, "tsdb")
		var err error
		switch mode {
		case "compact-sync-in":
			slog.Info("compact stage-in", "src", dataTSDB, "dst", scratchTSDB)
			err = export.CompactSyncIn(dataTSDB, scratchTSDB)
		case "compact-sync-out":
			slog.Info("compact stage-out", "src", scratchTSDB, "dst", dataTSDB)
			err = export.CompactSyncOut(scratchTSDB, dataTSDB)
		case "compact-sync-in-window":
			start, end, perr := parseCompactWindow()
			if perr != nil {
				slog.Error("window", "err", perr)
				os.Exit(1)
			}
			slog.Info("compact stage-in-window", "src", dataTSDB, "dst", scratchTSDB, "start", start, "end", end)
			err = export.CompactSyncInWindow(dataTSDB, scratchTSDB, start, end)
		case "compact-sync-out-window":
			slog.Info("compact stage-out-window", "src", scratchTSDB, "dst", dataTSDB)
			err = export.CompactSyncOutWindow(scratchTSDB, dataTSDB)
		}
		if err != nil {
			slog.Error(mode, "err", err)
			os.Exit(1)
		}
		slog.Info(mode + " ok")
		return
	}
	if exportName == "" || dataset == "" || base == "" {
		slog.Error("missing KUBEGENX_EXPORT, KUBEGENX_DATASET, or KUBEGENX_CONTROLLER")
		os.Exit(1)
	}
	if len(base) < 4 || base[:4] != "http" {
		base = "http://" + base
	}
	planPath := filepath.Join(data, "datasets", dataset, "plan", "generation.json")
	raw, err := os.ReadFile(planPath)
	if err != nil {
		slog.Error("plan", "err", err)
		os.Exit(1)
	}
	var plan planner.Plan
	if err := json.Unmarshal(raw, &plan); err != nil {
		slog.Error("plan", "err", err)
		os.Exit(1)
	}
	raw = nil
	// Samples are step indexes from the dataset epoch. The clock on the blocks
	// is the export start chosen at serve time, not that epoch.
	start := os.Getenv("KUBEGENX_START")
	if start == "" {
		slog.Error("missing KUBEGENX_START")
		os.Exit(1)
	}
	origin, err := time.Parse(time.RFC3339, start)
	if err != nil {
		slog.Error("start", "err", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go heartbeat(ctx, base, exportName, workerID)
	client := &http.Client{Timeout: 2 * time.Minute}
	labels := map[string]string{"kubegenx_export": exportName}
	datasetDir := filepath.Join(data, "datasets", dataset)
	dest := filepath.Join(data, "exports", dataset, exportName, "tsdb")
	for {
		blocks, drained, err := lease(client, base, exportName, workerID)
		if err != nil {
			slog.Error("lease", "err", err)
			time.Sleep(2 * time.Second)
			continue
		}
		if len(blocks) == 0 {
			if drained {
				slog.Info("queue drained")
				return
			}
			time.Sleep(2 * time.Second)
			continue
		}
		for _, b := range blocks {
			job := export.BlockJob{
				ID: b.ID, Chunk: b.Chunk, Index: b.Index,
				WlLo: b.WlLo, WlHi: b.WlHi, StepLo: b.StepLo, StepHi: b.StepHi, EmitNS: b.EmitNS,
			}
			sz, err := export.ConvertBlock(ctx, &plan, datasetDir, scratch, exportName, dest, origin, labels, job)
			if err != nil {
				slog.Error("block", "id", b.ID, "err", err)
				post(client, fmt.Sprintf("%s/api/v1/exports/%s/blocks/%s/fail", base, exportName, b.ID), map[string]any{
					"workerId": workerID, "error": err.Error(), "retryable": true,
				})
				continue
			}
			if err := post(client, fmt.Sprintf("%s/api/v1/exports/%s/blocks/%s/complete", base, exportName, b.ID), map[string]any{"bytes": sz}); err != nil {
				slog.Error("complete", "id", b.ID, "err", err)
			} else {
				slog.Info("block done", "id", b.ID, "bytes", sz)
			}
		}
	}
}

func heartbeat(ctx context.Context, base, exportName, workerID string) {
	client := &http.Client{Timeout: 10 * time.Second}
	t := time.NewTicker(15 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = post(client, fmt.Sprintf("%s/api/v1/exports/%s/blocks/heartbeat", base, exportName), map[string]any{"workerId": workerID})
		}
	}
}

func lease(client *http.Client, base, exportName, workerID string) ([]exportBlock, bool, error) {
	var out leaseResp
	if err := postInto(client, fmt.Sprintf("%s/api/v1/exports/%s/blocks/lease", base, exportName), map[string]any{"workerId": workerID, "n": 1}, &out); err != nil {
		return nil, false, err
	}
	return out.Blocks, out.Drained, nil
}

func post(client *http.Client, url string, body any) error {
	return postInto(client, url, body, nil)
}

func postInto(client *http.Client, url string, body, dest any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	res, err := client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode >= 300 {
		return fmt.Errorf("%s: %s", url, res.Status)
	}
	if dest == nil {
		return nil
	}
	return json.NewDecoder(res.Body).Decode(dest)
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func parseCompactWindow() (time.Time, time.Time, error) {
	minRaw := os.Getenv("KUBEGENX_MIN_TIME")
	maxRaw := os.Getenv("KUBEGENX_MAX_TIME")
	if minRaw == "" || maxRaw == "" {
		return time.Time{}, time.Time{}, fmt.Errorf("missing KUBEGENX_MIN_TIME or KUBEGENX_MAX_TIME")
	}
	start, err := time.Parse(time.RFC3339, minRaw)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	end, err := time.Parse(time.RFC3339, maxRaw)
	if err != nil {
		return time.Time{}, time.Time{}, err
	}
	return start, end, nil
}
