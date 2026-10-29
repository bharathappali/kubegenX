package app

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/bharathappali/kubegenx/internal/export"
	"github.com/bharathappali/kubegenx/internal/planner"
	"github.com/bharathappali/kubegenx/internal/spec"
	"github.com/bharathappali/kubegenx/internal/store"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// runConverters fills the export with 2h block jobs and lets converter pods
// write them. This process only watches the queue and then starts Thanos.
func (a *App) runConverters(ctx context.Context, cm *corev1.ConfigMap, name, dsName string, plan *planner.Plan, ex spec.Export, start, end time.Time, started int64) {
	jobs := export.BlockJobs(plan)
	if len(jobs) == 0 {
		_ = a.Store.TouchExport(name, "Failed", 0, 0, 0, "", started)
		a.writeStatus(cm, name, map[string]any{"phase": "Failed", "error": "no samples landed in a block"})
		return
	}
	blocks := make([]store.Block, len(jobs))
	for i, job := range jobs {
		blocks[i] = store.Block{
			ID: job.ID, Chunk: job.Chunk, Index: job.Index,
			WlLo: job.WlLo, WlHi: job.WlHi, StepLo: job.StepLo, StepHi: job.StepHi, EmitNS: job.EmitNS,
		}
	}
	if err := a.Store.InsertBlocks(name, blocks); err != nil {
		slogErr("export blocks", err)
		_ = a.Store.TouchExport(name, "Failed", 0, 0, 0, "", started)
		a.writeStatus(cm, name, map[string]any{"phase": "Failed", "error": err.Error()})
		return
	}
	prog, err := a.Store.BlockProgress(name)
	if err != nil {
		slogErr("export blocks", err)
		return
	}
	dest := filepath.Join(a.Data, "exports", dsName, name, "tsdb")
	if prog.Done == 0 {
		if err := os.RemoveAll(dest); err != nil {
			slogErr("export reset", err)
			return
		}
	}
	if err := os.MkdirAll(dest, 0o775); err != nil {
		slogErr("export dir", err)
		return
	}
	effort := ex.Effort
	if effort == "" {
		effort = "xhigh"
	}
	workers := a.sizeWorkers(ctx, effort, spec.Workers{CPU: converterCPU, Memory: converterMem})
	slog.Info("export converters", "name", name, "blocks", len(jobs), "workers", workers.Count, "cpu", workers.CPU, "memory", workers.Memory, "effort", effort)
	if err := a.ensureConverterJob(cm, name, dsName, start, workers); err != nil {
		slogErr("converter job", err)
		_ = a.Store.TouchExport(name, "Failed", 0, prog.Total, 0, "", started)
		a.writeStatus(cm, name, map[string]any{"phase": "Failed", "error": err.Error()})
		return
	}
	_ = a.Store.TouchExport(name, "Building", prog.Done, prog.Total, prog.Bytes, "", started)
	_ = a.Store.TouchExportLive(name, workers.Count, 0, 0)
	a.writeStatus(cm, name, map[string]any{
		"phase": "Building", "blocks": prog.Done, "blocksTotal": prog.Total, "bytes": prog.Bytes,
		"workers": workers.Count, "startTime": start.UTC().Format(time.RFC3339), "endTime": end.UTC().Format(time.RFC3339), "startedAt": started,
	})
	tick := time.NewTicker(2 * time.Second)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		prog, err = a.Store.BlockProgress(name)
		if err != nil {
			slogErr("export progress", err)
			continue
		}
		_ = a.Store.TouchExport(name, "Building", prog.Done, prog.Total, prog.Bytes, "", started)
		_ = a.Store.TouchExportLive(name, workers.Count, 0, 0)
		a.writeStatus(cm, name, map[string]any{
			"phase": "Building", "blocks": prog.Done, "blocksTotal": prog.Total, "bytes": prog.Bytes,
			"workers": workers.Count, "startTime": start.UTC().Format(time.RFC3339), "endTime": end.UTC().Format(time.RFC3339), "startedAt": started,
		})
		if prog.Total == 0 {
			continue
		}
		if prog.Done+prog.Failed == prog.Total && prog.Leased == 0 {
			a.deleteConverterJob(name)
			if prog.Failed > 0 {
				_ = a.Store.TouchExport(name, "Failed", prog.Done, prog.Total, prog.Bytes, "", started)
				a.writeStatus(cm, name, map[string]any{"phase": "Failed", "error": "a block failed", "blocks": prog.Done, "blocksTotal": prog.Total})
				return
			}
			url, err := a.ensureThanos(cm, name, dsName)
			if err != nil {
				slogErr("thanos", err)
				_ = a.Store.TouchExport(name, "Failed", prog.Done, prog.Total, prog.Bytes, "", started)
				a.writeStatus(cm, name, map[string]any{"phase": "Failed", "error": err.Error(), "blocks": prog.Done})
				return
			}
			url = withQueryWindow(url, start, end)
			_ = a.Store.TouchExport(name, "Serving", prog.Done, prog.Total, prog.Bytes, url, started)
			a.writeStatus(cm, name, map[string]any{
				"phase": "Serving", "url": url, "blocks": prog.Done, "blocksTotal": prog.Total, "bytes": prog.Bytes,
				"startTime": start.UTC().Format(time.RFC3339), "endTime": end.UTC().Format(time.RFC3339),
			})
			return
		}
	}
}

func (a *App) datasetsBusy() bool {
	busy, err := a.Store.Generating()
	if err != nil {
		slogErr("datasets", err)
		return true
	}
	return busy
}

func (a *App) ensureConverterJob(parent *corev1.ConfigMap, exportName, dataset string, start time.Time, w spec.Workers) error {
	name := converterJobName(exportName)
	c, cancel := ctx()
	defer cancel()
	existing, err := a.Kube.BatchV1().Jobs(a.NS).Get(c, name, metav1.GetOptions{})
	if err == nil {
		if jobRunning(existing) {
			return nil
		}
		a.deleteConverterJob(exportName)
		_ = a.Store.RequeueBlocks(exportName)
	} else if !errors.IsNotFound(err) {
		return err
	} else {
		_ = a.Store.RequeueBlocks(exportName)
	}
	n := int32(w.Count)
	if n < 1 {
		n = 1
	}
	ttl := int32(600)
	backoff := int32(6)
	auto := false
	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: a.NS,
			Labels:          partLabels(map[string]string{"kubegenx.io/export": exportName}),
			OwnerReferences: []metav1.OwnerReference{ownerRef(parent)},
		},
		Spec: batchv1.JobSpec{
			Parallelism:             &n,
			Completions:             &n,
			TTLSecondsAfterFinished: &ttl,
			BackoffLimit:            &backoff,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: partLabels(map[string]string{"kubegenx.io/export": exportName})},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyOnFailure,
					ServiceAccountName:           "kubegenx-worker",
					AutomountServiceAccountToken: &auto,
					SecurityContext:              podSC(),
					Containers: []corev1.Container{{
						Name:            "converter",
						Image:           a.ConverterImage,
						ImagePullPolicy: corev1.PullAlways,
						Env: []corev1.EnvVar{
							{Name: "KUBEGENX_EXPORT", Value: exportName},
							{Name: "KUBEGENX_DATASET", Value: dataset},
							{Name: "KUBEGENX_CONTROLLER", Value: "http://kubegenx-controller." + a.NS + ".svc:8080"},
							{Name: "KUBEGENX_DATA", Value: "/data"},
							{Name: "KUBEGENX_SCRATCH", Value: "/scratch"},
							{Name: "KUBEGENX_START", Value: start.UTC().Format(time.RFC3339)},
						},
						Resources:       res(w.CPU, w.Memory),
						SecurityContext: containerSC(),
						VolumeMounts: []corev1.VolumeMount{
							{Name: "data", MountPath: "/data"},
							{Name: "scratch", MountPath: "/scratch"},
						},
					}},
					Volumes: []corev1.Volume{
						{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "kubegenx-data"}}},
						{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
					},
				},
			},
		},
	}
	_, err = a.Kube.BatchV1().Jobs(a.NS).Create(c, job, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func jobRunning(job *batchv1.Job) bool {
	if job.Status.Succeeded >= completionsOf(job) && completionsOf(job) > 0 {
		return false
	}
	for _, c := range job.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == corev1.ConditionTrue {
			return false
		}
	}
	return true
}

func completionsOf(job *batchv1.Job) int32 {
	if job.Spec.Completions == nil {
		return 1
	}
	return *job.Spec.Completions
}

func converterJobName(exportName string) string {
	return "kgx-cv-" + exportName
}

func (a *App) deleteConverterJob(exportName string) {
	c, cancel := ctx()
	defer cancel()
	prop := metav1.DeletePropagationBackground
	err := a.Kube.BatchV1().Jobs(a.NS).Delete(c, converterJobName(exportName), metav1.DeleteOptions{PropagationPolicy: &prop})
	if err != nil && !errors.IsNotFound(err) {
		slogErr("delete converter", err)
	}
}
