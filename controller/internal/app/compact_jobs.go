package app

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	exportpkg "github.com/bharathappali/kubegenx/internal/export"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func (a *App) volumeMode() string {
	if v := os.Getenv("KUBEGENX_VOLUME"); v != "" {
		return v
	}
	if a.Node != "" {
		return "node-local"
	}
	return "s3"
}

func (a *App) needsCompactScratch() bool {
	return a.volumeMode() != "node-local"
}

func (a *App) setCompactProgress(parent *corev1.ConfigMap, name, stage string, done, total int, msg string) {
	_ = a.Store.TouchExportCompact(name, stage, done, total, msg)
	row, _ := a.Store.GetExport(name)
	a.writeStatusThrottled(parent, name, map[string]any{
		"phase": "Compacting", "compactStage": stage, "compactDone": done, "compactTotal": total, "compactMessage": msg,
		"blocks": row.Blocks, "blocksTotal": row.BlocksTotal, "bytes": row.Bytes,
	})
}

func compactDayJobName(exportName, dayKey string) string {
	return "kgx-ex-" + exportName + "-cd-" + dayKey
}

func compactFinalJobName(exportName string) string {
	return "kgx-ex-" + exportName + "-cf"
}

func (a *App) runThanosCompact(parent *corev1.ConfigMap, exportName, dataset string) error {
	tsdb := filepath.Join(a.Data, "exports", dataset, exportName, "tsdb")
	if exportpkg.Compacted(tsdb) {
		slog.Info("thanos compact skip", "export", exportName, "reason", "already compacted")
		a.setCompactProgress(parent, exportName, "done", 1, 1, "already compacted")
		return nil
	}
	buckets, err := exportpkg.ListDayBuckets(tsdb)
	if err != nil {
		return err
	}
	if len(buckets) == 0 {
		return fmt.Errorf("no tsdb blocks to compact")
	}
	// Progress steps = one per day + final.
	totalSteps := len(buckets) + 1
	a.setCompactProgress(parent, exportName, "compacting-days", 0, totalSteps, fmt.Sprintf("scheduling %d day jobs (%s)", len(buckets), a.volumeMode()))

	c, cancel := context.WithTimeout(context.Background(), 6*time.Hour)
	defer cancel()
	a.deleteCompactJobs(exportName)

	objName := "kgx-ex-" + exportName
	dayNames := make([]string, 0, len(buckets))
	for _, b := range buckets {
		name := compactDayJobName(exportName, b.Key)
		dayNames = append(dayNames, name)
		if err := a.createCompactDayJob(c, parent, exportName, dataset, objName, name, b); err != nil {
			return err
		}
		slog.Info("thanos compact day start", "export", exportName, "day", b.Key, "job", name, "volume", a.volumeMode())
	}

	doneDays := 0
	for doneDays < len(dayNames) {
		doneDays = 0
		var fail error
		for i, name := range dayNames {
			st, err := a.jobTerminal(c, name)
			if err != nil {
				return err
			}
			switch st {
			case "complete":
				doneDays++
			case "failed":
				fail = fmt.Errorf("day compact job %s failed", name)
			case "running":
				a.setCompactProgress(parent, exportName, "compacting-days", doneDays, totalSteps,
					fmt.Sprintf("day %s running (%d/%d days done)", buckets[i].Key, doneDays, len(buckets)))
			}
		}
		if fail != nil {
			return fail
		}
		if doneDays == len(dayNames) {
			break
		}
		a.setCompactProgress(parent, exportName, "compacting-days", doneDays, totalSteps,
			fmt.Sprintf("%d/%d day jobs complete", doneDays, len(buckets)))
		select {
		case <-c.Done():
			return fmt.Errorf("day compact timed out")
		case <-time.After(10 * time.Second):
		}
	}

	// On S3/Mountpoint the final pass would re-stage the whole tree onto emptyDir
	// (~tens of GiB) and gets evicted for ephemeral-storage. Day vertical compact
	// already merged overlapping shards; skip the full re-stage and serve.
	if a.needsCompactScratch() {
		a.setCompactProgress(parent, exportName, "compacting-final", len(buckets), totalSteps, "skipping full final stage on S3 (ephemeral disk)")
		if err := exportpkg.MarkCompacted(tsdb); err != nil {
			return err
		}
		a.setCompactProgress(parent, exportName, "done", totalSteps, totalSteps, "day compaction complete")
		slog.Info("thanos compact done", "export", exportName, "final", "skipped-s3")
		return nil
	}

	a.setCompactProgress(parent, exportName, "compacting-final", len(buckets), totalSteps, "final leveled compaction")
	finalName := compactFinalJobName(exportName)
	if err := a.createCompactFinalJob(c, parent, exportName, dataset, objName, finalName); err != nil {
		return err
	}
	slog.Info("thanos compact final start", "export", exportName, "job", finalName)
	for {
		st, err := a.jobTerminal(c, finalName)
		if err != nil {
			return err
		}
		switch st {
		case "complete":
			_ = exportpkg.MarkCompacted(tsdb)
			a.setCompactProgress(parent, exportName, "done", totalSteps, totalSteps, "compaction complete")
			slog.Info("thanos compact done", "export", exportName)
			return nil
		case "failed":
			return fmt.Errorf("final compact job failed")
		}
		select {
		case <-c.Done():
			return fmt.Errorf("final compact timed out")
		case <-time.After(10 * time.Second):
		}
	}
}

func (a *App) jobTerminal(c context.Context, name string) (string, error) {
	got, err := a.Kube.BatchV1().Jobs(a.NS).Get(c, name, metav1.GetOptions{})
	if err != nil {
		return "", err
	}
	for _, cond := range got.Status.Conditions {
		if cond.Type == batchv1.JobComplete && cond.Status == corev1.ConditionTrue {
			return "complete", nil
		}
		if cond.Type == batchv1.JobFailed && cond.Status == corev1.ConditionTrue {
			return "failed", nil
		}
	}
	return "running", nil
}

func (a *App) deleteCompactJobs(exportName string) {
	c, cancel := ctx()
	defer cancel()
	prop := metav1.DeletePropagationBackground
	list, err := a.Kube.BatchV1().Jobs(a.NS).List(c, metav1.ListOptions{
		LabelSelector: "kubegenx.io/export=" + exportName + ",kubegenx.io/compact",
	})
	if err == nil {
		for _, j := range list.Items {
			_ = a.Kube.BatchV1().Jobs(a.NS).Delete(c, j.Name, metav1.DeleteOptions{PropagationPolicy: &prop})
		}
	}
	_ = a.Kube.BatchV1().Jobs(a.NS).Delete(c, "kgx-ex-"+exportName+"-compact", metav1.DeleteOptions{PropagationPolicy: &prop})
}

func (a *App) createCompactDayJob(c context.Context, parent *corev1.ConfigMap, exportName, dataset, objName, jobName string, day exportpkg.DayBucket) error {
	a.waitJobGone(c, jobName)
	minT := day.Start.UTC().Format(time.RFC3339)
	maxT := day.End.UTC().Format(time.RFC3339)
	labels := partLabels(map[string]string{
		"kubegenx.io/export": exportName, "kubegenx.io/compact": "day", "kubegenx.io/day": day.Key,
	})
	job := a.baseCompactJob(parent, jobName, labels)
	if a.needsCompactScratch() {
		job.Spec.Template.Spec.InitContainers = []corev1.Container{
			a.stageContainer("stage-in", "compact-sync-in-window", exportName, dataset, minT, maxT, true),
			a.thanosCompactContainer(objName+"-objstore-scratch", minT, maxT, true, day.Key),
		}
		job.Spec.Template.Spec.Containers = []corev1.Container{
			a.stageContainer("stage-out", "compact-sync-out-window", exportName, dataset, "", "", false),
		}
		job.Spec.Template.Spec.Volumes = a.compactScratchVolumes(objName)
	} else {
		job.Spec.Template.Spec.Containers = []corev1.Container{
			a.thanosCompactContainer(objName+"-objstore", minT, maxT, false, day.Key),
		}
		job.Spec.Template.Spec.Volumes = a.compactLocalVolumes(objName)
	}
	_, err := a.Kube.BatchV1().Jobs(a.NS).Create(c, job, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (a *App) createCompactFinalJob(c context.Context, parent *corev1.ConfigMap, exportName, dataset, objName, jobName string) error {
	a.waitJobGone(c, jobName)
	labels := partLabels(map[string]string{
		"kubegenx.io/export": exportName, "kubegenx.io/compact": "final",
	})
	job := a.baseCompactJob(parent, jobName, labels)
	if a.needsCompactScratch() {
		job.Spec.Template.Spec.InitContainers = []corev1.Container{
			a.stageContainer("stage-in", "compact-sync-in", exportName, dataset, "", "", true),
			a.thanosCompactContainer(objName+"-objstore-scratch", "", "", true, "final"),
		}
		job.Spec.Template.Spec.Containers = []corev1.Container{
			a.stageContainer("stage-out", "compact-sync-out", exportName, dataset, "", "", false),
		}
		job.Spec.Template.Spec.Volumes = a.compactScratchVolumes(objName)
	} else {
		job.Spec.Template.Spec.Containers = []corev1.Container{
			a.thanosCompactContainer(objName+"-objstore", "", "", false, "final"),
		}
		job.Spec.Template.Spec.Volumes = a.compactLocalVolumes(objName)
	}
	_, err := a.Kube.BatchV1().Jobs(a.NS).Create(c, job, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		return nil
	}
	return err
}

func (a *App) baseCompactJob(parent *corev1.ConfigMap, name string, labels map[string]string) *batchv1.Job {
	backoff := int32(1)
	ttl := int32(600)
	completions := int32(1)
	auto := false
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: a.NS, Labels: labels,
			OwnerReferences: []metav1.OwnerReference{ownerRef(parent)},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoff, Completions: &completions, TTLSecondsAfterFinished: &ttl,
			Template: corev1.PodTemplateSpec{
				ObjectMeta: metav1.ObjectMeta{Labels: labels},
				Spec: corev1.PodSpec{
					RestartPolicy:                corev1.RestartPolicyNever,
					ServiceAccountName:           "kubegenx-thanos",
					NodeSelector:                 a.nodeSelector(),
					AutomountServiceAccountToken: &auto,
					SecurityContext:              podSC(),
				},
			},
		},
	}
}

func (a *App) stageContainer(name, mode, exportName, dataset, minT, maxT string, readOnlyData bool) corev1.Container {
	env := []corev1.EnvVar{
		{Name: "KUBEGENX_MODE", Value: mode},
		{Name: "KUBEGENX_EXPORT", Value: exportName},
		{Name: "KUBEGENX_DATASET", Value: dataset},
		{Name: "KUBEGENX_DATA", Value: "/data"},
		{Name: "KUBEGENX_SCRATCH", Value: "/scratch"},
	}
	if minT != "" {
		env = append(env, corev1.EnvVar{Name: "KUBEGENX_MIN_TIME", Value: minT})
	}
	if maxT != "" {
		env = append(env, corev1.EnvVar{Name: "KUBEGENX_MAX_TIME", Value: maxT})
	}
	return corev1.Container{
		Name: name, Image: a.ConverterImage, ImagePullPolicy: corev1.PullAlways,
		Env: env, Resources: resReqLim("500m", "1Gi", "1", "2Gi"), SecurityContext: containerSC(),
		VolumeMounts: []corev1.VolumeMount{
			{Name: "data", MountPath: "/data", ReadOnly: readOnlyData},
			{Name: "scratch", MountPath: "/scratch"},
		},
	}
}

func (a *App) thanosCompactContainer(secretName, minT, maxT string, useScratch bool, workSuffix string) corev1.Container {
	// Parallel day jobs share the data PVC; each needs its own --data-dir or they
	// race creating the same compact/…/index paths ("no such file or directory").
	dataDir := "/data/compact-work"
	if useScratch {
		dataDir = "/scratch/compact-work"
	}
	if workSuffix != "" {
		dataDir = dataDir + "-" + workSuffix
	}
	_ = secretName // volume wiring selects the secret; name kept for call-site clarity.
	args := []string{
		"compact", "--log.level=info",
		"--data-dir=" + dataDir,
		"--objstore.config-file=/etc/thanos/objstore.yml",
		"--no-wait",
		"--downsampling.disable",
		"--retention.resolution-raw=0d",
		"--retention.resolution-5m=0d",
		"--retention.resolution-1h=0d",
		"--delete-delay=0s",
		"--consistency-delay=0s",
		"--compact.concurrency=1",
		"--compact.enable-vertical-compaction",
	}
	if minT != "" && maxT != "" {
		args = append(args, "--min-time="+minT, "--max-time="+maxT)
	}
	mounts := []corev1.VolumeMount{{Name: "objstore", MountPath: "/etc/thanos", ReadOnly: true}}
	if useScratch {
		mounts = append(mounts, corev1.VolumeMount{Name: "scratch", MountPath: "/scratch"})
	} else {
		mounts = append(mounts, corev1.VolumeMount{Name: "data", MountPath: "/data"})
	}
	return corev1.Container{
		Name: "compact", Image: a.ThanosImage, ImagePullPolicy: corev1.PullAlways,
		Args: args, Env: []corev1.EnvVar{{Name: "GOMAXPROCS", Value: "1"}},
		Resources: resReqLim("1", "8Gi", "1500m", "16Gi"), SecurityContext: containerSC(),
		VolumeMounts: mounts,
	}
}

func (a *App) compactScratchVolumes(objName string) []corev1.Volume {
	return []corev1.Volume{
		{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "kubegenx-data"}}},
		{Name: "scratch", VolumeSource: corev1.VolumeSource{EmptyDir: &corev1.EmptyDirVolumeSource{}}},
		{Name: "objstore", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: objName + "-objstore-scratch"}}},
	}
}

func (a *App) compactLocalVolumes(objName string) []corev1.Volume {
	return []corev1.Volume{
		{Name: "data", VolumeSource: corev1.VolumeSource{PersistentVolumeClaim: &corev1.PersistentVolumeClaimVolumeSource{ClaimName: "kubegenx-data"}}},
		{Name: "objstore", VolumeSource: corev1.VolumeSource{Secret: &corev1.SecretVolumeSource{SecretName: objName + "-objstore"}}},
	}
}

func (a *App) waitJobGone(c context.Context, name string) {
	prop := metav1.DeletePropagationBackground
	_ = a.Kube.BatchV1().Jobs(a.NS).Delete(c, name, metav1.DeleteOptions{PropagationPolicy: &prop})
	for i := 0; i < 60; i++ {
		if _, err := a.Kube.BatchV1().Jobs(a.NS).Get(c, name, metav1.GetOptions{}); errors.IsNotFound(err) {
			return
		}
		time.Sleep(time.Second)
	}
}
