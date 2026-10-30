# Implementation notes

These are the places v1 differs from `instruct/design.md` so the workload can run on the only reachable cluster (OpenShift 4.22, Mountpoint S3 CSI `s3.csi.aws.com` v2.8.0, EBS `gp3-csi` RWO).

## Volumes

- `kubegenx-data` is a static 750Gi RWX PV on the existing S3 bucket, prefix `kubegenx/`, reclaim Retain. `deploy.sh` creates it. The controller never creates PVs or PVCs. The pre-existing `s3-volume-pvc` is not modified.
- Mountpoint cannot fcntl-lock, mmap, or rename in place. Controller SQLite (WAL) and temp files live on a second install-time 10Gi RWO PVC `kubegenx-state` (`gp3-csi`), mounted at `/var/lib/kubegenx`.
- TSDB blocks are written with Prometheus `BlockWriter` on an `emptyDir` (`/scratch`, `TMPDIR=/scratch`) and copied as complete files to `/data/exports/<dataset>/<export>/tsdb/<ULID>/`. Thanos store mounts that prefix read-only. Its index-header cache is an `emptyDir`.
- Startup still runs a flock self-test on `kubegenx-data`. When the lock or the directory create fails, the log records `scratchMode=always`. Export always uses the scratch path.
- Mount options add `allow-other`, `file-mode 0666`, and `dir-mode 0777` so the restricted-v2 UID can write through Mountpoint. `prefix kubegenx/` isolates this PV from the bucket's other claim.
- Mountpoint rejects `O_TRUNC` on an object that already exists (`operation not permitted`). Deleting the PV does not delete the bucket prefix. `hack/deploy.sh --clean-on-start` runs a tools pod that removes `/data/datasets` and `/data/exports`. A plain `hack/deploy.sh` leaves those directories in place.

## Generation

- Sample values are an integer Philox kernel shared by the Go tests and the Rust worker. Counters are a running sum of that kernel, so chunk files stitch exactly and a `life_seq` change resets the counter. The design's float AR water-fill and `chunk_offsets.parquet` are not written; Kruize and the TSDB writer read the cumulative Parquet columns directly.
- GPU workloads are `round(ratio * n)` with a minimum of 1 when the template enables GPUs. The e2e ratio `1/15` yields one GPU workload.
- Plan topology is `datasets/<name>/plan/generation.json` (plus `manifest.json`). Workers do not read plan Parquet tables.
- Kruize session aggregation is in-process Go over `parquet-go`, not DuckDB. That keeps the controller image static and multi-arch.

## Runtime

- Lease TTL is 10 minutes. Heartbeats every 15 seconds extend every item leased to that worker. The design's 60 second TTL requeued slow items on this cluster.
- One Thanos Deployment per export (store + query, no query-frontend). Store requests 500m CPU / 1Gi memory. Upstream image `quay.io/thanos/thanos:v0.42.4`. `v0.39.2` exits on this kernel during query startup (`lfstack.push invalid packing`). Query gRPC listens on `127.0.0.1:10911` so it does not collide with the store's `:10901`. `v0.42` rejects `--query.auto-downsampling=false`; the export has only raw blocks, so the default does not change the results.
- Cleanup of dataset and export files is `os.RemoveAll` in the controller. The `tools` image is still built and published for `rm`, `du`, and `selftest`.
- The controller Route sets `haproxy.router.openshift.io/timeout=180s`. A Kruize results page reads the dataset Parquet through Mountpoint, and the default 30s router timeout returns 504 before that read finishes.
- Worker and controller container user IDs are the namespace SCC range injected at deploy time. Images themselves are built as uid 65532.
