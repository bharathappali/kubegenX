# KubeGenX

KubeGenX builds the usage history a real Kubernetes cluster would leave in Prometheus, then lets you place that history on any calendar.

Recommendation tools, recording rules, and PromQL dashboards need weeks of container CPU, memory, kube-state-metrics, and GPU series before they have anything to decide on. Collecting that from a live cluster means running the cluster, waiting, and accepting whatever happened to be scheduled. KubeGenX produces the same shape of data from a seed, once, and keeps the expensive files. The clock is chosen later, when you serve.

## Generate once, serve on a clock

A dataset is a cluster scenario: namespaces, workloads, replica mix, usage bands, GPU share, labels, and a seed. The same template and seed reproduce the same cluster. Workers write samples as step indexes from the dataset epoch, stored as Parquet on a shared volume. A sample is `f(seed, container, metric, chunk, step)`, so any chunk can be generated first, a retry writes the identical file, and the timestamp is applied only by whoever is serving.

Two doors read those files:

- **Thanos.** One export per dataset. The controller stamps the samples onto a start or end time you choose, writes Prometheus TSDB blocks, and stands up a Thanos store and querier on their own route. Point Kruize local monitoring, Grafana, or any PromQL client at that route.
- **Aggregation API.** Many sessions per dataset. Each session is the same samples rolled into fixed intervals (15 minutes, 1 hour, 1 day) and served as JSON pages of min, max, avg, and sum. A session is removed 24 hours after it is created.

Generation and conversion share the cluster. A dataset stays queued while an export is building, and an export stays queued while samples are still being written.

## What a query finds

Thanos serves the series a live kubelet, kube-state-metrics, and DCGM endpoint would:

- `container_cpu_usage_seconds_total` and `container_cpu_cfs_throttled_seconds_total`
- `container_memory_working_set_bytes` and `container_memory_rss`
- OpenShift-style recorded CPU rates and memory gauges
- `kube_pod_info`, owners, phases, requests, limits, and restarts, expanded from the plan while the blocks are written
- `DCGM_FI_DEV_GPU_UTIL`, `DCGM_FI_DEV_MEM_COPY_UTIL`, and `DCGM_FI_DEV_FB_USED` for workloads planned with a GPU

The aggregation API returns the same CPU and memory signals per container, already reduced to each interval.

## Where it runs

One OpenShift namespace, `kube-gen-x`. A Go controller plans the cluster, leases work, and serves the UI and the HTTP API. Rust workers write the Parquet. Templates, datasets, and exports are ConfigMaps in that namespace. The UI is the controller route `kubegenx`.

```bash
oc get route kubegenx -n kube-gen-x
```

Open `https://<host>`. The same host is the API base in the guides below.

## Guides

- [Using the UI](docs/ui.md) covers creating a dataset, reading progress, and copying serve URLs.
- [Scripting with the API](docs/api.md) covers the same flow from curl, including how to tell that generation is finished and where the URLs are.
- [Adding a metric](docs/adding-a-metric.md) lists every place a new series has to land, from the sample kernel through Parquet, Thanos, and the aggregation API.
- [Implementation notes](docs/IMPLEMENTATION_NOTES.md) records how this install differs from the original design on the current cluster.
- [Design walkthrough](docs/workflow.html) is the picture of “generate once, move the calendar.” A few serving details there are ahead of the code: blocks are 2 hours, a dataset has one Thanos export, and aggregation sessions are precomputed JSON with a 24-hour life.
