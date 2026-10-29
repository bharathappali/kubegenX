# Using the UI

The UI is served by the controller. Find the route and open it:

```bash
oc get route kubegenx -n kube-gen-x
```

The address bar is a hash route. `#/wizard` is Create, `#/datasets` is the list, `#/status?name=<dataset>` is one dataset, and `#/storage` is the volume.

The chip in the header is the busiest thing on the cluster: `IDLE`, `QUEUED`, `GENERATING`, `BUILDING`, `FAILED`, `SERVING`, or `READY`. The bell records when a dataset is queued behind an export and when it starts again.

## Create a dataset

Create asks for the scenario, then posts a template and a dataset with the same name.

| Field | What it sets |
|---|---|
| Name | Lowercase letters, digits, and single hyphens. At most 40 characters. The shuffle button picks a fresh name. |
| Shape | **By namespace** fixes the namespace count and the workloads in each. **By workloads** fixes the total and spreads them unevenly. |
| GPU workloads | Percent of workloads that get a GPU. `0` turns GPUs off. Any positive percent plans at least one GPU workload. |
| Days | Length of the history. The samples are step indexes; the calendar is chosen later, at export time. |
| Data interval | `15s`, `30s`, `45s`, or `60s`. |
| Effort | How much free CPU and memory the workers may use. XHigh is 80%, High 40%, Medium 15%, Low 5%, with at least two workers. A generation worker asks for 500m CPU and 1Gi memory and has no limit. |

The rest of the scenario is fixed by the form: seed `424242`, about 80% Deployments and 20% StatefulSets, mostly 1–3 replicas, and a mix of idle, low, medium, and high usage. The epoch stored with the dataset is the next UTC midnight, minus the number of days, so the current UTC day is included. A 15 day dataset created on 29 October starts at 15 October 00:00 UTC and ends at 30 October 00:00 UTC.

**Create dataset** sends you to that dataset’s status page. Generation starts immediately unless an export is already converting, in which case the page says the dataset is queued.

## Datasets

The list refreshes every few seconds. Each card shows the phase, whether Thanos or the aggregation API is active, namespace and workload counts, sample and entry counts, duration, and interval. The bar is generation percent.

Open a card for the status page. The trash icon deletes the dataset and the files it generated. Grid and table are the same rows.

A phase of `Queued` means generation is waiting for a conversion to finish. `Generating` means workers are writing Parquet. `Ready` means generation finished and the file check passed. `Failed` means planning, a worker, or that check stopped the run.

## Status, and when generation is done

`#/status?name=<dataset>` is the live view. It follows a WebSocket and falls back to polling. The chip on the page reads `Live` or `Polling`.

Generation is complete when all of these are true:

- The Progress card says `100.0%` and its hint is `Ready`.
- Chunks reads `<total>/<total>` and the hint is no longer counting leased or failed chunks.
- The Creation card has stopped counting and its hint is `done`.
- The progress bar is full and still.

`Ready` is the phase to wait for. `100%` with a hint of `Generating` means the last chunks are still being checked. `Failed` means stop and look at the controller; the HTTP body for this page does not include the error string. With cluster access:

```bash
oc get cm kgx-ds-<name>-status -n kube-gen-x -o jsonpath='{.data.status\.json}'
```

While it runs, the Days view is the 6-hour chunks. Mint is done, butter is being written, slate is waiting, pink failed. Namespace and Workload switch the same cells to those groupings. ETA is a rough remaining time from recent chunk completions, and the byte hint under it is Parquet written so far.

## Export and the URLs

The export button on the status page opens the rail. Nothing is served until you configure an export and press **Serve**. Configuring only records the window.

### Thanos

One Thanos export per dataset. Pick **Start** or **End**, then a time. The other edge is the full generated duration away, shown under the field in UTC. Effort here sizes conversion workers (2 CPU and 4Gi each, no limit), which is separate from the effort used to generate the dataset.

**Create** stores the export in phase `Configured`. **Serve** queues the conversion.

| Phase | Meaning |
|---|---|
| `Configured` | Window is saved. Serve is available. |
| `Queued` | Waiting. Generation, or another export, is using the cluster. |
| `Building` | Parquet is being written into 2-hour TSDB blocks. The bar is blocks done. |
| `Serving` | The route is up. |
| `Failed` | Conversion stopped. Serve is available again. The status ConfigMap `kgx-ex-<name>-status` holds `error`. |

When the phase is `Serving`, the rail shows:

- **Thanos**, the origin of the query route, for example `https://kgx-ex-demo-query-kube-gen-x.apps.example.com`.
- **Try these**, three PromQL links (pods, CPU, and memory by namespace) already aimed at the export window.

If the phase is `Serving` and the address line still says the route is not ready, wait a moment and let the rail refresh. The controller polls the route host before it records the URL.

Deleting the export removes the Thanos deployment and the blocks. The dataset stays.

### Aggregation API

A dataset can have many of these. The form adds an interval: a whole number of minutes, hours, or days, longer than the data interval and an exact multiple of it. A 15-second dataset can use `15m`, `1h`, or `1d`. It cannot use `10m` or `7s`.

**Create** returns a session id such as `g3k1p0q9abcd`. **Serve** builds the buckets. The card shows `Configured` until you serve, then `Queued`, `Building`, and `Serving`.

URLs appear on the card only after `Serving`. They are on the controller route, not a new host:

```
https://<controller>/api/v1/datasets/<dataset>/sessions/<id>/namespaces
https://<controller>/api/v1/datasets/<dataset>/sessions/<id>/results?page=1
https://<controller>/api/v1/datasets/<dataset>/sessions/<id>/namespaces/<namespace>/workloads
```

The card lists the first three namespace workload URLs. The session is deleted 24 hours after it was created. The card prints that time in UTC. Deleting one session leaves the others and the dataset.

## Storage

Storage shows `kubegenx-data`: bytes used, the capacity, and each dataset’s phase, bytes on disk, and bytes reserved for the plan. The volume stays empty until a dataset is created.
