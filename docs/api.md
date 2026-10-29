# Scripting with the API

The UI is a client of these routes. A script calls the same host, the controller route `kubegenx` in namespace `kube-gen-x`. Nothing in front of the API checks a token.

```bash
BASE=https://$(oc get route kubegenx -n kube-gen-x -o jsonpath='{.spec.host}')
curl -sS "$BASE/api/v1/health"
# {"ok":true}
```

Errors are JSON, `{"error":"..."}`, with an HTTP status. `400` is a bad body, `404` is a missing template, dataset, or export, `409` is a conflict (a second Thanos export, or serve while one is already running).

Names are lowercase DNS labels: letters, digits, and single hyphens, at most 40 characters, and they cannot start or end with a hyphen.

## End to end

```bash
BASE=https://$(oc get route kubegenx -n kube-gen-x -o jsonpath='{.spec.host}')
NAME=demo

# 1. Template. An empty spec stores the built-in default (15 days, 15s step,
#    3 namespaces, 5 workloads each, seed 424242).
curl -sS -X POST "$BASE/api/v1/templates" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"$NAME\",\"spec\":{}}"

# 2. Dataset. This starts generation.
curl -sS -X POST "$BASE/api/v1/datasets" \
  -H 'Content-Type: application/json' \
  -d "{\"name\":\"$NAME\",\"template\":\"$NAME\",\"epoch\":\"2026-10-05T00:00:00Z\",\"effort\":\"high\",\"workers\":{\"cpu\":\"500m\",\"memory\":\"1Gi\",\"threads\":2}}"

# 3. Wait until generation is Ready. See the next section.

# 4. Thanos window, then serve it.
curl -sS -X POST "$BASE/api/v1/exports" \
  -H 'Content-Type: application/json' \
  -d "{\"dataset\":\"$NAME\",\"type\":\"thanos\",\"anchor\":\"start\",\"time\":\"2026-10-05T00:00:00Z\",\"effort\":\"xhigh\"}"
curl -sS -X POST "$BASE/api/v1/exports/$NAME/serve"

# 5. Wait until phase is Serving, then read .url. See "Thanos URL".
```

`effort` is `low` (5%), `medium` (15%), `high` (40%), or `xhigh` (80%) of the free CPU and memory that can hold a worker. Exports default to `xhigh` when the field is omitted. An empty dataset effort is treated as `xhigh`.

## When generation is complete

`POST /api/v1/datasets` returns `201` as soon as the ConfigMap exists. Workers start after that. Poll either of these; they return the same document:

```bash
curl -sS "$BASE/api/v1/datasets/$NAME"
curl -sS "$BASE/api/v1/datasets/$NAME/progress"
```

```json
{
  "name": "demo",
  "phase": "Generating",
  "percent": 42.5,
  "items": { "total": 80, "done": 34, "failed": 0, "leased": 4 },
  "bytes": 1048576,
  "startedAt": 1760000000,
  "finishedAt": 0,
  "cells": [{ "chunk": 0, "state": "done", "wlLo": 0, "wlHi": 1024 }],
  "kpi": { "percent": 42.5, "itemsDone": 34, "itemsTotal": 80, "itemsLeased": 4, "itemsFailed": 0, "bytes": 1048576 }
}
```

`startedAt` and `finishedAt` are Unix seconds. `finishedAt` is present once the run has stopped.

Generation is complete when `phase` is `Ready`. That means every work item is done, none failed, and the Parquet check passed. `percent` is then `100` and `finishedAt` is set.

| `phase` | Meaning |
|---|---|
| `Queued` | An export is converting. Generation starts when that export finishes. |
| `Generating` | Workers are writing Parquet. `items.done` climbs toward `items.total`. `items.leased` is in flight. |
| `Ready` | Done. Safe to create an export. |
| `Failed` | Planning failed, a worker failed, or the file check failed. `items.failed` is non-zero when workers failed. |

The progress JSON does not include the failure string. That string is on the status ConfigMap:

```bash
oc get cm kgx-ds-$NAME-status -n kube-gen-x -o jsonpath='{.data.status\.json}'
```

A shell wait:

```bash
while true; do
  phase=$(curl -fsS "$BASE/api/v1/datasets/$NAME" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("phase",""))')
  echo "$phase"
  case "$phase" in
    Ready) break ;;
    Failed) echo "generation failed" >&2; exit 1 ;;
  esac
  sleep 5
done
```

`GET /api/v1/datasets` lists every dataset with `phase`, `progress`, and, once the plan exists, `namespaces`, `workloads`, `samples`, `entries`, `days`, and `stepSeconds`.

After `Ready`, two more reads describe what was generated:

```bash
curl -sS "$BASE/api/v1/datasets/$NAME/layout"
curl -sS "$BASE/api/v1/datasets/$NAME/workloads?offset=0&limit=50"
curl -sS "$BASE/api/v1/datasets/$NAME/orgs"
curl -sS "$BASE/api/v1/datasets/$NAME/verify"
```

`GET /api/v1/datasets/{name}/orgs` lists the `org_id` values that appear in the plan and, under each one, the `cluster_id` values that belong to it, with namespace and workload counts. A cluster belongs to one org. The catalog behind the names has 100 orgs spelled the way the company is pronounced (`amazan`, `googal`, `pacebuk`) and 10000 region-style clusters (`us-east-1`, `eu-west-12`); a dataset uses a small skewed subset chosen from its workload count.

`layout` has `chunks`, `chunkSeconds` (21600, six hours), `stepSeconds`, `epoch`, `workloads`, and `namespaces`. `verify` returns `200` with `files`, `expected`, `rows`, and `resets`. A mismatch is `409`.

There is also `GET /api/v1/ws?topics=dataset:<name>`, the WebSocket the status page uses. Each message is the same progress document.

## Create the template you actually want

`spec` is the scenario. Omit it, or send `{}`, and the controller stores the built-in default: 15 days at a 15-second step, 3 namespaces, 5 workloads each. The Create page in the UI sends a full spec and a shorter duration. A script that should match a UI dataset sends the same fields. The shape is `controller/internal/spec/spec.go` (`seed`, `topology`, `profiles`, `gpu`, `nodes`, `time`, `features`, `workloadLabels`, `workers`, `workItem`).

`time.step` is a Go duration (`15s`). `time.duration` accepts Go durations and `Nd` for days (`1d`, `15d`). `time.chunk` is `6h` in every path the controller plans today.

One template can back many datasets. `POST /api/v1/datasets` copies the template into the dataset, so later edits to the template do not change a dataset that already exists.

`POST /api/v1/datasets` body:

| Field | Required | Meaning |
|---|---|---|
| `name` | yes | Dataset name. |
| `template` | yes | Template name. `404` if it is missing. |
| `epoch` | no | RFC3339. Empty becomes the next UTC midnight, minus the dataset duration, so the current UTC day is included. A 15 day dataset created on 29 October starts at 15 October 00:00 UTC and ends at 30 October 00:00 UTC. |
| `effort` | no | `low`, `medium`, `high`, `xhigh`. |
| `workers.cpu`, `workers.memory`, `workers.threads` | no | Request size and threads. The controller still chooses `workers.count` from effort and free capacity. |

Response `201`: `{"name":"demo"}`.

## Thanos URL

Create the export only after `phase` is `Ready`. The plan has to be on disk.

```bash
curl -sS -X POST "$BASE/api/v1/exports" \
  -H 'Content-Type: application/json' \
  -d '{"dataset":"demo","type":"thanos","anchor":"start","time":"2026-10-05T00:00:00Z","effort":"xhigh"}'
```

| Field | Meaning |
|---|---|
| `dataset` | Dataset name. |
| `type` | `thanos` or `generic`. Empty means `thanos`. |
| `anchor` | `start` or `end`. Empty means `start`. |
| `time` | RFC3339 instant for that edge. The other edge is one dataset duration away. |
| `effort` | Conversion workers. Empty means `xhigh`. |
| `interval` | Required for `generic`. Leave empty for Thanos. |

A dataset can have one Thanos export. The export name is the dataset name. A second one returns `409` with `dataset already has a Thanos export`.

Response `201`:

```json
{"name":"demo","sessionId":"","startTime":"2026-10-05T00:00:00Z","endTime":"2026-10-06T00:00:00Z","type":"thanos"}
```

That response does not start conversion. Serve does:

```bash
curl -sS -X POST "$BASE/api/v1/exports/demo/serve"
# {"name":"demo","phase":"Queued"}
```

`202` means it is queued. `409` means it is already `Queued`, `Building`, or `Serving`.

Poll until it is being served:

```bash
curl -sS "$BASE/api/v1/exports/demo"
```

```json
{
  "name": "demo",
  "dataset": "demo",
  "phase": "Serving",
  "type": "thanos",
  "startTime": "2026-10-05T00:00:00Z",
  "endTime": "2026-10-06T00:00:00Z",
  "blocks": 12,
  "blocksTotal": 12,
  "bytes": 8388608,
  "url": "https://kgx-ex-demo-query-kube-gen-x.apps.example.com/graph?g0.expr=...",
  "startedAt": 1760001000,
  "finishedAt": 1760001300
}
```

| `phase` | Meaning |
|---|---|
| `Configured` | Saved. Call serve. |
| `Queued` | Waiting for generation, or for the single conversion slot. |
| `Building` | `blocks` / `blocksTotal` is the progress. `workers` is how many converters are running. |
| `Serving` | `url` is set. |
| `Failed` | Stop. `kgx-ex-<name>-status` in `kube-gen-x` has `error` inside `status.json`. Serve again after you have looked at it. |

The URL to keep is the origin of `url`. The path is a Thanos Query graph link with a sample expression and the export window filled in. PromQL uses that same host:

```bash
ORIGIN=$(curl -fsS "$BASE/api/v1/exports/demo" | python3 -c 'import json,sys; from urllib.parse import urlparse; u=urlparse(json.load(sys.stdin)["url"]); print(u.scheme+"://"+u.netloc)')

curl -sS -G "$ORIGIN/api/v1/query" \
  --data-urlencode 'query=count by (namespace) (kube_pod_info)'

curl -sS -G "$ORIGIN/api/v1/query_range" \
  --data-urlencode 'query=sum by (namespace) (rate(container_cpu_usage_seconds_total[5m]))' \
  --data-urlencode 'start=2026-10-05T00:00:00Z' \
  --data-urlencode 'end=2026-10-06T00:00:00Z' \
  --data-urlencode 'step=60s'
```

`url` stays empty through `Building`. A `Serving` row whose `url` is still empty means the route host has not been written yet; poll again.

`GET /api/v1/exports` lists every export, Thanos and aggregation, in the same shape.

## Aggregation API URLs

```bash
curl -sS -X POST "$BASE/api/v1/exports" \
  -H 'Content-Type: application/json' \
  -d '{"dataset":"demo","type":"generic","anchor":"end","time":"2026-10-06T00:00:00Z","interval":"15m"}'
```

`interval` is a whole number of `m`, `h`, or `d`. It must be longer than the dataset step and an exact multiple of that step. The server assigns the name. Response `201`:

```json
{"name":"g3k1p0q9abcd","sessionId":"g3k1p0q9abcd","startTime":"...","endTime":"...","type":"generic"}
```

Use `sessionId` for every later call. Then serve and wait for `Serving`, the same way as Thanos:

```bash
SID=g3k1p0q9abcd
curl -sS -X POST "$BASE/api/v1/exports/$SID/serve"
curl -sS "$BASE/api/v1/exports/$SID"
```

There is no separate host. The URLs are on `$BASE`, and they answer only while `phase` is `Serving` (`409` with `session is not serving` before that):

```bash
curl -sS "$BASE/api/v1/datasets/demo/sessions/$SID/namespaces"
# {"sessionId":"...","namespaces":["ns-0","ns-1"]}

curl -sS "$BASE/api/v1/datasets/demo/sessions/$SID/namespaces/ns-0/workloads"
# {"sessionId":"...","namespace":"ns-0","workloads":[{"name":"...","kind":"Deployment","replicas":2,"containers":["app"]}]}

curl -sS -G "$BASE/api/v1/datasets/demo/sessions/$SID/results" \
  --data-urlencode 'namespace=ns-0' \
  --data-urlencode 'workload=deploy-0' \
  --data-urlencode 'start=2026-10-05T00:00:00Z' \
  --data-urlencode 'end=2026-10-05T06:00:00Z' \
  --data-urlencode 'page=1'
```

`results` query parameters:

| Parameter | Meaning |
|---|---|
| `namespace` | Optional filter. |
| `workload` | Optional filter. |
| `start`, `end` | RFC3339. Both must sit inside the session window. Omit them to take the whole window. |
| `page` | 1-based. Each page holds 200 buckets. |

Each item is one container over one interval:

```json
{
  "namespace": "ns-0",
  "kind": "Deployment",
  "workload": "deploy-0",
  "replicas": 2,
  "container": "app",
  "start": "2026-10-05T00:00:00Z",
  "end": "2026-10-05T00:15:00Z",
  "metrics": {
    "cpuUsage":    {"min": 0.01, "max": 0.4, "avg": 0.1, "sum": 0.2, "unit": "cores"},
    "cpuThrottle": {"min": 0, "max": 0, "avg": 0, "sum": 0, "unit": "cores"},
    "memoryUsage": {"min": 1000000, "max": 2000000, "avg": 1500000, "sum": 3000000, "unit": "bytes"},
    "memoryRSS":   {"min": 0, "max": 0, "avg": 0, "sum": 0, "unit": "bytes"}
  }
}
```

`cpuRequest`, `cpuLimit`, `memoryRequest`, and `memoryLimit` appear when the plan set them. A trailing partial interval is dropped.

The controller deletes a `generic` export 24 hours after `createdAt`. `createdAt` on `GET /api/v1/exports/$SID` is Unix seconds, so the session disappears at `createdAt + 86400`.

## Other calls

| Call | Effect |
|---|---|
| `GET /api/v1/templates` | `{"items":[{"name":"..."}]}` |
| `GET /api/v1/storage` | `capacityBytes`, `usedBytes`, per-dataset `bytes` and `reserved` |
| `DELETE /api/v1/datasets/{name}` | `202`. Removes the dataset and its files. |
| `DELETE /api/v1/exports/{name}` | `202`. For Thanos, also removes the query route. The dataset stays. |
| `DELETE /api/v1/datasets/{name}/parquet` | Removes samples. `409` with `KruizeSessionsDependOnParquet` while a live session still needs them. |

Generation and conversion take turns. A second dataset stays `Queued` while any export is `Queued` or `Building`. An export stays `Queued` while any dataset is `Generating`.
