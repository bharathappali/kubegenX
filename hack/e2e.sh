#!/usr/bin/env bash
# Acceptance checks for the small 3x5, 1 GPU, 1 day dataset. Any miss fails the script.
set -euo pipefail
export KUBECONFIG="${KUBECONFIG:-$HOME/.kube/config}"
NS="kube-gen-x"
HOST="$(oc get route kubegenx -n "$NS" -o jsonpath='{.spec.host}')"
BASE="https://${HOST}"
CURL=(curl -kfsS --retry 5 --retry-delay 2 --retry-all-errors)

echo "route ${BASE}"
"${CURL[@]}" "$BASE/" | grep -q KubeGenX
"${CURL[@]}" "$BASE/api/v1/health" | grep -q '"ok":true'

python3 - "$BASE" <<'PY'
import json, os, ssl, sys, urllib.request
base = sys.argv[1]
ctx = ssl._create_unverified_context()
spec = {
  "seed": 424242,
  "topology": {
    "mode": "NamespaceMatrix",
    "totalWorkloads": 15,
    "namespaces": {"count": 3, "distribution": "Uniform"},
    "workloadsPerNamespace": 5,
    "kinds": {"Deployment": 0.8, "StatefulSet": 0.2},
    "replicas": {
      "buckets": [{"range": [1, 3], "share": 70}, {"range": [4, 7], "share": 22}, {"range": [8, 10], "share": 8}],
      "statefulSetBuckets": [{"range": [1, 3], "share": 85}, {"range": [4, 7], "share": 15}],
    },
    "containersPerPod": {"1": 0.75, "2": 0.18, "3": 0.07},
  },
  "profiles": {
    "cpu": {"idle": 0.15, "low": 0.40, "medium": 0.30, "high": 0.15},
    "memory": {"low": 0.35, "medium": 0.45, "high": 0.20},
    "sizing": {"rightSized": 0.45, "over": 0.30, "under": 0.10, "noLimits": 0.10, "bestEffort": 0.05},
    "behaviours": {"steady": 0.30, "diurnal": 0.40, "bursty": 0.15, "batchOnOff": 0.10, "idleWindows": 0.05},
    "anomalies": {"throttleAggressive": 0.08, "memoryLeak": 0.03, "oomLoop": 0.01, "spikes": 0.10},
  },
  "gpu": {"enabled": True, "workloadRatio": 1/15, "labelProfile": "kruize", "migRatio": 0.3, "nodePools": ["p4d.24xlarge"]},
  "nodes": {"pools": [{"instanceType": "m7i.4xlarge", "weight": 1}], "maxPodsPerNode": 250},
  "time": {"step": "15s", "duration": "1d", "chunk": "6h", "epoch": "2026-10-05T00:00:00Z"},
  "features": ["cpu", "memory", "gpu", "ksm"],
  "workloadLabels": {
    "scope": "namespace",
    "keys": [
      {"key": "org_id", "values": [{"value": "acme-retail", "weight": 3}, {"value": "globex", "weight": 1}]},
      {"key": "cluster_id", "values": [{"value": "ocp-east-1", "weight": 2}, {"value": "ocp-west-2", "weight": 1}]},
    ],
    "carryOn": ["namespace", "ksm-pod", "ksm-container", "recorded", "cadvisor"],
  },
  "workers": {"count": 2, "cpu": "500m", "memory": "1Gi", "threads": 2},
  "workItem": {"workloadsPerItem": 1024},
}
def post(path, body):
    req = urllib.request.Request(base + path, data=json.dumps(body).encode(), headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, context=ctx) as res:
        print(path, res.status)
        return json.load(res)
post("/api/v1/templates", {"name": "e2e", "spec": spec})
post("/api/v1/datasets", {"name": "e2e", "template": "e2e", "epoch": "2026-10-05T00:00:00Z", "workers": spec["workers"]})
PY

echo "waiting for generation"
ready=0
for _ in $(seq 1 180); do
  body="$("${CURL[@]}" "$BASE/api/v1/datasets/e2e/progress" || true)"
  phase="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1]).get("phase",""))' "$body" 2>/dev/null || true)"
  echo "phase=${phase}"
  if [[ "$phase" == "Ready" ]]; then ready=1; break; fi
  if [[ "$phase" == "Failed" ]]; then
    echo "$body"
    oc logs deployment/kubegenx-controller -n "$NS" --tail=80 || true
    oc get pods -n "$NS" -o wide || true
    exit 1
  fi
  sleep 10
done
[[ "$ready" == 1 ]]
python3 -c 'import json,sys; d=json.loads(sys.argv[1]); assert d.get("percent",0)==100' "$body"

"${CURL[@]}" "$BASE/api/v1/datasets/e2e/verify" >/tmp/kubegenx-verify.json
python3 - <<'PY'
import json
d=json.load(open("/tmp/kubegenx-verify.json"))
assert d["files"]==d["expected"] and d["files"]>0 and d["rows"]>0, d
print("verify", d)
PY

echo "waiting for worker jobs to leave"
gone=0
for _ in $(seq 1 30); do
  if ! oc get jobs -n "$NS" -l app.kubernetes.io/part-of=kubegenx -o name | grep -q workers; then
    gone=1
    break
  fi
  sleep 4
done
[[ "$gone" == 1 ]]

python3 - "$BASE" <<'PY'
import base64, json, os, socket, ssl, sys
from urllib.parse import urlparse
base = sys.argv[1]
u = urlparse(base)
path = "/api/v1/ws?topics=dataset:e2e"
key = base64.b64encode(os.urandom(16)).decode()
raw = socket.create_connection((u.hostname, 443), 20)
ss = ssl._create_unverified_context().wrap_socket(raw, server_hostname=u.hostname)
req = (
    f"GET {path} HTTP/1.1\r\nHost: {u.hostname}\r\nUpgrade: websocket\r\nConnection: Upgrade\r\n"
    f"Sec-WebSocket-Key: {key}\r\nSec-WebSocket-Version: 13\r\n\r\n"
)
ss.sendall(req.encode())
buf = b""
while b"\r\n\r\n" not in buf:
    buf += ss.recv(4096)
head, rest = buf.split(b"\r\n\r\n", 1)
assert b"101" in head.split(b"\r\n", 1)[0], head
pending = rest
def take(n):
    global pending
    while len(pending) < n:
        pending += ss.recv(4096)
    out, pending = pending[:n], pending[n:]
    return out
hdr = take(2)
assert hdr, "no websocket frame"
ln = hdr[1] & 0x7F
if ln == 126:
    ln = int.from_bytes(take(2), "big")
payload = take(ln)
msg = json.loads(payload.decode())
assert msg.get("type") == "progress", msg
assert msg.get("kpi", {}).get("percent") == 100, msg
print("websocket", msg["phase"])
ss.close()
PY

used_before="$("${CURL[@]}" "$BASE/api/v1/storage")"
python3 -c 'import json,sys; d=json.loads(sys.argv[1]); assert any(x["name"]=="e2e" for x in d["datasets"]); assert d["usedBytes"]>0' "$used_before"

"${CURL[@]}" -X POST "$BASE/api/v1/exports" -H 'Content-Type: application/json' \
  -d '{"name":"e2e","dataset":"e2e","startTime":"2026-10-05T00:00:00Z"}' >/dev/null

echo "waiting for thanos export"
url=""
for _ in $(seq 1 120); do
  body="$("${CURL[@]}" "$BASE/api/v1/exports/e2e" || true)"
  phase="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1]).get("phase",""))' "$body" 2>/dev/null || true)"
  echo "export=${phase}"
  if [[ "$phase" == "Serving" ]]; then
    url="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["url"])' "$body")"
    break
  fi
  if [[ "$phase" == "Failed" ]]; then
    echo "$body"
    oc logs deployment/kubegenx-controller -n "$NS" --tail=120 || true
    exit 1
  fi
  sleep 10
done
[[ -n "$url" ]]
echo "thanos ${url}"

python3 - "$BASE" "$url" <<'PY'
import json, ssl, sys, time, urllib.parse, urllib.request
base, thanos = sys.argv[1:]
ctx = ssl._create_unverified_context()

def get(url):
    last = None
    for _ in range(4):
        try:
            with urllib.request.urlopen(url, context=ctx, timeout=180) as res:
                return json.load(res)
        except urllib.error.HTTPError as exc:
            last = exc
            if exc.code not in (502, 503, 504):
                raise
            time.sleep(8)
    raise last

sess = get_json = None
req = urllib.request.Request(
    base + "/api/v1/datasets/e2e/exporters/kruize/sessions",
    data=json.dumps({"name": "e2e", "endTime": "2026-10-06T00:00:00Z"}).encode(),
    headers={"Content-Type": "application/json"},
    method="POST",
)
with urllib.request.urlopen(req, context=ctx) as res:
    sess = json.load(res)
assert sess["totals"]["experiments"] == 15, sess
sid = sess["sessionId"]
exps = get(f"{base}/api/v1/datasets/e2e/exporters/kruize/sessions/{sid}/experiments?page=1&pageSize=100")
names = []
for exp in exps["data"]:
    assert "labels" not in exp
    name = exp["experiment_name"]
    assert name.count("|") == 3 and name.startswith("e2e|")
    names.append(name)
assert len(names) == len(set(names)) == 15
parts = names[0].split("|")
ns, kind, wl = parts[1], parts[2], parts[3]
results = get(f"{base}/api/v1/datasets/e2e/exporters/kruize/sessions/{sid}/experiments/{urllib.parse.quote(names[0], safe='')}/results?page=0")
assert len(results["data"]) == 24, results["metadata"]
assert results["data"][0]["interval_start_time"][:19] == sess["effectiveRange"]["start"][:19], results["data"][0]
assert "labels" not in results["data"][0]
open("/tmp/kubegenx-session.txt","w").write(sid + "\n" + names[0] + "\n" + ns + "\n" + wl)

def prom(q, at=None):
    params = {"query": q}
    if at is not None:
        params["time"] = str(at)
    last = None
    for _ in range(60):
        url = thanos + "/api/v1/query?" + urllib.parse.urlencode(params)
        try:
            body = get(url)
        except Exception as exc:
            last = exc
            time.sleep(5)
            continue
        if body.get("status") == "success" and body["data"]["result"]:
            return body["data"]["result"]
        last = body
        time.sleep(5)
    raise SystemExit(f"empty query {q}: {last}")

start = 1759622400  # 2026-10-05T00:00:00Z — overwritten below
import datetime
start = int(datetime.datetime(2026, 10, 5, tzinfo=datetime.timezone.utc).timestamp())
# recording-rule trio
for metric in (
    "node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate",
    "node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate5m",
    "node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate",
):
    rows = prom(f'{metric}{{namespace="{ns}"}}', start + 60)
    assert rows, metric
same = prom(
    f'(node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate{{namespace="{ns}"}} == node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate5m{{namespace="{ns}"}})',
    start + 300,
)
assert same
# no-recording-rule raw rate plus metadata join
raw = prom(f'sum by (namespace,pod,container) (rate(container_cpu_usage_seconds_total{{namespace="{ns}",container!=""}}[5m]))', start + 300)
assert raw
meta = prom(f'namespace_workload_pod:kube_pod_owner:relabel{{namespace="{ns}",workload="{wl}"}} and on (namespace,pod) kube_pod_container_info{{namespace="{ns}"}}', start + 60)
assert meta
info = prom(f'kube_pod_info{{namespace="{ns}"}}', start + 60)
assert info
# DCGM for the GPU namespace: find a series, then confirm the GPU workload's namespace returns rows
gpu = prom('DCGM_FI_DEV_GPU_UTIL{job="nvidia-dcgm-exporter"}', start + 60)
assert gpu
gns = gpu[0]["metric"]["exported_namespace"]
assert prom(f'DCGM_FI_DEV_GPU_UTIL{{exported_namespace="{gns}",job="nvidia-dcgm-exporter"}}', start + 60)
assert prom(f'DCGM_FI_DEV_MEM_COPY_UTIL{{exported_namespace="{gns}"}}', start + 60)
assert prom(f'DCGM_FI_DEV_FB_USED{{exported_namespace="{gns}"}}', start + 60)
# shifted sample timestamp: step 1 is start+15s
q = urllib.parse.urlencode({
    "query": f'container_cpu_usage_seconds_total{{namespace="{ns}"}}',
    "start": str(start),
    "end": str(start + 45),
    "step": "15",
})
body = get(thanos + "/api/v1/query_range?" + q)
vals = body["data"]["result"][0]["values"]
ts = [int(v[0]) for v in vals]
assert start in ts and (start + 15) in ts, ts
print("thanos ok", ns, wl, "gpu_ns", gns)
PY

code="$(curl -k -s -o /tmp/kubegenx-del-export.txt -w '%{http_code}' -X DELETE "$BASE/api/v1/exports/e2e")"
[[ "$code" == "202" ]]

code="$(curl -k -s -o /tmp/kubegenx-del-parquet.txt -w '%{http_code}' -X DELETE "$BASE/api/v1/datasets/e2e/parquet")"
[[ "$code" == "409" ]]
grep -q KruizeSessionsDependOnParquet /tmp/kubegenx-del-parquet.txt

SID="$(head -1 /tmp/kubegenx-session.txt)"
"${CURL[@]}" -X DELETE "$BASE/api/v1/datasets/e2e/exporters/kruize/sessions/${SID}" >/dev/null
code="$(curl -k -s -o /tmp/kubegenx-del-parquet2.txt -w '%{http_code}' -X DELETE "$BASE/api/v1/datasets/e2e/parquet")"
[[ "$code" == "200" ]]

code="$(curl -k -s -o /dev/null -w '%{http_code}' -X DELETE "$BASE/api/v1/datasets/e2e")"
[[ "$code" == "202" ]]

echo "waiting for dataset cleanup"
BEFORE_USED="$(python3 -c 'import json,sys; print(json.loads(sys.argv[1])["usedBytes"])' "$used_before")"
dropped=0
for _ in $(seq 1 30); do
  body="$("${CURL[@]}" "$BASE/api/v1/storage")"
  if BEFORE_USED="$BEFORE_USED" BODY="$body" python3 - <<'PY'
import json, os, sys
d = json.loads(os.environ["BODY"])
names = [x["name"] for x in d.get("datasets") or []]
sys.exit(0 if "e2e" not in names and d["usedBytes"] < int(os.environ["BEFORE_USED"]) else 1)
PY
  then
    dropped=1
    break
  fi
  sleep 4
done
[[ "$dropped" == 1 ]]
echo "e2e passed"
