#!/usr/bin/env python3
"""Check that a KubeGenX Thanos export still has every metric on every generated day.

The KubeGenX base URL is the only required input. Datasets, Thanos URLs, and the
export window come from its API. Each metric is counted once per day with
count_over_time, which reads stored samples instead of downloading them.

  python3 test/thanos/validate.py --kubegenx https://kubegenx.example

KubeGenX and Thanos on the sandbox clusters use a certificate Python does not
trust. Pass --insecure for those.
"""

import argparse
import json
import os
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone

# Gauges expanded from the plan are written on every other step.
DOUBLE_STEP = {
    "kube_namespace_status_phase",
    "kube_pod_container_info",
    "kube_pod_container_resource_limits",
    "kube_pod_container_resource_requests",
    "kube_pod_container_status_ready",
    "kube_pod_container_status_restarts_total",
    "kube_pod_info",
    "kube_pod_owner",
    "kube_pod_status_phase",
    "kube_replicaset_owner",
    "namespace_workload_pod:kube_pod_owner:relabel",
    "node_namespace_pod:kube_pod_info:",
}

# Rate series start on the second sample of each 2h block.
RATE_METRICS = {
    "node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate",
    "node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate",
    "node_namespace_pod_container:container_cpu_usage_seconds_total:sum_rate5m",
}

BLOCK_SECONDS = 2 * 60 * 60


def main():
    parser = argparse.ArgumentParser(description="Validate Thanos samples for KubeGenX exports")
    parser.add_argument("--kubegenx", default=os.environ.get("KUBEGENX_URL", ""), help="KubeGenX base URL, or KUBEGENX_URL")
    parser.add_argument("--dataset", action="append", default=[], help="Limit to this dataset. Repeatable.")
    parser.add_argument("--insecure", action="store_true", help="Do not verify TLS certificates")
    parser.add_argument("--timeout", type=float, default=120, help="HTTP timeout in seconds")
    parser.add_argument("--retries", type=int, default=3, help="Attempts per query before recording a failure")
    args = parser.parse_args()
    if args.retries < 1:
        parser.error("--retries must be at least 1")
    if not args.kubegenx:
        parser.error("pass --kubegenx or set KUBEGENX_URL")

    client = Client(args.kubegenx.rstrip("/"), args.insecure, args.timeout)
    exports = thanos_exports(client, set(args.dataset))
    if not exports:
        print("no Serving Thanos exports found", file=sys.stderr)
        sys.exit(1)

    failed = False
    for exp in exports:
        rows = check_export(client, exp, args.retries)
        print_report(exp, rows)
        if any(r["missed"] not in ("0", 0) for r in rows):
            failed = True
    sys.exit(1 if failed else 0)


def thanos_exports(client, only):
    datasets = client.get_json("/api/v1/datasets").get("items") or []
    by_name = {}
    for item in datasets:
        name = item.get("name") or ""
        if only and name not in only:
            continue
        layout = client.get_json("/api/v1/datasets/" + urllib.parse.quote(name) + "/layout")
        by_name[name] = layout
    found = []
    for row in client.get_json("/api/v1/exports").get("items") or []:
        if (row.get("type") or "thanos") != "thanos":
            continue
        if row.get("phase") != "Serving":
            continue
        name = row.get("dataset") or ""
        if only and name not in only:
            continue
        layout = by_name.get(name)
        if layout is None:
            layout = client.get_json("/api/v1/datasets/" + urllib.parse.quote(name) + "/layout")
        url = origin(row.get("url") or "")
        if not url:
            print("export %s has no Thanos URL" % row.get("name"), file=sys.stderr)
            continue
        found.append({
            "dataset": name,
            "export": row.get("name") or name,
            "thanos": url,
            "start": parse_time(row.get("startTime")),
            "end": parse_time(row.get("endTime")),
            "epoch": parse_time(layout.get("epoch")),
            "step": int(layout.get("stepSeconds") or 15),
        })
    return found


def check_export(client, exp, retries):
    print("checking %s via %s" % (exp["export"], exp["thanos"]), file=sys.stderr)
    names = metric_names(client, exp["thanos"])
    days = list(day_slices(exp["start"], exp["end"]))
    rows = []
    for name in names:
        stride = exp["step"] * 2 if name in DOUBLE_STEP else exp["step"]
        daily = []
        for start, end in days:
            series, actual, err = sample_count(client, exp, name, start, end, retries)
            daily.append((start, end, series, actual, err))
            if err:
                print("  %s %s QUERY FAILED: %s" % (name, start.date(), err), file=sys.stderr)
            else:
                print("  %s %s series=%d samples=%d" % (name, start.date(), series, actual), file=sys.stderr)
        counted = [series for _, _, series, _, err in daily if err is None and series is not None]
        reference = max(counted, default=0)
        for start, end, series, actual, err in daily:
            if err or series is None or actual is None:
                rows.append({
                    "metric": name,
                    "day": start.date().isoformat(),
                    "series": "error" if series is None else str(series),
                    "expected": "-",
                    "actual": "error" if actual is None else str(actual),
                    "missed": "error",
                })
                continue
            expect = expected_samples(name, start, end, stride, reference)
            if reference == 0:
                missed = "absent"
            else:
                missed = str(max(0, expect - actual))
            rows.append({
                "metric": name,
                "day": start.date().isoformat(),
                "series": str(series),
                "expected": str(expect),
                "actual": str(actual),
                "missed": missed,
            })
    return rows


def metric_names(client, thanos):
    data = client.get_json(thanos + "/api/v1/label/__name__/values")
    found = set(data.get("data") or [])
    # Names the exporter always emits. A name absent from Thanos is still reported.
    found.update(DOUBLE_STEP)
    found.update(RATE_METRICS)
    found.update({
        "container_cpu_usage_seconds_total",
        "container_cpu_cfs_throttled_seconds_total",
        "container_memory_working_set_bytes",
        "container_memory_rss",
        "node_namespace_pod_container:container_memory_working_set_bytes",
        "node_namespace_pod_container:container_memory_rss",
        "DCGM_FI_DEV_GPU_UTIL",
        "DCGM_FI_DEV_MEM_COPY_UTIL",
        "DCGM_FI_DEV_FB_USED",
    })
    return sorted(found)


def sample_count(client, exp, name, start, end, retries):
    span = int((end - start).total_seconds())
    if span < 1:
        return 0, 0, None
    # Evaluate 1ms before the slice ends so the sample on the next boundary stays out.
    when = end.timestamp() - 0.001
    sel = '{__name__="%s",kubegenx_dataset="%s",kubegenx_export="%s"}[%ds]' % (
        name, exp["dataset"], exp["export"], span,
    )
    with ThreadPoolExecutor(max_workers=2) as pool:
        series_f = pool.submit(instant_value, client, exp["thanos"], "count(count_over_time(%s))" % sel, when, retries)
        actual_f = pool.submit(instant_value, client, exp["thanos"], "sum(count_over_time(%s))" % sel, when, retries)
        series, series_err = settle(series_f)
        actual, actual_err = settle(actual_f)
    err = series_err or actual_err
    if err is None and (series == 0) != (actual == 0):
        err = RuntimeError("series count %s and sample count %s disagree" % (series, actual))
    # A failed count is not a measured zero. Drop that side so the report cannot
    # treat it as missing samples.
    if err and series == 0:
        series = None
    if err and actual == 0:
        actual = None
    return series, actual, err


def settle(future):
    try:
        return future.result(), None
    except Exception as err:
        return None, err


def instant_value(client, thanos, query, when, retries):
    delay = 2
    last_err = None
    for attempt in range(1, retries + 1):
        try:
            value = query_once(client, thanos, query, when)
        except Exception as err:
            last_err = err
            if attempt == retries:
                break
            print("  query failed (%s), retry %d/%d" % (err, attempt, retries - 1), file=sys.stderr)
            time.sleep(delay)
            delay = min(delay * 2, 30)
            continue
        if value is not None:
            return value
        # A heavy count_over_time can come back empty while the samples are stored.
        # Retry that before accepting a real zero.
        if attempt == retries:
            return 0
        print("  query returned no series, retry %d/%d" % (attempt, retries - 1), file=sys.stderr)
        time.sleep(delay)
        delay = min(delay * 2, 30)
    raise RuntimeError("query failed after %d attempts: %s" % (retries, last_err))


def query_once(client, thanos, query, when):
    body = client.get_json(thanos + "/api/v1/query?" + urllib.parse.urlencode({
        "query": query,
        "time": "%.3f" % when,
    }))
    if body.get("status") != "success":
        raise RuntimeError(body.get("error") or body)
    warnings = body.get("warnings") or []
    if warnings:
        raise RuntimeError("; ".join(str(item) for item in warnings))
    result = body.get("data", {}).get("result") or []
    if not result:
        return None
    return int(float(result[0]["value"][1]))


def expected_samples(name, start, end, stride, series):
    if series <= 0 or stride <= 0:
        return 0
    slots = int((end - start).total_seconds() // stride)
    gaps = 0
    if name in RATE_METRICS:
        gaps = int((end - start).total_seconds() // BLOCK_SECONDS)
    per_series = max(0, slots - gaps)
    return series * per_series


def day_slices(start, end):
    cursor = start
    while cursor < end:
        nxt = datetime(cursor.year, cursor.month, cursor.day, tzinfo=timezone.utc) + timedelta(days=1)
        yield cursor, min(nxt, end)
        cursor = nxt


def print_report(exp, rows):
    print()
    print("dataset %s  export %s" % (exp["dataset"], exp["export"]))
    print("thanos %s" % exp["thanos"])
    print("export window %s .. %s   dataset epoch %s   step %ss" % (
        exp["start"].strftime("%Y-%m-%dT%H:%MZ"),
        exp["end"].strftime("%Y-%m-%dT%H:%MZ"),
        exp["epoch"].strftime("%Y-%m-%dT%H:%MZ"),
        exp["step"],
    ))
    headers = ("metric", "day", "series", "expected", "actual", "missed")
    table = [headers]
    for row in rows:
        table.append((row["metric"], row["day"], row["series"], row["expected"], row["actual"], row["missed"]))
    widths = [max(len(row[i]) for row in table) for i in range(len(headers))]
    for row in table:
        print("  ".join(cell.ljust(widths[i]) for i, cell in enumerate(row)))
    print()
    totals = {}
    for row in rows:
        bucket = totals.setdefault(row["metric"], {"missed": 0, "absent": False, "errors": 0})
        if row["missed"] == "error":
            bucket["errors"] += 1
        elif row["missed"] == "absent":
            bucket["absent"] = True
        else:
            bucket["missed"] += int(row["missed"])
    print("missed samples by metric")
    for name in sorted(totals):
        bucket = totals[name]
        if bucket["errors"]:
            note = "%d query failures" % bucket["errors"]
            if bucket["missed"]:
                note += ", %d missed on days that did answer" % bucket["missed"]
            print("  %-8s  %12s  %s  (%s)" % ("ERROR", "-", name, note))
        elif bucket["absent"]:
            print("  %-8s  %12s  %s" % ("ABSENT", "-", name))
        elif bucket["missed"] == 0:
            print("  %-8s  %12d  %s" % ("ok", 0, name))
        else:
            print("  %-8s  %12d  %s" % ("GAP", bucket["missed"], name))


def origin(url):
    parsed = urllib.parse.urlparse(url)
    if not parsed.scheme or not parsed.netloc:
        return ""
    return parsed.scheme + "://" + parsed.netloc


def parse_time(value):
    if not value:
        raise RuntimeError("missing timestamp")
    if value.endswith("Z"):
        value = value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


class Client:
    def __init__(self, base, insecure, timeout):
        self.base = base
        self.timeout = timeout
        self.ctx = ssl._create_unverified_context() if insecure else ssl.create_default_context()

    def get_json(self, path):
        url = path if path.startswith("http") else self.base + path
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        try:
            with urllib.request.urlopen(req, timeout=self.timeout, context=self.ctx) as res:
                return json.load(res)
        except urllib.error.HTTPError as err:
            detail = err.read().decode("utf-8", "replace")[:500]
            raise RuntimeError("%s %s" % (err.code, detail)) from err


if __name__ == "__main__":
    main()
