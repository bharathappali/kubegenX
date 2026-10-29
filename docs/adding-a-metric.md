# Adding a metric

Samples are produced twice, on purpose, from the same integer kernel: once in Rust when a worker writes Parquet, and once in Go in the tests that lock the golden values. Thanos and the aggregation API never invent a number. They read those files, or they expand a value that already lives on the plan (requests, limits, pod owners, restarts).

A new metric is one of those two kinds. The file list is different.

## A value that varies over time

CPU, memory, and GPU are this kind. Each one is a Philox feature id, a Parquet column family, a reader, a TSDB series, and an aggregation field. CPU is feature `1`, memory is `2`, GPU is `3`. The next metric takes `4`. Reusing an id changes every existing series that shares it.

The kernel is `philox(seed, feature, entity, chunk, sample, lane)`. `entity` is the container id or the GPU id. `lane` selects a second stream inside the same feature, the way CPU usage and the memory draw use different lanes. Counters are a running sum of per-step increments, so a chunk file stitches to the next chunk and a new `life_seq` resets the counter. Gauges (memory bytes, GPU utilization) are the draw at that step, not a sum.

Touch these, in this order.

1. **Kernel, Go.** `controller/internal/generate/generate.go`. Add the feature constant and a function next to `CPUInc`, `MemAt`, or `GPUAt`. Keep it integer and deterministic. `controller/internal/generate/generate_test.go` locks a golden tuple the way `TestGolden` locks `CPUInc(424242, 7, 0, 0, 700, 500)`.

2. **Kernel, Rust.** `worker/src/gen.rs`. Copy the function. The comment at the top of that file is the contract: golden values match Go. `worker/src/philox.rs` is the shared Philox; leave it unless the hash itself is wrong. Add the same assertion the Go test uses.

3. **Plan inputs, if the draw needs them.** Usage bands and limits are chosen in `controller/internal/planner/planner.go` and stored on the container or GPU entity (`cpuMeanMilli`, `memMeanBytes`, `utilLo`, and so on). A metric that depends on a new knob needs a field on `planner.Plan`, a line in `Build` that fills it from `spec.Template`, and the matching serde field in `worker/src/main.rs` (`PlanContainer` / `PlanGpu`). The worker receives the plan JSON from the controller and does not re-plan.

4. **Parquet schema, Rust.** `worker/src/parquet_out.rs`. Add a row struct, a `write_*` function, and a call from `generate_item`. Files land at `samples/feature=<name>/chunk=NNNN/item-NNNNNN.parquet` plus an `item-NNNNNN.ok` marker. Column types are unsigned or signed integers, never floats. Sort by the entity id and then `step_idx` before writing. `generate_item` always writes `cpu` and `memory`. It writes `gpu` only when the work item covers a GPU workload.

5. **Parquet reader, Go.** `controller/internal/samples/rows.go`. Add the struct with `parquet:"column_name"` tags that match the Rust field names, plus `Read*File` and, if the aggregation path should scan a whole dataset, `Read*`.

6. **Expected file count.** `controller/internal/verify/verify.go`, `ExpectedFiles`. Today each work item counts as two files (cpu and memory) plus one gpu file when any workload in the item has a GPU. A new file per item has to be added there or generation ends `Failed` with `parquet files N, planned M`. If the new file can be absent for some items, mirror the GPU loop instead of adding one per item unconditionally.

7. **Conversion reads the file.** Both entry points load cpu, memory, and gpu before they call `writeBlock`:
   - `controller/internal/export/blocks.go`, `ConvertBlock`
   - `controller/internal/export/batch.go`, the three `Read*File` calls and the `feature` list in the size walk (`cpu`, `memory`, `gpu`)
   Missing files are an empty slice, which is what GPU does for a CPU-only item. A required feature should fail the block the way a missing cpu file does.

8. **TSDB series.** `controller/internal/export/tsdb.go`. `writeBlock` and `emit` take the row slices. Add the Prometheus name next to `metricCPU` and emit it from `emit` with the labels that series should carry. Container series reuse the cadvisor label set (`namespace`, `pod`, `container`, `id`, …). Timestamps are `origin + step_idx * step`, applied here, not stored in the file. `controller/internal/export/tsdb_window_test.go` is the place for a block that contains the new series.

9. **Aggregation buckets.** `controller/internal/kruize/buckets.go`. `Buckets` reduces cpu and memory into `cpuUsage`, `cpuThrottle`, `memoryUsage`, and `memoryRSS` (each `min`, `max`, `avg`, `sum`, `unit`). Add the key and the reducer. `controller/internal/kruize/shape_test.go` checks the shape. The HTTP page in `controller/internal/app/http_export.go` (`sessionResults`) does not know metric names; it filters buckets and returns them whole.

10. **UI sample queries, optional.** `ui/src/export.tsx`, `SAMPLE_QUERIES`. Those three expressions are the “Try these” links on a serving Thanos export. Add one if the series is something a person should paste on day one.

11. **Images.** Workers and the controller are separate images. A kernel or schema change needs both rebuilt and rolled out. Old datasets keep the old files; the new columns appear on datasets generated after the worker rolls out. A Thanos export has to be served again to pick up series that were not in the blocks it already wrote.

Filter helpers in `batch.go` (`filterCPU`, `filterMem`, `filterGPU`, and the `Part` variants) drop rows outside a block’s workload and step range. A new row type needs the same pair if conversion splits a chunk across blocks.

## A series derived from the plan

kube-state-metrics is this kind. Nothing in `feature=ksm` is written. `builder.ksmContainer` and `builder.ksmPods` in `controller/internal/export/tsdb.go` expand pod info, owners, phases, requests, limits, and restarts from `planner.Plan` onto the same timestamps as the container samples. Recorded rules (`sum_irate`, `sum_rate5m`, `sum_rate`, the recorded memory gauges) are computed in `b.rates` from the CPU counter while the block is written.

Add that kind of series only in `tsdb.go`, plus a test in `tsdb_window_test.go`. Skip the worker, the Parquet struct, `ExpectedFiles`, and `buckets.go` unless the aggregation API should also return it. The template field `features` lists `cpu`, `memory`, `gpu`, and `ksm` as a description; generation does not switch files on or off from that list.

## What you do not change

- The HTTP routes. Progress, serve, and the session URLs are metric-agnostic.
- The lease protocol. A work item is still “this workload range, this chunk.” A new column rides inside the files that item already writes.
- SQLite. It tracks phases and blocks, not sample values.
- The dataset epoch and the export window. They only move the timestamps applied at serve time.
