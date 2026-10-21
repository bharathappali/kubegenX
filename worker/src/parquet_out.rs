use std::fs::{self, File};
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use arrow_array::{Int64Array, RecordBatch, UInt32Array, UInt8Array};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use xxhash_rust::xxh3::xxh3_64;

use crate::gen::{emit_cpu, gpu_at, mem_at, CpuRow};

pub struct Written {
    pub path: String,
    pub bytes: i64,
    pub rows: i64,
    pub xxh3: String,
}

fn props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::try_new(1).unwrap()))
        .set_data_page_size_limit(1024 * 1024)
        .set_write_batch_size(8192)
        .build()
}

fn finish(path: &Path, rows: i64) -> anyhow::Result<Written> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let hash = xxh3_64(&bytes);
    Ok(Written {
        path: path.display().to_string(),
        bytes: bytes.len() as i64,
        rows,
        xxh3: format!("{hash:016x}"),
    })
}

pub fn write_cpu(dir: &Path, chunk: i32, item: i32, rows: &[CpuRow]) -> anyhow::Result<Written> {
    let mut sorted: Vec<&CpuRow> = rows.iter().collect();
    sorted.sort_by_key(|r| (r.ctr, r.step));
    let schema = Arc::new(Schema::new(vec![
        Field::new("ctr_id", DataType::UInt32, false),
        Field::new("life_seq", DataType::UInt8, false),
        Field::new("step_idx", DataType::UInt32, false),
        Field::new("usage_us", DataType::Int64, false),
        Field::new("throttled_us", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from_iter_values(sorted.iter().map(|r| r.ctr))),
            Arc::new(UInt8Array::from_iter_values(sorted.iter().map(|r| r.life))),
            Arc::new(UInt32Array::from_iter_values(sorted.iter().map(|r| r.step))),
            Arc::new(Int64Array::from_iter_values(sorted.iter().map(|r| r.usage))),
            Arc::new(Int64Array::from_iter_values(sorted.iter().map(|r| r.throttle))),
        ],
    )?;
    write_batch(dir, "cpu", chunk, item, schema, batch, rows.len() as i64)
}

pub struct MemRow {
    pub ctr: u32,
    pub life: u8,
    pub step: u32,
    pub ws: i64,
    pub rss: i64,
}

pub fn write_mem(dir: &Path, chunk: i32, item: i32, rows: &[MemRow]) -> anyhow::Result<Written> {
    let mut sorted: Vec<&MemRow> = rows.iter().collect();
    sorted.sort_by_key(|r| (r.ctr, r.step));
    let schema = Arc::new(Schema::new(vec![
        Field::new("ctr_id", DataType::UInt32, false),
        Field::new("life_seq", DataType::UInt8, false),
        Field::new("step_idx", DataType::UInt32, false),
        Field::new("ws_bytes", DataType::Int64, false),
        Field::new("rss_bytes", DataType::Int64, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from_iter_values(sorted.iter().map(|r| r.ctr))),
            Arc::new(UInt8Array::from_iter_values(sorted.iter().map(|r| r.life))),
            Arc::new(UInt32Array::from_iter_values(sorted.iter().map(|r| r.step))),
            Arc::new(Int64Array::from_iter_values(sorted.iter().map(|r| r.ws))),
            Arc::new(Int64Array::from_iter_values(sorted.iter().map(|r| r.rss))),
        ],
    )?;
    write_batch(dir, "memory", chunk, item, schema, batch, rows.len() as i64)
}

pub struct GpuRow {
    pub gpu: u32,
    pub step: u32,
    pub util: u8,
    pub memcopy: u8,
    pub fb: u32,
}

pub fn write_gpu(dir: &Path, chunk: i32, item: i32, rows: &[GpuRow]) -> anyhow::Result<Written> {
    let mut sorted: Vec<&GpuRow> = rows.iter().collect();
    sorted.sort_by_key(|r| (r.gpu, r.step));
    let schema = Arc::new(Schema::new(vec![
        Field::new("gpu_ent_id", DataType::UInt32, false),
        Field::new("step_idx", DataType::UInt32, false),
        Field::new("util", DataType::UInt8, false),
        Field::new("memcopy", DataType::UInt8, false),
        Field::new("fb_used_mib", DataType::UInt32, false),
    ]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(UInt32Array::from_iter_values(sorted.iter().map(|r| r.gpu))),
            Arc::new(UInt32Array::from_iter_values(sorted.iter().map(|r| r.step))),
            Arc::new(UInt8Array::from_iter_values(sorted.iter().map(|r| r.util))),
            Arc::new(UInt8Array::from_iter_values(sorted.iter().map(|r| r.memcopy))),
            Arc::new(UInt32Array::from_iter_values(sorted.iter().map(|r| r.fb))),
        ],
    )?;
    write_batch(dir, "gpu", chunk, item, schema, batch, rows.len() as i64)
}

fn write_batch(
    dir: &Path,
    feature: &str,
    chunk: i32,
    item: i32,
    schema: Arc<Schema>,
    batch: RecordBatch,
    rows: i64,
) -> anyhow::Result<Written> {
    let folder = dir.join(format!("samples/feature={feature}/chunk={chunk:04}"));
    fs::create_dir_all(&folder)?;
    let path = folder.join(format!("item-{item:06}.parquet"));
    let file = File::create(&path)?;
    let mut w = ArrowWriter::try_new(file, schema, Some(props()))?;
    w.write(&batch)?;
    w.close()?;
    let written = finish(&path, rows)?;
    let marker = folder.join(format!("item-{item:06}.ok"));
    fs::write(&marker, written.bytes.to_string())?;
    Ok(written)
}

pub fn generate_item(
    root: &Path,
    seed: u64,
    chunk_samples: i32,
    containers: &[crate::PlanContainer],
    gpus: &[crate::PlanGpu],
    wl_lo: i32,
    wl_hi: i32,
    chunk: i32,
    item_index: i32,
) -> anyhow::Result<Vec<Written>> {
    let mut cpu_rows = Vec::new();
    let mut mem_rows = Vec::new();
    for c in containers {
        if c.workload_id < wl_lo || c.workload_id >= wl_hi {
            continue;
        }
        let cpu = emit_cpu(
            seed,
            c.id as u32,
            c.cpu_mean_milli,
            c.cpu_limit_milli,
            c.restart_at,
            chunk,
            chunk_samples,
        );
        for row in &cpu {
            let (ws, rss) = mem_at(seed, c.id as u32, chunk as u32, row.step % chunk_samples as u32, c.mem_mean_bytes, c.mem_limit_bytes);
            mem_rows.push(MemRow { ctr: c.id as u32, life: row.life, step: row.step, ws, rss });
        }
        cpu_rows.extend(cpu);
    }
    let mut out = Vec::new();
    out.push(write_cpu(root, chunk, item_index, &cpu_rows)?);
    out.push(write_mem(root, chunk, item_index, &mem_rows)?);
    let mut gpu_rows = Vec::new();
    for g in gpus {
        if g.workload_id < wl_lo || g.workload_id >= wl_hi {
            continue;
        }
        for sample in 0..chunk_samples {
            let step = (chunk * chunk_samples + sample) as u32;
            let (util, memcopy, fb) = gpu_at(seed, g.id as u32, chunk as u32, sample as u32, g.util_lo, g.util_hi, g.fb_total_mib);
            gpu_rows.push(GpuRow { gpu: g.id as u32, step, util, memcopy, fb });
        }
    }
    if !gpu_rows.is_empty() {
        out.push(write_gpu(root, chunk, item_index, &gpu_rows)?);
    }
    Ok(out)
}
