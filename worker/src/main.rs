mod gen;
mod parquet_out;
mod philox;

use std::path::PathBuf;
use std::time::Duration;

use anyhow::Context;
use serde::Deserialize;
use tokio::time::sleep;
use tonic::transport::Channel;

pub mod leasev1 {
    tonic::include_proto!("kubegenx.lease.v1");
}

use leasev1::lease_queue_client::LeaseQueueClient;
use leasev1::{CompleteRequest, FailRequest, FileReport, HeartbeatRequest, LeaseRequest, RegisterRequest};

#[derive(Debug, Deserialize)]
struct PlanFile {
    seed: u64,
    #[serde(rename = "chunkSamples")]
    chunk_samples: i32,
    containers: Vec<PlanContainer>,
    gpus: Vec<PlanGpu>,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PlanContainer {
    pub id: i32,
    #[serde(rename = "workloadId")]
    pub workload_id: i32,
    #[serde(rename = "cpuMeanMilli")]
    pub cpu_mean_milli: u32,
    #[serde(rename = "cpuLimitMilli")]
    pub cpu_limit_milli: u32,
    #[serde(rename = "memMeanBytes")]
    pub mem_mean_bytes: i64,
    #[serde(rename = "memLimitBytes")]
    pub mem_limit_bytes: i64,
    #[serde(rename = "restartAt")]
    pub restart_at: i32,
}

#[derive(Debug, Deserialize, Clone)]
pub struct PlanGpu {
    pub id: i32,
    #[serde(rename = "workloadId")]
    pub workload_id: i32,
    #[serde(rename = "fbTotalMiB")]
    pub fb_total_mib: u32,
    #[serde(rename = "utilLo")]
    pub util_lo: u32,
    #[serde(rename = "utilHi")]
    pub util_hi: u32,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let dataset = std::env::var("KUBEGENX_DATASET").context("KUBEGENX_DATASET")?;
    let addr = std::env::var("KUBEGENX_CONTROLLER").unwrap_or_else(|_| "kubegenx-controller.kube-gen-x.svc:9091".into());
    let data = PathBuf::from(std::env::var("KUBEGENX_DATA").unwrap_or_else(|_| "/data".into()));
    let worker_id = std::env::var("HOSTNAME").unwrap_or_else(|_| "worker".into());
    let endpoint = if addr.starts_with("http") { addr } else { format!("http://{addr}") };

    let mut client = connect(&endpoint).await?;
    let root = data.join("datasets").join(&dataset);
    let plan_path = root.join("plan").join("generation.json");
    let plan: PlanFile = load_plan(&plan_path).await?;

    let reg = client
        .register(RegisterRequest {
            worker_id: worker_id.clone(),
            cores: std::thread::available_parallelism().map(|n| n.get() as i32).unwrap_or(1),
            version: env!("CARGO_PKG_VERSION").into(),
            dataset: dataset.clone(),
        })
        .await?
        .into_inner();
    eprintln!("registered digest {} ttl {}", reg.plan_digest, reg.lease_ttl_seconds);

    let hb_worker = worker_id.clone();
    let hb_dataset = dataset.clone();
    let hb_endpoint = endpoint.clone();
    tokio::spawn(async move {
        let mut hb = match connect(&hb_endpoint).await {
            Ok(c) => c,
            Err(err) => {
                eprintln!("heartbeat connect: {err}");
                return;
            }
        };
        loop {
            sleep(Duration::from_secs(15)).await;
            if hb
                .heartbeat(HeartbeatRequest {
                    worker_id: hb_worker.clone(),
                    dataset: hb_dataset.clone(),
                    inflight: vec![],
                    samples_done: 0,
                    bytes: 0,
                })
                .await
                .is_err()
            {
                eprintln!("heartbeat failed");
            }
        }
    });

    loop {
        let resp = client
            .lease(LeaseRequest { worker_id: worker_id.clone(), dataset: dataset.clone(), n: 1 })
            .await?
            .into_inner();
        if resp.items.is_empty() {
            if resp.drained {
                eprintln!("queue drained");
                return Ok(());
            }
            sleep(Duration::from_secs(2)).await;
            continue;
        }
        for item in resp.items {
            let result = parquet_out::generate_item(
                &root,
                plan.seed,
                plan.chunk_samples,
                &plan.containers,
                &plan.gpus,
                item.wl_lo,
                item.wl_hi,
                item.chunk,
                item.item_index,
            );
            match result {
                Ok(files) => {
                    let reports = files
                        .iter()
                        .map(|f| FileReport {
                            path: f.path.clone(),
                            bytes: f.bytes,
                            rows: f.rows,
                            xxh3: f.xxh3.clone(),
                        })
                        .collect();
                    client
                        .complete(CompleteRequest {
                            worker_id: worker_id.clone(),
                            dataset: dataset.clone(),
                            item_id: item.id.clone(),
                            files: reports,
                        })
                        .await?;
                    eprintln!("completed {} files {}", item.id, files.len());
                }
                Err(err) => {
                    eprintln!("item {} failed: {err}", item.id);
                    client
                        .fail(FailRequest {
                            worker_id: worker_id.clone(),
                            dataset: dataset.clone(),
                            item_id: item.id,
                            error: err.to_string(),
                            retryable: true,
                        })
                        .await?;
                }
            }
        }
    }
}

async fn load_plan(path: &std::path::Path) -> anyhow::Result<PlanFile> {
    let mut last = String::new();
    for _ in 0..60 {
        match std::fs::read(path) {
            Ok(b) => return Ok(serde_json::from_slice(&b).with_context(|| format!("parse {}", path.display()))?),
            Err(err) => {
                last = err.to_string();
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
    Err(anyhow::anyhow!("plan {}: {last}", path.display()))
}

async fn connect(endpoint: &str) -> anyhow::Result<LeaseQueueClient<Channel>> {
    let mut last = None;
    for attempt in 0..60 {
        match LeaseQueueClient::connect(endpoint.to_string()).await {
            Ok(c) => return Ok(c),
            Err(err) => {
                last = Some(err);
                eprintln!("connect {endpoint} attempt {attempt}: waiting");
                sleep(Duration::from_secs(2)).await;
            }
        }
    }
    Err(anyhow::anyhow!("connect {endpoint}: {last:?}"))
}
