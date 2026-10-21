//! Integer sample kernel. Golden values are locked to the Go generator.

use crate::philox::u32;

const FEAT_CPU: u32 = 1;
const FEAT_MEM: u32 = 2;
const FEAT_GPU: u32 = 3;
const SCALE: i64 = 15 * 1000;
const RESTART_STARTUP_US: i64 = 50_000;

pub fn cpu_inc(seed: u64, ctr: u32, chunk: u32, sample: u32, mean_mc: u32, limit_mc: u32) -> (i64, i64) {
    let n = u32(seed, FEAT_CPU, ctr, chunk, sample, 4) % 10000;
    let mut raw = ((mean_mc as u64).wrapping_mul(5000 + n as u64) / 10000) as u32;
    if raw == 0 {
        raw = 1;
    }
    if limit_mc == 0 || raw <= limit_mc {
        return (raw as i64 * SCALE, 0);
    }
    (limit_mc as i64 * SCALE, (raw - limit_mc) as i64 * SCALE)
}

pub struct CpuRow {
    pub ctr: u32,
    pub life: u8,
    pub step: u32,
    pub usage: i64,
    pub throttle: i64,
}

pub fn emit_cpu(
    seed: u64,
    ctr: u32,
    mean_mc: u32,
    limit_mc: u32,
    restart_at: i32,
    chunk: i32,
    chunk_samples: i32,
) -> Vec<CpuRow> {
    let start = chunk * chunk_samples;
    let end = start + chunk_samples;
    let mut rows = Vec::with_capacity(chunk_samples as usize);
    let mut life0_u = 0i64;
    let mut life0_th = 0i64;
    let mut life1_u = 0i64;
    let mut life1_th = 0i64;

    let life0_limit = if restart_at >= 0 { restart_at } else { end };
    let prefix_life0_to = if restart_at >= 0 && restart_at < start {
        restart_at
    } else {
        start
    };
    let mut s = 0;
    while s < prefix_life0_to && s < life0_limit {
        let (du, dt) = cpu_inc(seed, ctr, (s / chunk_samples) as u32, (s % chunk_samples) as u32, mean_mc, limit_mc);
        life0_u += du;
        life0_th += dt;
        s += 1;
    }
    if restart_at >= 0 && restart_at < start {
        let mut s = restart_at;
        while s < start {
            if s == restart_at {
                life1_u += RESTART_STARTUP_US;
            } else {
                let (du, dt) = cpu_inc(seed, ctr, (s / chunk_samples) as u32, (s % chunk_samples) as u32, mean_mc, limit_mc);
                life1_u += du;
                life1_th += dt;
            }
            s += 1;
        }
    }

    let mut s = start;
    while s < end {
        if restart_at >= 0 && s >= restart_at {
            if s == restart_at {
                life1_u = RESTART_STARTUP_US;
                life1_th = 0;
            } else {
                let (du, dt) = cpu_inc(seed, ctr, (s / chunk_samples) as u32, (s % chunk_samples) as u32, mean_mc, limit_mc);
                life1_u += du;
                life1_th += dt;
            }
            rows.push(CpuRow { ctr, life: 1, step: s as u32, usage: life1_u, throttle: life1_th });
        } else {
            let (du, dt) = cpu_inc(seed, ctr, (s / chunk_samples) as u32, (s % chunk_samples) as u32, mean_mc, limit_mc);
            life0_u += du;
            life0_th += dt;
            rows.push(CpuRow { ctr, life: 0, step: s as u32, usage: life0_u, throttle: life0_th });
        }
        s += 1;
    }
    rows
}

pub fn mem_at(seed: u64, ctr: u32, chunk: u32, sample: u32, mean_bytes: i64, limit_bytes: i64) -> (i64, i64) {
    let n = u32(seed, FEAT_MEM, ctr, chunk, sample, 5) % 1000;
    let mut ws = mean_bytes * (800 + n as i64) / 1000;
    if limit_bytes > 0 && ws > limit_bytes {
        ws = limit_bytes;
    }
    if ws < 4096 {
        ws = 4096;
    }
    ws &= !4095;
    if ws == 0 {
        ws = 4096;
    }
    let mut rss = ws * (55 + (n % 36) as i64) / 100;
    rss &= !4095;
    if rss == 0 {
        rss = 4096;
    }
    if rss > ws {
        rss = ws;
    }
    (ws, rss)
}

pub fn gpu_at(seed: u64, gpu: u32, chunk: u32, sample: u32, util_lo: u32, util_hi: u32, fb_total: u32) -> (u8, u8, u32) {
    let mut span = util_hi.saturating_sub(util_lo);
    if span == 0 {
        span = 1;
    }
    let mut u = util_lo + u32(seed, FEAT_GPU, gpu, chunk, sample, 6) % span;
    if u > 100 {
        u = 100;
    }
    let rho = 30 + u32(seed, FEAT_GPU, gpu, chunk, sample, 7) % 60;
    let m = u * rho / 100;
    let plateau = 40 + u32(seed, FEAT_GPU, gpu, 0, 0, 8) % 40;
    let jitter = u32(seed, FEAT_GPU, gpu, chunk, sample, 9) % 5;
    let mut fb = fb_total * (plateau + jitter) / 100;
    if fb > fb_total {
        fb = fb_total;
    }
    (u as u8, m as u8, fb)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn golden_and_stitch() {
        assert_eq!(cpu_inc(424242, 7, 0, 0, 700, 500), (7_500_000, 1_155_000));
        let (ws, rss) = mem_at(424242, 7, 0, 0, 256 << 20, 0);
        assert_eq!((ws, rss), (319_434_752, 271_515_648));
        assert_eq!(gpu_at(424242, 1, 0, 0, 30, 70, 40960), (66, 31, 18022));

        let a = emit_cpu(424242, 7, 700, 500, -1, 0, 1440);
        let b = emit_cpu(424242, 7, 700, 500, -1, 1, 1440);
        assert_eq!(a[0].usage, 7_500_000);
        assert_eq!(a.last().unwrap().usage, 10_459_500_000);
        assert!(b[0].usage > a.last().unwrap().usage);
        let restart = emit_cpu(424242, 7, 700, 500, 100, 1, 64);
        // chunk 1 starts at step 64; restart at 100 is inside this chunk.
        let before = restart.iter().find(|r| r.step == 99).unwrap();
        let after = restart.iter().find(|r| r.step == 100).unwrap();
        assert_eq!(before.life, 0);
        assert_eq!(after.life, 1);
        assert!(after.usage < before.usage);
    }
}
