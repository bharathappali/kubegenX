// Package generate is the pure per-sample function shared with the Rust worker.
// Counters are prefix sums of integer increments, so chunks stitch exactly and
// a restart (life_seq change) starts again near zero.
package generate

import "github.com/bharathappali/kubegenx/internal/philox"

const (
	FeatCPU uint32 = 1
	FeatMem uint32 = 2
	FeatGPU uint32 = 3
	// UsPerMillicoreStep is millicores * 15s converted to microseconds.
	// 1 millicore-second = 1000 µs, times 15s.
	UsPerMillicoreStep int64 = 15 * 1000
	RestartStartupUs   int64 = 50_000
)

// CPUInc is the usage and throttle increment, in microseconds, at one step.
func CPUInc(seed uint64, ctr, chunk, sample, meanMc, limitMc uint32) (usageUs, throttleUs int64) {
	n := philox.U32(seed, FeatCPU, ctr, chunk, sample, 4) % 10000
	raw := uint32(uint64(meanMc) * uint64(5000+n) / 10000)
	if raw == 0 {
		raw = 1
	}
	if limitMc == 0 || raw <= limitMc {
		return int64(raw) * UsPerMillicoreStep, 0
	}
	return int64(limitMc) * UsPerMillicoreStep, int64(raw-limitMc) * UsPerMillicoreStep
}

// CPUAt returns the cumulative usage and throttle at this step for one lifetime.
// restartAt < 0 means the container never restarts. Steps are global indexes.
func CPUAt(seed uint64, ctr uint32, meanMc, limitMc uint32, restartAt, step, chunkSamples int) (life uint8, usageUs, throttleUs int64) {
	lifeStart := 0
	if restartAt >= 0 && step >= restartAt {
		life = 1
		lifeStart = restartAt
	}
	var u, th int64
	for s := lifeStart; s <= step; s++ {
		du, dt := CPUInc(seed, ctr, uint32(s/chunkSamples), uint32(s%chunkSamples), meanMc, limitMc)
		if s == lifeStart && life == 1 {
			du = RestartStartupUs
			dt = 0
		}
		u += du
		th += dt
	}
	return life, u, th
}

// MemAt returns working set and rss bytes at a step. Gauges, not counters.
func MemAt(seed uint64, ctr uint32, chunk, sample uint32, meanBytes, limitBytes int64) (ws, rss int64) {
	n := philox.U32(seed, FeatMem, ctr, chunk, sample, 5) % 1000
	ws = meanBytes * int64(800+n) / 1000
	if limitBytes > 0 && ws > limitBytes {
		ws = limitBytes
	}
	if ws < 4096 {
		ws = 4096
	}
	ws = ws &^ 4095
	if ws == 0 {
		ws = 4096
	}
	rss = ws * int64(55+n%36) / 100
	rss = rss &^ 4095
	if rss == 0 {
		rss = 4096
	}
	if rss > ws {
		rss = ws
	}
	return ws, rss
}

// GPUAt returns util percent, mem-copy percent and framebuffer MiB.
func GPUAt(seed uint64, gpu uint32, chunk, sample uint32, utilLo, utilHi, fbTotal uint32) (util, memcopy uint8, fb uint32) {
	span := utilHi - utilLo
	if span == 0 {
		span = 1
	}
	u := utilLo + philox.U32(seed, FeatGPU, gpu, chunk, sample, 6)%span
	if u > 100 {
		u = 100
	}
	rho := 30 + philox.U32(seed, FeatGPU, gpu, chunk, sample, 7)%60
	m := u * rho / 100
	plateau := 40 + philox.U32(seed, FeatGPU, gpu, 0, 0, 8)%40
	jitter := philox.U32(seed, FeatGPU, gpu, chunk, sample, 9) % 5
	fb = fbTotal * (plateau + jitter) / 100
	if fb > fbTotal {
		fb = fbTotal
	}
	return uint8(u), uint8(m), fb
}

// CPUBandMilli returns [lo, hi] millicores for a band name.
func CPUBandMilli(band string) (lo, hi uint32) {
	switch band {
	case "idle":
		return 1, 20
	case "low":
		return 20, 300
	case "high":
		return 1500, 8000
	default:
		return 250, 1800
	}
}

// MemBandBytes returns [lo, hi] bytes for a band name.
func MemBandBytes(band string) (lo, hi int64) {
	const mib = 1024 * 1024
	switch band {
	case "idle":
		return 16 * mib, 128 * mib
	case "low":
		return 128 * mib, 768 * mib
	case "high":
		return 6 * 1024 * mib, 32 * 1024 * mib
	default:
		return 1 * 1024 * mib, 6 * 1024 * mib
	}
}

// PickMilli chooses a mean inside the inclusive range.
func PickMilli(seed uint64, entity uint32, lo, hi uint32) uint32 {
	if hi <= lo {
		return lo
	}
	return lo + philox.U32(seed, FeatCPU, entity, 0, 0, 1)%(hi-lo+1)
}

// PickBytes chooses a mean inside the inclusive byte range, 4 KiB aligned.
func PickBytes(seed uint64, entity uint32, lo, hi int64) int64 {
	if hi <= lo {
		return lo
	}
	span := uint64(hi - lo + 1)
	n := uint64(philox.U32(seed, FeatMem, entity, 0, 0, 2))
	v := lo + int64(n%span)
	v = v &^ 4095
	if v < lo {
		v = lo
	}
	return v
}
