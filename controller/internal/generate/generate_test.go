package generate

import "testing"

func TestStitchAndRestart(t *testing.T) {
	const (
		seed         = uint64(424242)
		ctr          = uint32(7)
		mean         = uint32(700)
		limit        = uint32(500)
		chunkSamples = 64
		restartAt    = 100
	)
	var prevLife uint8
	var prevU int64
	for step := 0; step < chunkSamples*3; step++ {
		life, u, th := CPUAt(seed, ctr, mean, limit, restartAt, step, chunkSamples)
		if th < 0 || u <= 0 {
			t.Fatalf("step %d negative or zero u=%d th=%d", step, u, th)
		}
		if step > 0 && life == prevLife && u <= prevU {
			t.Fatalf("step %d not increasing %d -> %d", step, prevU, u)
		}
		if step > 0 && life != prevLife {
			if u >= prevU {
				t.Fatalf("restart did not drop counter %d -> %d", prevU, u)
			}
			if u > RestartStartupUs*2 {
				t.Fatalf("restart counter not near zero: %d", u)
			}
		}
		// Cross the chunk boundary explicitly.
		if step > 0 && step%chunkSamples == 0 && life == prevLife {
			// previous step is last of prior chunk; current must be strictly above it.
			if u <= prevU {
				t.Fatalf("chunk boundary stall at %d", step)
			}
		}
		prevLife, prevU = life, u
	}
}

func TestGolden(t *testing.T) {
	u, th := CPUInc(424242, 7, 0, 0, 700, 500)
	t.Logf("CPUInc = %d %d", u, th)
	_, c0, _ := CPUAt(424242, 7, 700, 500, -1, 0, 1440)
	_, c1, _ := CPUAt(424242, 7, 700, 500, -1, 1439, 1440)
	_, c2, _ := CPUAt(424242, 7, 700, 500, -1, 1440, 1440)
	t.Logf("c0=%d cLast=%d cNext=%d", c0, c1, c2)
	ws, rss := MemAt(424242, 7, 0, 0, 256<<20, 0)
	util, mc, fb := GPUAt(424242, 1, 0, 0, 30, 70, 40960)
	t.Logf("mem %d %d gpu %d %d %d", ws, rss, util, mc, fb)
}
