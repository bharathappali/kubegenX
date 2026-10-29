package export

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/bharathappali/kubegenx/internal/planner"
	"github.com/bharathappali/kubegenx/internal/samples"
)

const (
	// podMemory is the controller limit. Three quarters of it is the conversion
	// budget. The remaining quarter stays unused by these jobs so aggregation
	// API requests, which cannot be queued, still have room to run.
	podMemory int64 = 16 << 30
	jobBudget int64 = podMemory * 75 / 100
	// writerBytes is the TSDB appender cost of one 2h block covering 1024 workloads.
	writerBytes int64 = 2 << 30
	// decodeFactor turns a parquet file size into a decoded-row estimate when the
	// scan can see the files. The row-size formula is the floor.
	decodeFactor int64 = 8
	maxBatchCPU        = 8
)

// buildBatched scans the parquet files, estimates each 2h block, and runs as
// many item goroutines as fit in jobBudget. A 6h item is read once. Its 2h
// blocks are independent and are written together only when that does not
// crowd out another item read.
func buildBatched(ctx context.Context, plan *planner.Plan, datasetDir, scratch, exportName string, start time.Time, external map[string]string, progress func(done, total int, bytes int64, workers int, heap, budget int64)) (Result, error) {
	if plan == nil || plan.StepSeconds < 1 || plan.Steps < 1 {
		return Result{}, fmt.Errorf("dataset has no duration")
	}
	if len(plan.Items) == 0 {
		return Result{}, fmt.Errorf("dataset has no work items")
	}
	destRoot := filepath.Join(filepath.Dir(filepath.Dir(datasetDir)), "exports", plan.Dataset, exportName, "tsdb")
	if err := os.RemoveAll(destRoot); err != nil {
		return Result{}, err
	}
	if err := os.MkdirAll(destRoot, 0o775); err != nil {
		return Result{}, err
	}
	total := 0
	for _, it := range plan.Items {
		total += len(itemSlices(plan, it))
	}
	if total == 0 {
		return Result{}, fmt.Errorf("no samples landed in a block")
	}
	ests := scanItems(plan, datasetDir)
	best := fattest(ests)
	cpus := runtime.GOMAXPROCS(0)
	items, sliceN, parts := planLayout(best.decoded, best.perSlice, best.slices, cpus)
	var disk int64
	for _, e := range ests {
		disk += e.disk
	}
	slog.Info("export batch",
		"items", len(plan.Items), "slices", total,
		"flight", items, "sliceParallel", sliceN, "parts", parts,
		"writers", items*sliceN*parts,
		"parquet", disk, "decoded", best.decoded, "block", best.perSlice,
		"budget", jobBudget, "reserved", podMemory-jobBudget,
	)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	gate := newMemGate(jobBudget, cpus*2)
	gate.base = readHeap()
	go gate.tick(ctx)

	var mu sync.Mutex
	var done, blocks int
	var bytes int64
	var first error
	report := func(log bool) {
		mu.Lock()
		d, b := done, bytes
		mu.Unlock()
		flight, heap, scale := gate.snapshot()
		writers := flight * sliceN * parts
		if writers < 1 {
			writers = items * sliceN * parts
		}
		if log {
			slog.Info("export estimate", "flight", flight, "writers", writers, "heap", heap, "budget", jobBudget, "scale", scale, "blocks", d, "bytes", b)
		}
		if progress != nil {
			progress(d, total, b, writers, heap, jobBudget)
		}
	}
	fail := func(err error) {
		if err == nil {
			return
		}
		mu.Lock()
		if first == nil {
			first = err
			cancel()
		}
		mu.Unlock()
	}
	report(true)
	go func() {
		t := time.NewTicker(5 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				report(true)
			}
		}
	}()

	var wg sync.WaitGroup
	for i, it := range plan.Items {
		ticket, err := gate.acquire(ctx, ests[i].charge(sliceN))
		if err != nil {
			fail(err)
			break
		}
		wg.Add(1)
		go func(it planner.WorkItem, ticket int64) {
			defer wg.Done()
			defer gate.release(ticket)
			err := convertItem(ctx, plan, datasetDir, scratch, exportName, destRoot, start, external, it, sliceN, parts, func(n int, sz int64) {
				mu.Lock()
				done++
				blocks += n
				bytes += sz
				mu.Unlock()
				report(false)
			})
			if err != nil {
				fail(err)
			}
		}(it, ticket)
	}
	wg.Wait()
	if first != nil {
		return Result{}, first
	}
	if blocks == 0 {
		return Result{}, fmt.Errorf("no samples landed in a block")
	}
	return Result{Blocks: blocks, Bytes: bytes}, nil
}

type stepSlice struct {
	lo, hi int
	emitNS bool
}

func itemSlices(plan *planner.Plan, it planner.WorkItem) []stepSlice {
	chunkSamples := plan.ChunkSamples
	if chunkSamples < 1 {
		chunkSamples = 1440
	}
	perBlock := int(blockSpan/time.Second) / plan.StepSeconds
	if perBlock < 1 {
		perBlock = 1
	}
	base := it.Chunk * chunkSamples
	span := chunkSamples
	if base+span > plan.Steps {
		span = plan.Steps - base
	}
	if span < 1 {
		return nil
	}
	var out []stepSlice
	for off := 0; off < span; off += perBlock {
		lo := base + off
		hi := lo + perBlock
		if hi > base+span {
			hi = base + span
		}
		if hi <= lo {
			continue
		}
		out = append(out, stepSlice{lo: lo, hi: hi, emitNS: it.Index == 0})
	}
	return out
}

type itemEstimate struct {
	disk     int64
	decoded  int64
	perSlice int64
	slices   int
}

func (e itemEstimate) one() int64 {
	c := e.decoded + e.perSlice
	if c < 1 {
		return 1
	}
	return c
}

func (e itemEstimate) charge(sliceN int) int64 {
	n := sliceN
	if n > e.slices {
		n = e.slices
	}
	if n < 1 {
		n = 1
	}
	c := e.decoded + int64(n)*e.perSlice
	if c < 1 {
		return 1
	}
	return c
}

func fattest(ests []itemEstimate) itemEstimate {
	var best itemEstimate
	for _, e := range ests {
		if best.decoded == 0 || e.one() > best.one() {
			best = e
		}
	}
	return best
}

func scanItems(plan *planner.Plan, dir string) []itemEstimate {
	out := make([]itemEstimate, len(plan.Items))
	cpus := runtime.GOMAXPROCS(0)
	if cpus < 1 {
		cpus = 1
	}
	sem := make(chan struct{}, cpus)
	var wg sync.WaitGroup
	for i, it := range plan.Items {
		wg.Add(1)
		go func(i int, it planner.WorkItem) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			out[i] = estimateItem(plan, dir, it)
		}(i, it)
	}
	wg.Wait()
	return out
}

func estimateItem(plan *planner.Plan, dir string, it planner.WorkItem) itemEstimate {
	var disk int64
	for _, feature := range []string{"cpu", "memory", "gpu"} {
		info, err := os.Stat(samples.ItemFile(dir, feature, it.Chunk, it.Index))
		if err != nil || info.IsDir() {
			continue
		}
		disk += info.Size()
	}
	decoded := disk * decodeFactor
	formula := itemBytes(plan)
	if decoded < formula {
		decoded = formula
	}
	span := it.WlHi - it.WlLo
	if span < 1 {
		span = 1
	}
	per := writerBytes * int64(span) / 1024
	if per < 32<<20 {
		per = 32 << 20
	}
	n := len(itemSlices(plan, it))
	if n < 1 {
		n = 1
	}
	return itemEstimate{disk: disk, decoded: decoded, perSlice: per, slices: n}
}

// planLayout packs 2h blocks into jobBudget. Concurrent item reads come first,
// because each read waits on storage. Extra 2h blocks of an item already in
// memory are added only when that does not give up a read. Parts then split a
// block across goroutines up to two per CPU.
func planLayout(decoded, perSlice int64, nSlices, cpus int) (items, sliceN, parts int) {
	if cpus < 1 {
		cpus = 1
	}
	if cpus > maxBatchCPU {
		cpus = maxBatchCPU
	}
	if nSlices < 1 {
		nSlices = 1
	}
	if decoded < 1 {
		decoded = 1
	}
	if perSlice < 1 {
		perSlice = 1
	}
	capItems := cpus * 2
	one := decoded + perSlice
	items = int(jobBudget / one)
	if items < 1 {
		items = 1
	}
	if items > capItems {
		items = capItems
	}
	sliceN = 1
	for sliceN < nSlices {
		next := decoded + int64(sliceN+1)*perSlice
		if int64(items)*next > jobBudget || items*(sliceN+1) > capItems {
			break
		}
		sliceN++
	}
	writers := items * sliceN
	if writers < 1 {
		writers = 1
	}
	parts = 1
	if writers < capItems {
		parts = capItems / writers
	}
	if parts < 1 {
		parts = 1
	}
	if parts > capItems {
		parts = capItems
	}
	return items, sliceN, parts
}

func itemBytes(plan *planner.Plan) int64 {
	if plan == nil {
		return 32 << 20
	}
	wls := len(plan.Workloads)
	if wls < 1 {
		wls = 1
	}
	perItem := 1024
	if wls < perItem {
		perItem = wls
	}
	samplesN := plan.ChunkSamples
	if samplesN < 1 {
		samplesN = 1440
	}
	ctrs := len(plan.Containers)
	if ctrs < 1 {
		ctrs = wls
	}
	rows := int64(perItem) * int64(ctrs) / int64(wls) * int64(samplesN) * 64 * 2
	if rows < 32<<20 {
		rows = 32 << 20
	}
	return rows
}

// memGate admits item goroutines until their estimates fill jobBudget.
// scale rises when the live heap runs ahead of the scan, so later items
// take a larger ticket and fewer of them stay in flight.
type memGate struct {
	mu        sync.Mutex
	cond      *sync.Cond
	budget    int64
	used      int64
	flight    int
	maxFlight int
	scale     int64
	base      int64
}

func newMemGate(budget int64, maxFlight int) *memGate {
	if maxFlight < 1 {
		maxFlight = 1
	}
	g := &memGate{budget: budget, maxFlight: maxFlight, scale: 100}
	g.cond = sync.NewCond(&g.mu)
	return g
}

func (g *memGate) tick(ctx context.Context) {
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			g.mu.Lock()
			g.cond.Broadcast()
			g.mu.Unlock()
			return
		case <-t.C:
			g.mu.Lock()
			g.cond.Broadcast()
			g.mu.Unlock()
		}
	}
}

func (g *memGate) acquire(ctx context.Context, cost int64) (int64, error) {
	if cost < 1 {
		cost = 1
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		ticket := cost * g.scale / 100
		if ticket < 1 {
			ticket = 1
		}
		heap := readHeap()
		room := g.flight == 0 || (g.flight < g.maxFlight && g.used+ticket <= g.budget && heap <= jobBudget)
		if room {
			g.used += ticket
			g.flight++
			return ticket, nil
		}
		g.cond.Wait()
	}
}

func (g *memGate) release(ticket int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.used -= ticket
	if g.used < 0 {
		g.used = 0
	}
	g.flight--
	if g.flight < 0 {
		g.flight = 0
	}
	if g.used > 0 {
		job := readHeap() - g.base
		if job < 0 {
			job = 0
		}
		ratio := job * 100 / g.used
		prev := g.scale
		if ratio > 120 && g.scale < 300 {
			g.scale = g.scale * ratio / 100
			if g.scale > 300 {
				g.scale = 300
			}
		}
		if g.scale != prev {
			slog.Info("export estimate", "scale", g.scale, "heap", readHeap(), "reserved", g.used, "flight", g.flight)
		}
	}
	g.cond.Broadcast()
}

func (g *memGate) snapshot() (flight int, heap int64, scale int64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.flight, readHeap(), g.scale
}

func heapInuse() int64 {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	return int64(m.HeapInuse)
}

var readHeap = heapInuse

func splitRange(lo, hi, parts int) [][2]int {
	if hi <= lo {
		return nil
	}
	if parts < 1 {
		parts = 1
	}
	span := hi - lo
	if parts > span {
		parts = span
	}
	out := make([][2]int, 0, parts)
	for i := 0; i < parts; i++ {
		a := lo + span*i/parts
		b := lo + span*(i+1)/parts
		if b > a {
			out = append(out, [2]int{a, b})
		}
	}
	return out
}

type owners struct {
	ctr map[uint32]int
	gpu map[uint32]int
}

func ownersFor(plan *planner.Plan, lo, hi int) owners {
	o := owners{ctr: map[uint32]int{}, gpu: map[uint32]int{}}
	for _, c := range plan.Containers {
		if c.WorkloadID >= lo && c.WorkloadID < hi {
			o.ctr[uint32(c.ID)] = c.WorkloadID
		}
	}
	for _, g := range plan.GPUs {
		if g.WorkloadID >= lo && g.WorkloadID < hi {
			o.gpu[uint32(g.ID)] = g.WorkloadID
		}
	}
	return o
}

func convertItem(ctx context.Context, plan *planner.Plan, datasetDir, scratch, exportName, destRoot string, start time.Time, external map[string]string, it planner.WorkItem, sliceN, parts int, onSlice func(n int, sz int64)) error {
	slices := itemSlices(plan, it)
	if len(slices) == 0 {
		return nil
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}
	cpuPath := samples.ItemFile(datasetDir, "cpu", it.Chunk, it.Index)
	memPath := samples.ItemFile(datasetDir, "memory", it.Chunk, it.Index)
	gpuPath := samples.ItemFile(datasetDir, "gpu", it.Chunk, it.Index)
	cpu, err := samples.ReadCPUFile(cpuPath)
	dropCache(cpuPath)
	if err != nil {
		return err
	}
	mem, err := samples.ReadMemFile(memPath)
	dropCache(memPath)
	if err != nil {
		return err
	}
	gpu, err := samples.ReadGPUFile(gpuPath)
	dropCache(gpuPath)
	if err != nil {
		return err
	}
	if sliceN < 1 {
		sliceN = 1
	}
	if sliceN > len(slices) {
		sliceN = len(slices)
	}
	own := ownersFor(plan, it.WlLo, it.WlHi)
	ranges := splitRange(it.WlLo, it.WlHi, parts)
	if len(ranges) == 0 {
		return nil
	}
	sem := make(chan struct{}, sliceN)
	var wg sync.WaitGroup
	var mu sync.Mutex
	var first error
	sec := time.Duration(plan.StepSeconds) * time.Second
	for _, sl := range slices {
		if ctx.Err() != nil {
			mu.Lock()
			if first == nil {
				first = ctx.Err()
			}
			mu.Unlock()
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(sl stepSlice) {
			defer wg.Done()
			defer func() { <-sem }()
			n, sz, err := writeSlice(ctx, plan, cpu, mem, gpu, own, ranges, scratch, exportName, destRoot, start, external, it, sl, sec)
			mu.Lock()
			if err != nil && first == nil {
				first = err
			}
			mu.Unlock()
			if err == nil && onSlice != nil {
				onSlice(n, sz)
			}
		}(sl)
	}
	wg.Wait()
	return first
}

func writeSlice(ctx context.Context, plan *planner.Plan, cpu []samples.CPURow, mem []samples.MemRow, gpu []samples.GPURow, own owners, ranges [][2]int, scratch, exportName, destRoot string, start time.Time, external map[string]string, it planner.WorkItem, sl stepSlice, sec time.Duration) (int, int64, error) {
	keepLo := sl.lo - 20
	if keepLo < 0 {
		keepLo = 0
	}
	var wg sync.WaitGroup
	var mu sync.Mutex
	var blocks int
	var bytes int64
	var first error
	for pi, part := range ranges {
		wg.Add(1)
		go func(pi, wlLo, wlHi int) {
			defer wg.Done()
			if ctx.Err() != nil {
				return
			}
			emitNS := sl.emitNS && pi == 0
			cpuW := filterCPUPart(cpu, keepLo, sl.hi, own.ctr, wlLo, wlHi)
			memW := filterMemPart(mem, keepLo, sl.hi, own.ctr, wlLo, wlHi)
			gpuW := filterGPUPart(gpu, keepLo, sl.hi, own.gpu, wlLo, wlHi)
			if len(cpuW) == 0 && len(memW) == 0 && len(gpuW) == 0 && !emitNS {
				return
			}
			lo := start.Add(time.Duration(sl.lo) * sec)
			hi := start.Add(time.Duration(sl.hi) * sec)
			token := fmt.Sprintf("c%04d-i%06d-s%d-p%d", it.Chunk, it.Index, sl.lo, pi)
			n, sz, err := writeBlock(ctx, plan, cpuW, memW, gpuW, scratch, exportName, token, start, lo, hi, destRoot, external, wlLo, wlHi, sl.lo, sl.hi, emitNS)
			mu.Lock()
			if err != nil && first == nil {
				first = err
			}
			blocks += n
			bytes += sz
			mu.Unlock()
		}(pi, part[0], part[1])
	}
	wg.Wait()
	if first != nil {
		return blocks, bytes, first
	}
	return blocks, bytes, nil
}

func filterCPU(rows []samples.CPURow, lo, hi int) []samples.CPURow {
	out := make([]samples.CPURow, 0)
	for _, r := range rows {
		if int(r.StepIdx) >= lo && int(r.StepIdx) < hi {
			out = append(out, r)
		}
	}
	return out
}

func filterMem(rows []samples.MemRow, lo, hi int) []samples.MemRow {
	out := make([]samples.MemRow, 0)
	for _, r := range rows {
		if int(r.StepIdx) >= lo && int(r.StepIdx) < hi {
			out = append(out, r)
		}
	}
	return out
}

func filterGPU(rows []samples.GPURow, lo, hi int) []samples.GPURow {
	if len(rows) == 0 {
		return nil
	}
	out := make([]samples.GPURow, 0)
	for _, r := range rows {
		if int(r.StepIdx) >= lo && int(r.StepIdx) < hi {
			out = append(out, r)
		}
	}
	return out
}

func filterCPUPart(rows []samples.CPURow, stepLo, stepHi int, own map[uint32]int, wlLo, wlHi int) []samples.CPURow {
	out := make([]samples.CPURow, 0)
	for _, r := range rows {
		if int(r.StepIdx) < stepLo || int(r.StepIdx) >= stepHi {
			continue
		}
		wl, ok := own[r.CtrID]
		if !ok || wl < wlLo || wl >= wlHi {
			continue
		}
		out = append(out, r)
	}
	return out
}

func filterMemPart(rows []samples.MemRow, stepLo, stepHi int, own map[uint32]int, wlLo, wlHi int) []samples.MemRow {
	out := make([]samples.MemRow, 0)
	for _, r := range rows {
		if int(r.StepIdx) < stepLo || int(r.StepIdx) >= stepHi {
			continue
		}
		wl, ok := own[r.CtrID]
		if !ok || wl < wlLo || wl >= wlHi {
			continue
		}
		out = append(out, r)
	}
	return out
}

func filterGPUPart(rows []samples.GPURow, stepLo, stepHi int, own map[uint32]int, wlLo, wlHi int) []samples.GPURow {
	if len(rows) == 0 {
		return nil
	}
	out := make([]samples.GPURow, 0)
	for _, r := range rows {
		if int(r.StepIdx) < stepLo || int(r.StepIdx) >= stepHi {
			continue
		}
		wl, ok := own[r.GPUEntID]
		if !ok || wl < wlLo || wl >= wlHi {
			continue
		}
		out = append(out, r)
	}
	return out
}
