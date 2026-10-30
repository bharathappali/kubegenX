package samples

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/parquet-go/parquet-go"
)

func TestReadCPUMatchingSkipsOtherRowGroups(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "cpu.parquet")
	rows := []cpuWrite{
		{CtrID: 1, StepIdx: 0, UsageUs: 15_000_000},
		{CtrID: 1, StepIdx: 1, UsageUs: 30_000_000},
		{CtrID: 99, StepIdx: 0, UsageUs: 9_000_000_000},
		{CtrID: 99, StepIdx: 1, UsageUs: 9_000_000_000},
	}
	if err := parquet.WriteFile(path, rows, parquet.MaxRowsPerRowGroup(2)); err != nil {
		t.Fatal(err)
	}
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		t.Fatal(err)
	}
	if len(pf.RowGroups()) != 2 {
		t.Fatalf("row groups %d", len(pf.RowGroups()))
	}
	kept := 0
	for _, rg := range pf.RowGroups() {
		if rowGroupHasIDs(rg, 1, 1) {
			kept++
		}
	}
	if kept != 1 {
		t.Fatalf("matching row groups %d", kept)
	}
	got, err := ReadCPUMatching(path, []uint32{1})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].CtrID != 1 || got[1].CtrID != 1 {
		t.Fatalf("rows %+v", got)
	}
}

type cpuWrite struct {
	CtrID       uint32 `parquet:"ctr_id"`
	LifeSeq     int32  `parquet:"life_seq"`
	StepIdx     uint32 `parquet:"step_idx"`
	UsageUs     int64  `parquet:"usage_us"`
	ThrottledUs int64  `parquet:"throttled_us"`
}
