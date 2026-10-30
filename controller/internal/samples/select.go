package samples

import (
	"io"
	"os"

	"github.com/parquet-go/parquet-go"
)

// ReadCPUMatching decodes the row groups whose ctr_id range overlaps ids.
// ReadCPUFile still reads every row; Thanos conversion uses that path.
func ReadCPUMatching(path string, ids []uint32) ([]CPURow, error) {
	return readMatching(path, ids, func(r CPURow) uint32 { return r.CtrID })
}

// ReadMemMatching decodes the row groups whose ctr_id range overlaps ids.
func ReadMemMatching(path string, ids []uint32) ([]MemRow, error) {
	return readMatching(path, ids, func(r MemRow) uint32 { return r.CtrID })
}

func readMatching[T any](path string, ids []uint32, idOf func(T) uint32) ([]T, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	f, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}
	pf, err := parquet.OpenFile(f, info.Size())
	if err != nil {
		return nil, err
	}
	lo, hi := ids[0], ids[0]
	want := make(map[uint32]bool, len(ids))
	for _, id := range ids {
		want[id] = true
		if id < lo {
			lo = id
		}
		if id > hi {
			hi = id
		}
	}
	var out []T
	buf := make([]T, 4096)
	for _, rg := range pf.RowGroups() {
		if !rowGroupHasIDs(rg, lo, hi) {
			continue
		}
		reader := parquet.NewGenericRowGroupReader[T](rg)
		for {
			n, err := reader.Read(buf)
			for i := 0; i < n; i++ {
				if want[idOf(buf[i])] {
					out = append(out, buf[i])
				}
			}
			if err == io.EOF {
				break
			}
			if err != nil {
				reader.Close()
				return nil, err
			}
			if n == 0 {
				break
			}
		}
		reader.Close()
	}
	return out, nil
}

func rowGroupHasIDs(rg parquet.RowGroup, lo, hi uint32) bool {
	cols := rg.Schema().Columns()
	idx := -1
	for i, path := range cols {
		if len(path) > 0 && path[len(path)-1] == "ctr_id" {
			idx = i
			break
		}
	}
	chunks := rg.ColumnChunks()
	if idx < 0 || idx >= len(chunks) {
		return true
	}
	bounded, ok := chunks[idx].(*parquet.FileColumnChunk)
	if !ok {
		return true
	}
	min, max, ok := bounded.Bounds()
	if !ok {
		return true
	}
	return max.Uint32() >= lo && min.Uint32() <= hi
}
