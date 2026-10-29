// Package samples reads worker Parquet files.
package samples

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/parquet-go/parquet-go"
)

type CPURow struct {
	CtrID       uint32 `parquet:"ctr_id"`
	LifeSeq     uint8  `parquet:"life_seq"`
	StepIdx     uint32 `parquet:"step_idx"`
	UsageUs     int64  `parquet:"usage_us"`
	ThrottledUs int64  `parquet:"throttled_us"`
}

type MemRow struct {
	CtrID    uint32 `parquet:"ctr_id"`
	LifeSeq  uint8  `parquet:"life_seq"`
	StepIdx  uint32 `parquet:"step_idx"`
	WsBytes  int64  `parquet:"ws_bytes"`
	RssBytes int64  `parquet:"rss_bytes"`
}

type GPURow struct {
	GPUEntID uint32 `parquet:"gpu_ent_id"`
	StepIdx  uint32 `parquet:"step_idx"`
	Util     uint8  `parquet:"util"`
	MemCopy  uint8  `parquet:"memcopy"`
	FBUsed   uint32 `parquet:"fb_used_mib"`
}

// ItemFile is one work-item Parquet file: a workload range inside one 6h chunk.
func ItemFile(dir, feature string, chunk, index int) string {
	return filepath.Join(dir, "samples", "feature="+feature, fmt.Sprintf("chunk=%04d", chunk), fmt.Sprintf("item-%06d.parquet", index))
}

func ReadCPUFile(path string) ([]CPURow, error) {
	return readFile[CPURow](path)
}

func ReadMemFile(path string) ([]MemRow, error) {
	return readFile[MemRow](path)
}

func ReadGPUFile(path string) ([]GPURow, error) {
	return readFile[GPURow](path)
}

func readFile[T any](path string) ([]T, error) {
	if _, err := os.Stat(path); err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return parquet.ReadFile[T](path)
}

func ReadCPU(dir string) ([]CPURow, error) {
	return readAll[CPURow](dir, "cpu")
}

func ReadMem(dir string) ([]MemRow, error) {
	return readAll[MemRow](dir, "memory")
}

func ReadGPU(dir string) ([]GPURow, error) {
	return readAll[GPURow](dir, "gpu")
}

func readAll[T any](dir, feature string) ([]T, error) {
	pattern := filepath.Join(dir, "samples", "feature="+feature, "chunk=*", "item-*.parquet")
	matches, err := filepath.Glob(pattern)
	if err != nil {
		return nil, err
	}
	sort.Strings(matches)
	var out []T
	for _, path := range matches {
		rows, err := parquet.ReadFile[T](path)
		if err != nil {
			return nil, err
		}
		out = append(out, rows...)
	}
	return out, nil
}

func CountParquet(dir string) (int, error) {
	n := 0
	root := filepath.Join(dir, "samples")
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if !info.IsDir() && filepath.Ext(path) == ".parquet" {
			n++
		}
		return nil
	})
	return n, err
}
