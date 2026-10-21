// Package samples reads worker Parquet files.
package samples

import (
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
