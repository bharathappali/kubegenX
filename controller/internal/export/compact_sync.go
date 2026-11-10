package export

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

const CompactedMarker = ".kubegenx-compacted"
const stagedULIDsFile = "staged-ulids.txt"

// TSDBDir is the export block root under the data volume.
func TSDBDir(data, dataset, exportName string) string {
	return filepath.Join(data, "exports", dataset, exportName, "tsdb")
}

// Compacted reports whether a prior compact finished successfully.
func Compacted(tsdbDir string) bool {
	_, err := os.Stat(filepath.Join(tsdbDir, CompactedMarker))
	return err == nil
}

// DayBucket is one UTC calendar day in the export window.
type DayBucket struct {
	Key   string // YYYYMMDD
	Start time.Time
	End   time.Time
}

// ListDayBuckets returns UTC days that have at least one overlapping block.
func ListDayBuckets(tsdbDir string) ([]DayBucket, error) {
	ents, err := os.ReadDir(tsdbDir)
	if err != nil {
		return nil, err
	}
	seen := map[string]DayBucket{}
	for _, ent := range ents {
		if !ent.IsDir() || !looksLikeULID(ent.Name()) {
			continue
		}
		minT, maxT, err := ReadBlockTimes(filepath.Join(tsdbDir, ent.Name()))
		if err != nil {
			continue
		}
		for t := time.UnixMilli(minT).UTC().Truncate(24 * time.Hour); t.UnixMilli() < maxT; t = t.Add(24 * time.Hour) {
			key := t.Format("20060102")
			if _, ok := seen[key]; ok {
				continue
			}
			seen[key] = DayBucket{Key: key, Start: t, End: t.Add(24 * time.Hour)}
		}
	}
	out := make([]DayBucket, 0, len(seen))
	for _, b := range seen {
		out = append(out, b)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out, nil
}

// ReadBlockTimes reads minTime/maxTime from a block's meta.json.
func ReadBlockTimes(blockDir string) (minTime, maxTime int64, err error) {
	raw, err := os.ReadFile(filepath.Join(blockDir, "meta.json"))
	if err != nil {
		return 0, 0, err
	}
	var meta struct {
		MinTime int64 `json:"minTime"`
		MaxTime int64 `json:"maxTime"`
	}
	if err := json.Unmarshal(raw, &meta); err != nil {
		return 0, 0, err
	}
	return meta.MinTime, meta.MaxTime, nil
}

func overlaps(minT, maxT int64, start, end time.Time) bool {
	return minT < end.UnixMilli() && maxT > start.UnixMilli()
}

// CompactSyncIn copies every ULID block onto scratch (S3 path, full compact).
func CompactSyncIn(dataTSDB, scratchTSDB string) error {
	return CompactSyncInWindow(dataTSDB, scratchTSDB, time.Time{}, time.Time{})
}

// CompactSyncInWindow copies blocks overlapping [start,end) onto scratch.
// Zero start/end copies all blocks. Writes staged-ulids.txt for scoped stage-out.
func CompactSyncInWindow(dataTSDB, scratchTSDB string, start, end time.Time) error {
	if err := os.RemoveAll(scratchTSDB); err != nil {
		return err
	}
	if err := os.MkdirAll(scratchTSDB, 0o775); err != nil {
		return err
	}
	ents, err := os.ReadDir(dataTSDB)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("tsdb missing: %s", dataTSDB)
		}
		return err
	}
	var staged []string
	for _, ent := range ents {
		if !ent.IsDir() || !looksLikeULID(ent.Name()) {
			continue
		}
		src := filepath.Join(dataTSDB, ent.Name())
		if !start.IsZero() && !end.IsZero() {
			minT, maxT, err := ReadBlockTimes(src)
			if err != nil || !overlaps(minT, maxT, start, end) {
				continue
			}
		}
		dst := filepath.Join(scratchTSDB, ent.Name())
		if err := copyTree(src, dst); err != nil {
			return fmt.Errorf("stage %s: %w", ent.Name(), err)
		}
		staged = append(staged, ent.Name())
	}
	return os.WriteFile(filepath.Join(scratchTSDB, stagedULIDsFile), []byte(strings.Join(staged, "\n")+"\n"), 0o644)
}

// CompactSyncOut publishes a full scratch tree back and writes CompactedMarker.
func CompactSyncOut(scratchTSDB, dataTSDB string) error {
	if err := compactSyncOutScoped(scratchTSDB, dataTSDB, true); err != nil {
		return err
	}
	return os.WriteFile(filepath.Join(dataTSDB, CompactedMarker), []byte("ok\n"), 0o644)
}

// CompactSyncOutWindow publishes scratch results and deletes only previously staged ULIDs.
func CompactSyncOutWindow(scratchTSDB, dataTSDB string) error {
	return compactSyncOutScoped(scratchTSDB, dataTSDB, false)
}

func compactSyncOutScoped(scratchTSDB, dataTSDB string, deleteAllMissing bool) error {
	if err := os.MkdirAll(dataTSDB, 0o775); err != nil {
		return err
	}
	scratchBlocks, err := listULIDDirs(scratchTSDB)
	if err != nil {
		return err
	}
	dataBlocks, err := listULIDDirs(dataTSDB)
	if err != nil {
		return err
	}
	for name := range scratchBlocks {
		if _, ok := dataBlocks[name]; ok {
			continue
		}
		if err := copyTree(filepath.Join(scratchTSDB, name), filepath.Join(dataTSDB, name)); err != nil {
			return fmt.Errorf("publish %s: %w", name, err)
		}
	}
	removable := dataBlocks
	if !deleteAllMissing {
		removable = map[string]bool{}
		raw, err := os.ReadFile(filepath.Join(scratchTSDB, stagedULIDsFile))
		if err != nil {
			return fmt.Errorf("staged ulids: %w", err)
		}
		for _, line := range strings.Split(string(raw), "\n") {
			line = strings.TrimSpace(line)
			if looksLikeULID(line) {
				removable[line] = true
			}
		}
	}
	for name := range removable {
		if scratchBlocks[name] {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dataTSDB, name)); err != nil {
			return fmt.Errorf("remove %s: %w", name, err)
		}
	}
	return nil
}

// MarkCompacted writes the success marker on the data TSDB root.
func MarkCompacted(tsdbDir string) error {
	return os.WriteFile(filepath.Join(tsdbDir, CompactedMarker), []byte("ok\n"), 0o644)
}

func listULIDDirs(root string) (map[string]bool, error) {
	out := map[string]bool{}
	ents, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return out, nil
		}
		return nil, err
	}
	for _, ent := range ents {
		if ent.IsDir() && looksLikeULID(ent.Name()) {
			out[ent.Name()] = true
		}
	}
	return out, nil
}

func looksLikeULID(name string) bool {
	if len(name) != 26 {
		return false
	}
	for _, c := range strings.ToUpper(name) {
		ok := (c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z')
		if !ok {
			return false
		}
	}
	return true
}
