package export

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestCompactSyncRoundTrip(t *testing.T) {
	root := t.TempDir()
	data := filepath.Join(root, "data")
	scratch := filepath.Join(root, "scratch", "tsdb")
	block := "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	if err := os.MkdirAll(filepath.Join(data, block), 0o775); err != nil {
		t.Fatal(err)
	}
	meta := []byte(`{"ulid":"x","minTime":1000,"maxTime":2000}`)
	if err := os.WriteFile(filepath.Join(data, block, "meta.json"), meta, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := CompactSyncIn(data, scratch); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(scratch, block, "meta.json")); err != nil {
		t.Fatal(err)
	}
	newBlock := "01BX5ZZKBKACTAV9WEVGEMMVRZ"
	if err := os.RemoveAll(filepath.Join(scratch, block)); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(scratch, newBlock), 0o775); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(scratch, newBlock, "meta.json"), []byte(`{"ulid":"y","minTime":1000,"maxTime":2000}`), 0o644); err != nil {
		t.Fatal(err)
	}
	// Preserve staged list from original sync-in for windowed out.
	if err := os.WriteFile(filepath.Join(scratch, stagedULIDsFile), []byte(block+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := CompactSyncOut(scratch, data); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(data, block)); !os.IsNotExist(err) {
		t.Fatalf("old block still present: %v", err)
	}
	if _, err := os.Stat(filepath.Join(data, newBlock, "meta.json")); err != nil {
		t.Fatal(err)
	}
	if !Compacted(data) {
		t.Fatal("expected compacted marker")
	}
}

func TestCompactSyncInWindowSkipsOtherDays(t *testing.T) {
	root := t.TempDir()
	data := filepath.Join(root, "data")
	scratch := filepath.Join(root, "scratch", "tsdb")
	day := time.Date(2026, 10, 28, 0, 0, 0, 0, time.UTC)
	inDay := "01ARZ3NDEKTSV4RRFFQ69G5FAV"
	outDay := "01BX5ZZKBKACTAV9WEVGEMMVRZ"
	writeBlock := func(id string, min, max int64) {
		dir := filepath.Join(data, id)
		if err := os.MkdirAll(dir, 0o775); err != nil {
			t.Fatal(err)
		}
		body := []byte(`{"minTime":` + strconv.FormatInt(min, 10) + `,"maxTime":` + strconv.FormatInt(max, 10) + `}`)
		if err := os.WriteFile(filepath.Join(dir, "meta.json"), body, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeBlock(inDay, day.UnixMilli(), day.Add(2*time.Hour).UnixMilli())
	writeBlock(outDay, day.Add(48*time.Hour).UnixMilli(), day.Add(50*time.Hour).UnixMilli())
	if err := CompactSyncInWindow(data, scratch, day, day.Add(24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(scratch, inDay)); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(scratch, outDay)); !os.IsNotExist(err) {
		t.Fatal("out-of-window block staged")
	}
}
