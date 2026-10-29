package store

import (
	"testing"
	"time"
)

func TestBlockLeaseCompletes(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	err = s.InsertBlocks("scale", []Block{
		{ID: "a", Chunk: 0, Index: 0, WlLo: 0, WlHi: 2, StepLo: 0, StepHi: 4, EmitNS: true},
		{ID: "b", Chunk: 0, Index: 1, WlLo: 2, WlHi: 4, StepLo: 0, StepHi: 4},
	})
	if err != nil {
		t.Fatal(err)
	}
	got, drained, err := s.LeaseBlocks("scale", "w1", 1, time.Minute)
	if err != nil || drained || len(got) != 1 || got[0].ID != "a" || !got[0].EmitNS {
		t.Fatalf("lease %+v drained %v err %v", got, drained, err)
	}
	if err := s.CompleteBlock("scale", "a", 10); err != nil {
		t.Fatal(err)
	}
	got, drained, err = s.LeaseBlocks("scale", "w1", 1, time.Minute)
	if err != nil || drained || len(got) != 1 || got[0].ID != "b" {
		t.Fatalf("second %+v drained %v err %v", got, drained, err)
	}
	if err := s.CompleteBlock("scale", "b", 4); err != nil {
		t.Fatal(err)
	}
	got, drained, err = s.LeaseBlocks("scale", "w1", 1, time.Minute)
	if err != nil || !drained || len(got) != 0 {
		t.Fatalf("drained %+v %v %v", got, drained, err)
	}
	p, err := s.BlockProgress("scale")
	if err != nil || p.Done != 2 || p.Total != 2 || p.Bytes != 14 {
		t.Fatalf("progress %+v %v", p, err)
	}
	listed, err := s.ListBlocks("scale")
	if err != nil || len(listed) != 2 || listed[0].State != "done" {
		t.Fatalf("list %+v %v", listed, err)
	}
}

func TestGenerationAndExportDurations(t *testing.T) {
	s, err := Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.UpsertDataset("demo", "Generating", "d", 1, 1); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkGenerationFinish("demo"); err != nil {
		t.Fatal(err)
	}
	started, finished, err := s.GenerationTimes("demo")
	if err != nil || started != 0 || finished != 0 {
		t.Fatalf("finish before start %d %d %v", started, finished, err)
	}
	if err := s.MarkGenerationStart("demo"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkGenerationStart("demo"); err != nil {
		t.Fatal(err)
	}
	started, _, err = s.GenerationTimes("demo")
	if err != nil || started == 0 {
		t.Fatalf("start %d %v", started, err)
	}
	if err := s.MarkGenerationFinish("demo"); err != nil {
		t.Fatal(err)
	}
	if err := s.MarkGenerationFinish("demo"); err != nil {
		t.Fatal(err)
	}
	again, finished, err := s.GenerationTimes("demo")
	if err != nil || again != started || finished < started {
		t.Fatalf("times %d %d %v", again, finished, err)
	}

	row := ExportRow{Name: "demo", Dataset: "demo", Phase: "Building", StartedAt: time.Now().Unix()}
	if err := s.SaveExport(row); err != nil {
		t.Fatal(err)
	}
	if err := s.TouchExport("demo", "Serving", 1, 1, 10, "", row.StartedAt); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetExport("demo")
	if err != nil || got.FinishedAt < got.StartedAt || got.StartedAt == 0 {
		t.Fatalf("export %+v %v", got, err)
	}
	if err := s.TouchExport("demo", "Building", 1, 1, 10, "", got.StartedAt); err != nil {
		t.Fatal(err)
	}
	got, err = s.GetExport("demo")
	if err != nil || got.FinishedAt != 0 {
		t.Fatalf("cleared %+v %v", got, err)
	}
}
