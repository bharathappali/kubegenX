package aggregate

import (
	"testing"
	"time"

	"github.com/bharathappali/kubegenx/internal/planner"
)

func TestParseInterval(t *testing.T) {
	step := 15 * time.Second
	d, err := ParseInterval("15m", step)
	if err != nil || d != 15*time.Minute {
		t.Fatalf("15m: %v %v", d, err)
	}
	if _, err := ParseInterval("2s", step); err == nil {
		t.Fatal("seconds should be rejected")
	}
	if _, err := ParseInterval("10s", step); err == nil {
		t.Fatal("10s should be rejected")
	}
	if _, err := ParseInterval("7s", step); err == nil {
		t.Fatal("7s should be rejected")
	}
	if _, err := ParseInterval("1.5m", step); err == nil {
		t.Fatal("decimals should be rejected")
	}
}

func TestWindow(t *testing.T) {
	p := &planner.Plan{Steps: 4 * 1440, StepSeconds: 15}
	start, end, err := Window(p, "start", "2026-10-05T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if !end.Equal(start.Add(24 * time.Hour)) {
		t.Fatalf("end %s", end)
	}
	start, end, err = Window(p, "end", "2026-10-21T00:00:00Z")
	if err != nil {
		t.Fatal(err)
	}
	if !start.Equal(end.Add(-24 * time.Hour)) {
		t.Fatalf("start %s end %s", start, end)
	}
}
