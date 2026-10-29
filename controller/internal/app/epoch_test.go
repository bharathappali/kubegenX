package app

import (
	"testing"
	"time"

	"github.com/bharathappali/kubegenx/internal/spec"
)

func TestDefaultEpochEndsAtUTCMidnight(t *testing.T) {
	tpl := spec.Template{Time: spec.Time{Duration: "15d"}}
	now := time.Date(2026, 10, 29, 12, 0, 0, 0, time.UTC)
	if got := defaultEpoch(now, tpl); got != "2026-10-15T00:00:00Z" {
		t.Fatalf("got %s", got)
	}
}

func TestDefaultEpochUsesUTCDay(t *testing.T) {
	tpl := spec.Template{Time: spec.Time{Duration: "15d"}}
	// 02:00 in UTC+5:30 is still the previous UTC day.
	ist := time.FixedZone("IST", 5*3600+30*60)
	now := time.Date(2026, 10, 29, 2, 0, 0, 0, ist)
	if got := defaultEpoch(now, tpl); got != "2026-10-14T00:00:00Z" {
		t.Fatalf("got %s", got)
	}
}
