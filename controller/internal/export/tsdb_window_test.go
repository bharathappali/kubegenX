package export

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/bharathappali/kubegenx/internal/samples"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
)

func TestStaticStepsStayInsideLaterBlock(t *testing.T) {
	dir := t.TempDir()
	start := time.Date(2026, 10, 20, 0, 0, 0, 0, time.UTC)
	w, err := tsdb.NewBlockWriter(slog.Default(), dir, (2 * time.Hour).Milliseconds())
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	b := &builder{
		app:     w.Appender(context.Background()),
		start:   start.UnixMilli(),
		stepSec: 15,
		winLo:   start.Add(2 * time.Hour).UnixMilli(),
		winHi:   start.Add(4 * time.Hour).UnixMilli(),
	}
	// A container series is appended at the window start before the gauges,
	// which is what pins the block's oldest allowed timestamp.
	lbls := labels.FromStrings("__name__", "container_cpu_usage_seconds_total", "namespace", "n", "pod", "p")
	if _, err := b.app.Append(0, lbls, b.winLo, 1); err != nil {
		t.Fatal(err)
	}
	rows := []samples.CPURow{
		{StepIdx: 0},
		{StepIdx: 480}, // 2h, inside
		{StepIdx: 482}, // odd step, skipped
		{StepIdx: 960}, // 4h, outside
	}
	b.staticSteps("kube_pod_info", map[string]string{"namespace": "n", "pod": "p"}, rows, 1)
	if b.err != nil {
		t.Fatal(b.err)
	}
	if b.appended != 2 {
		t.Fatalf("appended %d", b.appended)
	}
}
