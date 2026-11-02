package app

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestWorkerCountUsesTheLargerShare(t *testing.T) {
	// 40 free cores and 80Gi free: a worker is 500m and 1Gi, so 80 CPU slots and 80 memory slots.
	freeCPU := int64(40 * 1000)
	freeMem := int64(80) << 30
	cases := []struct {
		effort string
		want   int
	}{
		{"low", 4},     // 5% of 80
		{"medium", 12}, // 15% of 80
		{"high", 32},   // 40% of 80
		{"xhigh", 64},  // 80% of 80
		{"", 64},
	}
	for _, tc := range cases {
		got := workerCount(effortPercent(tc.effort), freeCPU, 500, freeMem, 1<<30)
		if got != tc.want {
			t.Fatalf("%s: got %d want %d", tc.effort, got, tc.want)
		}
	}
}

func TestWorkerCountFloorsAtTwo(t *testing.T) {
	got := workerCount(effortPercent("low"), 1000, 500, 2<<30, 1<<30)
	if got != 2 {
		t.Fatalf("got %d", got)
	}
}

func TestNodeLocalEffortUsesOnlyThePinnedNode(t *testing.T) {
	ready := corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: "f35-h17-000-r640"}}
	ready.Status.Conditions = []corev1.NodeCondition{{Type: corev1.NodeReady, Status: corev1.ConditionTrue}}
	other := ready
	other.Name = "f35-h18-000-r640"
	pinned := &App{Node: ready.Name}
	if !pinned.countsForEffort(ready) {
		t.Fatal("pinned node was skipped")
	}
	if pinned.countsForEffort(other) {
		t.Fatal("another node was counted")
	}
	if !(&App{}).countsForEffort(other) {
		t.Fatal("shared volume skipped a ready node")
	}
}

func TestWorkerCountFollowsTheSmallerPool(t *testing.T) {
	// Plenty of CPU, only 10Gi free, so 10 slots. High is 40% -> 4.
	got := workerCount(effortPercent("high"), 100*1000, 500, 10<<30, 1<<30)
	if got != 4 {
		t.Fatalf("got %d", got)
	}
}
