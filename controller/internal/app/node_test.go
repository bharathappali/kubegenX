package app

import "testing"

func TestNodeSelectorEmptyOnSharedVolume(t *testing.T) {
	if got := (&App{}).nodeSelector(); got != nil {
		t.Fatalf("shared volume selector = %v", got)
	}
}

func TestNodeSelectorPinsTheDatasetNode(t *testing.T) {
	got := (&App{Node: "f35-h17-000-r640"}).nodeSelector()
	if got["kubernetes.io/hostname"] != "f35-h17-000-r640" {
		t.Fatalf("selector = %v", got)
	}
}
