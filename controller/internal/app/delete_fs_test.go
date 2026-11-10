package app

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDirSizeAndRemoveTreeProgress(t *testing.T) {
	root := t.TempDir()
	sub := filepath.Join(root, "a", "b")
	if err := os.MkdirAll(sub, 0o775); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(sub, "one.bin"), make([]byte, 100), 0o664); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "two.bin"), make([]byte, 50), 0o664); err != nil {
		t.Fatal(err)
	}
	if got := dirSize(root); got != 150 {
		t.Fatalf("dirSize %d", got)
	}
	var sum int64
	var n int
	if err := removeTreeProgress(root, func(size int64) {
		sum += size
		n++
	}); err != nil {
		t.Fatal(err)
	}
	if n != 2 || sum != 150 {
		t.Fatalf("progress n=%d sum=%d", n, sum)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Fatalf("root still present: %v", err)
	}
}
