package export

import (
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

// dropCache tells the kernel the file is done, so blocks already copied to
// storage do not sit in the page cache and eat the quarter reserved for APIs.
func dropCache(root string) {
	if root == "" {
		return
	}
	_ = filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil || info == nil || info.IsDir() {
			return nil
		}
		f, err := os.Open(path)
		if err != nil {
			return nil
		}
		_ = unix.Fadvise(int(f.Fd()), 0, 0, unix.FADV_DONTNEED)
		f.Close()
		return nil
	})
}
