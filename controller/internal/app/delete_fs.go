package app

import (
	"io/fs"
	"os"
	"path/filepath"
)

func dirSize(root string) int64 {
	var total int64
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		total += info.Size()
		return nil
	})
	return total
}

// removeTreeProgress deletes files under root one by one (then empty dirs),
// calling onFile with each file's size before removing it.
func removeTreeProgress(root string, onFile func(size int64)) error {
	info, err := os.Stat(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	if !info.IsDir() {
		sz := info.Size()
		if onFile != nil {
			onFile(sz)
		}
		return os.Remove(root)
	}
	var files []string
	_ = filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		files = append(files, path)
		return nil
	})
	for _, path := range files {
		sz := int64(0)
		if st, err := os.Stat(path); err == nil {
			sz = st.Size()
		}
		if onFile != nil {
			onFile(sz)
		}
		_ = os.Remove(path)
	}
	return os.RemoveAll(root)
}
