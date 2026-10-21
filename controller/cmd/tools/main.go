package main

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintln(os.Stderr, "usage: tools rm|du|selftest <path>")
		os.Exit(2)
	}
	switch os.Args[1] {
	case "rm":
		if len(os.Args) != 3 {
			os.Exit(2)
		}
		if err := os.RemoveAll(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	case "du":
		var n int64
		var files int
		_ = filepath.Walk(os.Args[2], func(_ string, info os.FileInfo, err error) error {
			if err != nil || info == nil || info.IsDir() {
				return nil
			}
			n += info.Size()
			files++
			return nil
		})
		fmt.Printf("bytes=%d files=%d\n", n, files)
	case "selftest":
		if err := selftest(os.Args[2]); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		fmt.Println("ok")
	default:
		os.Exit(2)
	}
}

func selftest(dir string) error {
	path := filepath.Join(dir, ".selftest-tools")
	if err := os.MkdirAll(path, 0o775); err != nil {
		return err
	}
	defer os.RemoveAll(path)
	f, err := os.OpenFile(filepath.Join(path, "f"), os.O_CREATE|os.O_RDWR, 0o664)
	if err != nil {
		return err
	}
	defer f.Close()
	if _, err := f.Write([]byte("ok")); err != nil {
		return err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX); err != nil {
		return err
	}
	_ = syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
	return nil
}
