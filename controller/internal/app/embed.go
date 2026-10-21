package app

import (
	"embed"
	"io/fs"
)

//go:embed dist
var dist embed.FS

func fsSub() (fs.FS, error) {
	return fs.Sub(dist, "dist")
}
