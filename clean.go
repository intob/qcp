package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/inneslabs/jfmt"
)

func runClean(cfg Config, skipConf bool) {
	type junkItem struct {
		path  string
		isDir bool
		size  int64
	}
	var items []junkItem

	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			fmt.Printf("warning: %s not mounted, skipping\n", d.name())
			continue
		}
		root := filepath.Join(base, d.Root)
		fmt.Printf("scanning %s...\n", d.name())

		filepath.WalkDir(root, func(path string, de fs.DirEntry, err error) error {
			if err != nil {
				return nil
			}
			name := de.Name()
			if de.IsDir() {
				if junkDirs[name] {
					size := dirSize(path)
					items = append(items, junkItem{path, true, size})
					return filepath.SkipDir
				}
				return nil
			}
			if junkFiles[name] || strings.HasPrefix(name, "._") {
				if info, err := de.Info(); err == nil {
					items = append(items, junkItem{path, false, info.Size()})
				}
			}
			return nil
		})
	}

	if len(items) == 0 {
		fmt.Println("nothing to clean")
		return
	}

	var totalSize int64
	for _, item := range items {
		totalSize += item.size
		tag := "file"
		if item.isDir {
			tag = "dir "
		}
		fmt.Printf("  [%s] %s  (%s)\n", tag, item.path, jfmt.FmtSize64(uint64(item.size)))
	}
	fmt.Printf("\n%d item(s), %s\n", len(items), jfmt.FmtSize64(uint64(totalSize)))
	if !skipConf && !confirm() {
		return
	}

	var failed int
	for _, item := range items {
		var err error
		if item.isDir {
			err = os.RemoveAll(item.path)
		} else {
			err = os.Remove(item.path)
		}
		if err != nil {
			fmt.Printf("ERROR: %v\n", err)
			failed++
		}
	}
	if failed > 0 {
		fmt.Printf("%d item(s) could not be deleted\n", failed)
		return
	}
	fmt.Printf("removed %d item(s)\n", len(items))
}

func dirSize(path string) int64 {
	var total int64
	filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			if info, err := d.Info(); err == nil {
				total += info.Size()
			}
		}
		return nil
	})
	return total
}
