package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/inneslabs/jfmt"
)

func runClean(cfg Config, skipConf bool, yearExplicit bool, year int) {
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
		if yearExplicit {
			root = filepath.Join(root, strconv.Itoa(year))
			if !dirExists(root) {
				continue
			}
		}
		fmt.Printf("scanning %s...\n", d.name())

		filepath.WalkDir(root, func(path string, de fs.DirEntry, err error) error {
			if err != nil {
				fmt.Printf("warning: %v\n", err)
				return nil
			}
			name := de.Name()
			if de.IsDir() {
				if junkDirs[name] {
					items = append(items, junkItem{path, true, 0})
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

	var totalFileSize int64
	var dirCount, fileCount int
	for _, item := range items {
		if item.isDir {
			dirCount++
			fmt.Printf("  [dir ] %s\n", item.path)
		} else {
			fileCount++
			totalFileSize += item.size
			fmt.Printf("  [file] %s  (%s)\n", item.path, jfmt.FmtSize64(uint64(item.size)))
		}
	}
	fmt.Printf("\n%d dir(s), %d file(s)", dirCount, fileCount)
	if totalFileSize > 0 {
		fmt.Printf(", %s in files", jfmt.FmtSize64(uint64(totalFileSize)))
	}
	fmt.Println()

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
