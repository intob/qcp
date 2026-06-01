package main

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"

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
			fmt.Printf("%s %s not mounted, %s\n", yellow("warning:"), bold(d.name()), dim("skipping"))
			continue
		}
		root := filepath.Join(base, d.Root)
		if yearExplicit {
			root = filepath.Join(root, strconv.Itoa(year))
			if !dirExists(root) {
				continue
			}
		}
		fmt.Printf("scanning %s...\n", bold(d.name()))

		if err := filepath.WalkDir(root, func(path string, de fs.DirEntry, err error) error {
			if err != nil {
				fmt.Printf("%s %v\n", yellow("warning:"), err)
				return nil
			}
			name := de.Name()
			if de.IsDir() {
				if isJunk(name, true) {
					items = append(items, junkItem{path, true, 0})
					return filepath.SkipDir
				}
				return nil
			}
			if isJunk(name, false) {
				if info, err := de.Info(); err == nil {
					items = append(items, junkItem{path, false, info.Size()})
				}
			}
			return nil
		}); err != nil {
			fmt.Printf("%s walk error on %s: %v\n", yellow("warning:"), bold(d.name()), err)
		}
	}

	if len(items) == 0 {
		fmt.Println(dim("nothing to clean"))
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
			fmt.Printf("  [file] %s  (%s)\n", item.path, fmtSize(uint64(item.size)))
		}
	}
	fmt.Printf("\n%d dir(s), %d file(s)", dirCount, fileCount)
	if totalFileSize > 0 {
		fmt.Printf(", %s in files", fmtSize(uint64(totalFileSize)))
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
			fmt.Printf("%s %v\n", red("ERROR:"), err)
			failed++
		}
	}
	if failed > 0 {
		fmt.Printf("%d item(s) could not be deleted\n", failed)
		return
	}
	fmt.Printf("%s removed %d item(s)\n", green("✓"), len(items))
}
