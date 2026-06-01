package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func runInit(cfg Config, year int, yearExplicit bool) {
	seq, err := readSeq()
	if err != nil {
		exit(1, "err reading seq: %v", err)
	}

	maxByYear := make(map[int]int)

	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		rootDir := filepath.Join(base, d.Root)
		if yearExplicit {
			scanYearDir(filepath.Join(rootDir, strconv.Itoa(year)), year, maxByYear)
		} else {
			entries, err := os.ReadDir(rootDir)
			if err != nil {
				continue
			}
			for _, e := range entries {
				if !e.IsDir() {
					continue
				}
				y, err := strconv.Atoi(e.Name())
				if err != nil {
					continue
				}
				scanYearDir(filepath.Join(rootDir, e.Name()), y, maxByYear)
			}
		}
	}

	if len(maxByYear) == 0 {
		fmt.Println("no missions found on any mounted drive")
		return
	}

	var years []int
	for y := range maxByYear {
		years = append(years, y)
	}
	sort.Ints(years)

	changed := false
	for _, y := range years {
		max := maxByYear[y]
		current := seq[y]
		if max > current {
			fmt.Printf("  %d: %03d → %03d\n", y, current, max)
			seq[y] = max
			changed = true
		} else {
			fmt.Printf("  %d: %03d (already up to date)\n", y, current)
		}
	}

	if !changed {
		fmt.Println("seq already up to date")
		return
	}
	if err := writeSeq(seq); err != nil {
		exit(1, "err writing seq: %v", err)
	}
	fmt.Println("seq updated")
}

func scanYearDir(yearDir string, year int, maxByYear map[int]int) {
	entries, err := os.ReadDir(yearDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		parts := strings.SplitN(e.Name(), "_", 2)
		if len(parts) < 2 {
			continue
		}
		n, err := strconv.Atoi(parts[0])
		if err != nil || n <= 0 {
			continue
		}
		if n > maxByYear[year] {
			maxByYear[year] = n
		}
	}
}
