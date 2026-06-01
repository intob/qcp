package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"

)

func runStatus(cfg Config, year int) {
	yearStr := strconv.Itoa(year)
	const barWidth = 28

	// drive name column width
	maxName := 0
	for _, d := range cfg.Drives {
		if len(d.name()) > maxName {
			maxName = len(d.name())
		}
	}

	fmt.Println("DRIVES")
	for _, d := range cfg.Drives {
		base := d.basePath()
		name := fmt.Sprintf("%-*s", maxName, d.name())
		tags := d.Role
		if !d.pullAllowed() {
			tags += "  no-pull"
		}
		if !dirExists(base) {
			fmt.Printf("  %s  %-*s  %s\n", name, barWidth, "not mounted", tags)
			continue
		}
		var stat syscall.Statfs_t
		if err := syscall.Statfs(base, &stat); err != nil {
			fmt.Printf("  %s  %-*s  %s\n", name, barWidth, "?", tags)
			continue
		}
		total := stat.Blocks * uint64(stat.Bsize)
		avail := stat.Bavail * uint64(stat.Bsize)
		used := total - avail
		bar := driveSpaceBar(used, total, barWidth)
		fmt.Printf("  %s  %s  %s / %s  %s\n",
			name, bar,
			fmtSize(used), fmtSize(total),
			tags)
	}

	// missions section — same logic as runList
	fmt.Printf("\nMISSIONS  %d\n", year)

	var driveNames []string
	missionDrives := make(map[string]map[string]bool)
	var allSlugs []string
	seen := make(map[string]bool)

	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			driveNames = append(driveNames, d.name())
			continue
		}
		yearDir := filepath.Join(base, d.Root, yearStr)
		entries, err := os.ReadDir(yearDir)
		driveNames = append(driveNames, d.name())
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			slug := e.Name()
			if !seen[slug] {
				allSlugs = append(allSlugs, slug)
				seen[slug] = true
			}
			if missionDrives[slug] == nil {
				missionDrives[slug] = make(map[string]bool)
			}
			missionDrives[slug][d.name()] = true
		}
	}

	if len(allSlugs) == 0 {
		fmt.Printf("  no missions found\n")
		return
	}
	sort.Strings(allSlugs)

	maxSlug := 0
	for _, s := range allSlugs {
		if len(s) > maxSlug {
			maxSlug = len(s)
		}
	}

	// header
	fmt.Printf("  %-*s", maxSlug, "")
	for _, name := range driveNames {
		fmt.Printf("  %s", name)
	}
	fmt.Println()

	for _, slug := range allSlugs {
		drives := missionDrives[slug]
		fmt.Printf("  %-*s", maxSlug, slug)
		for _, name := range driveNames {
			if drives[name] {
				fmt.Printf("  %-*s", len(name), name)
			} else {
				fmt.Printf("  %-*s", len(name), "--")
			}
		}
		fmt.Println()
	}
}

func runList(cfg Config, year int) {
	yearStr := strconv.Itoa(year)

	var driveNames []string
	missionDrives := make(map[string]map[string]bool) // slug → drive name → present
	var allSlugs []string
	seen := make(map[string]bool)

	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		yearDir := filepath.Join(base, d.Root, yearStr)
		entries, err := os.ReadDir(yearDir)
		if err != nil {
			continue
		}
		driveNames = append(driveNames, d.name())
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			slug := e.Name()
			if !seen[slug] {
				allSlugs = append(allSlugs, slug)
				seen[slug] = true
			}
			if missionDrives[slug] == nil {
				missionDrives[slug] = make(map[string]bool)
			}
			missionDrives[slug][d.name()] = true
		}
	}

	if len(allSlugs) == 0 {
		fmt.Printf("no missions found for %d\n", year)
		return
	}
	sort.Strings(allSlugs)

	// column widths
	maxSlug := 0
	for _, s := range allSlugs {
		if len(s) > maxSlug {
			maxSlug = len(s)
		}
	}

	fmt.Printf("%-*s  %s\n", maxSlug, "mission", strings.Join(driveNames, "  "))
	fmt.Printf("%s\n", strings.Repeat("─", maxSlug+2+len(strings.Join(driveNames, "  "))))
	for _, slug := range allSlugs {
		drives := missionDrives[slug]
		var cols []string
		for _, name := range driveNames {
			if drives[name] {
				cols = append(cols, name)
			} else {
				cols = append(cols, strings.Repeat(" ", len(name)))
			}
		}
		fmt.Printf("%-*s  %s\n", maxSlug, slug, strings.Join(cols, "  "))
	}
}
