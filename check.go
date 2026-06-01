package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

func runCheck(cfg Config, year int) {
	yearStr := strconv.Itoa(year)

	// find hot drive (source of truth) and cold drives
	var hotDrive *DriveConfig
	var coldDrives []DriveConfig
	for i, d := range cfg.Drives {
		if !dirExists(d.basePath()) {
			fmt.Printf("%s %s %s\n", yellow("warning:"), bold(d.name()), dim("not mounted, skipping"))
			continue
		}
		if d.Role == "hot" {
			hotDrive = &cfg.Drives[i]
		} else {
			coldDrives = append(coldDrives, d)
		}
	}
	if hotDrive == nil {
		exit(1, "no hot drive mounted")
	}
	if len(coldDrives) == 0 {
		exit(1, "no cold drives mounted")
	}

	hotYearDir := filepath.Join(hotDrive.basePath(), hotDrive.Root, yearStr)
	entries, err := os.ReadDir(hotYearDir)
	if err != nil {
		exit(1, "cannot read %s on %s: %v", yearStr, hotDrive.name(), err)
	}

	var slugs []string
	for _, e := range entries {
		if e.IsDir() && isNumberedMission(e.Name()) {
			slugs = append(slugs, e.Name())
		}
	}
	if len(slugs) == 0 {
		fmt.Printf(dim("no missions found for %d\n"), year)
		return
	}
	sort.Strings(slugs)

	type gap struct {
		vol     string
		missing []string
		extra   []string
	}
	type missionReport struct {
		slug string
		gaps []gap
	}

	var reports []missionReport
	var totalMissing, totalExtra int

	for _, slug := range slugs {
		hotDir := filepath.Join(hotYearDir, slug)
		hotFiles, err := findFiles(hotDir)
		if err != nil {
			fmt.Printf("%s scanning %s on %s: %v\n", red("ERROR"), slug, hotDrive.name(), err)
			continue
		}
		hotSet := make(map[string]bool, len(hotFiles))
		for _, f := range hotFiles {
			hotSet[f.rel] = true
		}

		var gaps []gap
		for _, cold := range coldDrives {
			coldDir := filepath.Join(cold.basePath(), cold.Root, yearStr, slug)
			if !dirExists(coldDir) {
				gaps = append(gaps, gap{
					vol:     cold.name(),
					missing: []string{"(mission directory missing)"},
				})
				totalMissing++
				continue
			}
			coldFiles, err := findFiles(coldDir)
			if err != nil {
				fmt.Printf("%s scanning %s on %s: %v\n", red("ERROR"), slug, cold.name(), err)
				continue
			}
			coldSet := make(map[string]bool, len(coldFiles))
			for _, f := range coldFiles {
				coldSet[f.rel] = true
			}

			var missing, extra []string
			for _, f := range hotFiles {
				if !coldSet[f.rel] {
					missing = append(missing, f.rel)
				}
			}
			for _, f := range coldFiles {
				if !hotSet[f.rel] {
					extra = append(extra, f.rel)
				}
			}
			if len(missing) > 0 || len(extra) > 0 {
				gaps = append(gaps, gap{cold.name(), missing, extra})
				totalMissing += len(missing)
				totalExtra += len(extra)
			}
		}

		if len(gaps) > 0 {
			reports = append(reports, missionReport{slug, gaps})
		}
	}

	// summary header
	checked := len(slugs)
	incomplete := len(reports)
	fmt.Printf("checked %s in %d", bold(fmt.Sprintf("%d mission(s)", checked)), year)
	if incomplete == 0 {
		fmt.Printf("  %s\n", green("✓ all complete"))
		return
	}
	fmt.Printf("  %s\n\n", yellow(fmt.Sprintf("%d incomplete", incomplete)))

	for _, r := range reports {
		fmt.Printf("  %s\n", bold(r.slug))
		for _, g := range r.gaps {
			fmt.Printf("    %s\n", yellow(g.vol))
			for _, f := range g.missing {
				fmt.Printf("      %s %s\n", red("−"), f)
			}
			for _, f := range g.extra {
				fmt.Printf("      %s %s\n", dim("+"), f)
			}
		}
	}

	fmt.Println()
	if totalMissing > 0 {
		fmt.Printf("  %s files missing from cold drives", red(strconv.Itoa(totalMissing)))
		if totalExtra > 0 {
			fmt.Printf("  ·  %s extra files on cold drives", dim(strconv.Itoa(totalExtra)))
		}
		fmt.Println()
		fmt.Printf(dim("  run -update <n> to copy missing files for a specific mission\n"))
	}
}
