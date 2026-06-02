package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
)

func anyColdMounted(cfg Config) bool {
	for _, d := range cfg.Drives {
		if d.Role == "cold" && dirExists(d.basePath()) {
			return true
		}
	}
	return false
}

func runCheckMission(cfg Config, missionNum int, year int, yearExplicit bool) bool {
	var hotDrives, allColdDrives []DriveConfig
	for _, d := range cfg.Drives {
		if !dirExists(d.basePath()) {
			continue
		}
		if d.Role == "hot" {
			hotDrives = append(hotDrives, d)
		} else if d.Role == "cold" {
			allColdDrives = append(allColdDrives, d)
		}
	}
	if len(hotDrives) == 0 {
		exit(1, "no hot drive mounted")
	}
	if len(allColdDrives) == 0 {
		exit(1, "no cold drives mounted")
	}

	var searchYears []int
	if yearExplicit {
		searchYears = []int{year}
	} else {
		searchYears = allYears(cfg)
		if len(searchYears) == 0 {
			exit(1, "mission %03d not found", missionNum)
		}
	}

	allDrives := append(hotDrives, allColdDrives...)
	var slug, yearStr string
	var foundYear int
	for _, y := range searchYears {
		ys := strconv.Itoa(y)
		s, err := findMissionSlug(allDrives, ys, missionNum)
		if err == nil {
			slug, yearStr, foundYear = s, ys, y
			break
		}
	}
	if slug == "" {
		exit(1, "mission %03d not found", missionNum)
	}

	// filter cold drives to those scoped for this year
	var coldDrives []DriveConfig
	for _, c := range allColdDrives {
		if c.coversYear(foundYear) {
			coldDrives = append(coldDrives, c)
		}
	}
	if len(coldDrives) == 0 {
		fmt.Printf(dim("no cold drives are scoped for %s\n"), yearStr)
		return true
	}

	// prefer a hot drive as reference; fall back to the first cold drive that has it
	var refDir, refVol, refColdVol string
	for _, h := range hotDrives {
		dir := filepath.Join(h.basePath(), h.Root, yearStr, slug)
		if dirExists(dir) {
			refDir, refVol = dir, h.name()
			break
		}
	}
	if refDir == "" {
		for _, c := range coldDrives {
			dir := filepath.Join(c.basePath(), c.Root, yearStr, slug)
			if dirExists(dir) {
				refDir, refVol, refColdVol = dir, c.name(), c.name()
				break
			}
		}
	}
	if refDir == "" {
		exit(1, "mission %s not found on any drive", slug)
	}

	refFiles, err := findFiles(refDir)
	if err != nil {
		exit(1, "error scanning %s on %s: %v", slug, refVol, err)
	}
	refSet := make(map[string]bool, len(refFiles))
	for _, f := range refFiles {
		refSet[f.rel] = true
	}

	// check manifest: files listed in checksums.b3 but absent from disk
	var ghosts []string
	manifest := readChecksumFile(filepath.Join(refDir, "checksums.b3"))
	for rel := range manifest {
		if !refSet[rel] {
			ghosts = append(ghosts, rel)
		}
	}
	sort.Strings(ghosts)
	if len(ghosts) > 0 {
		fmt.Printf("  %s %s\n", yellow("!"), dim(refVol+" — in checksums.b3 but missing from disk:"))
		for _, f := range ghosts {
			fmt.Printf("    %s %s\n", yellow("!"), f)
		}
	}

	var totalMissing, totalExtra, scanErrors, coldChecked, coldGhostCount int
	for _, cold := range coldDrives {
		if cold.name() == refColdVol {
			continue // this drive is the reference, skip
		}
		coldChecked++
		coldDir := filepath.Join(cold.basePath(), cold.Root, yearStr, slug)
		if !dirExists(coldDir) {
			fmt.Printf("  %s %s\n", yellow(cold.name()), dim("(mission directory missing)"))
			totalMissing += len(refFiles)
			continue
		}
		coldFiles, err := findFiles(coldDir)
		if err != nil {
			fmt.Printf("%s scanning %s on %s: %v\n", red("ERROR"), slug, cold.name(), err)
			scanErrors++
			continue
		}
		coldSet := make(map[string]bool, len(coldFiles))
		for _, f := range coldFiles {
			coldSet[f.rel] = true
		}

		// check cold drive's own manifest for files gone missing from its disk
		var coldGhosts []string
		for rel := range readChecksumFile(filepath.Join(coldDir, "checksums.b3")) {
			if !coldSet[rel] {
				coldGhosts = append(coldGhosts, rel)
			}
		}
		sort.Strings(coldGhosts)

		var missing, extra []string
		for _, f := range refFiles {
			if !coldSet[f.rel] {
				missing = append(missing, f.rel)
			}
		}
		for _, f := range coldFiles {
			if !refSet[f.rel] {
				extra = append(extra, f.rel)
			}
		}
		if len(missing) > 0 || len(extra) > 0 || len(coldGhosts) > 0 {
			fmt.Printf("  %s\n", yellow(cold.name()))
			for _, f := range coldGhosts {
				fmt.Printf("    %s %s\n", yellow("!"), f)
			}
			for _, f := range missing {
				fmt.Printf("    %s %s\n", red("−"), f)
			}
			for _, f := range extra {
				fmt.Printf("    %s %s\n", dim("+"), f)
			}
			totalMissing += len(missing)
			totalExtra += len(extra)
			coldGhostCount += len(coldGhosts)
		}
	}

	if refColdVol != "" && coldChecked == 0 {
		fmt.Printf("%s %s exists on %s only — not found on any other mounted cold drive\n",
			yellow("!"), bold(slug), refVol)
		return false
	}
	if totalMissing == 0 && totalExtra == 0 && scanErrors == 0 && len(ghosts) == 0 && coldGhostCount == 0 {
		fmt.Printf("%s %s complete on all cold drives\n", green("✓"), bold(slug))
		return true
	}
	fmt.Println()
	if totalMissing > 0 {
		fmt.Printf("  %s files missing from cold drives", red(strconv.Itoa(totalMissing)))
		if totalExtra > 0 {
			fmt.Printf("  ·  %s extra files on cold drives", dim(strconv.Itoa(totalExtra)))
		}
		fmt.Println()
	} else if totalExtra > 0 {
		fmt.Printf("  %s extra files on cold drives\n", dim(strconv.Itoa(totalExtra)))
	}
	return false
}

func runCheckAll(cfg Config) bool {
	years := allYears(cfg)
	if len(years) == 0 {
		fmt.Println(dim("no missions found"))
		return true
	}
	ok := true
	for _, year := range years {
		fmt.Printf("%s\n\n", bold(strconv.Itoa(year)))
		if !runCheck(cfg, year) {
			ok = false
		}
		fmt.Println()
	}
	return ok
}

func runCheck(cfg Config, year int) bool {
	yearStr := strconv.Itoa(year)

	var hotDrives, coldDrives []DriveConfig
	for _, d := range cfg.Drives {
		mounted := dirExists(d.basePath())
		if d.Role == "hot" {
			if !mounted {
				fmt.Printf("%s %s %s\n", yellow("warning:"), bold(d.name()), dim("not mounted, skipping"))
				continue
			}
			hotDrives = append(hotDrives, d)
		} else if d.Role == "cold" {
			if !d.coversYear(year) {
				continue // out of scope for this year, silently skip
			}
			if !mounted {
				fmt.Printf("%s %s %s\n", yellow("warning:"), bold(d.name()), dim("not mounted, skipping"))
				continue
			}
			coldDrives = append(coldDrives, d)
		}
	}
	if len(hotDrives) == 0 {
		fmt.Println(red("no hot drive mounted"))
		return false
	}
	if len(coldDrives) == 0 {
		if anyColdMounted(cfg) {
			fmt.Printf(dim("no cold drives are scoped for %d\n"), year)
			return true
		}
		fmt.Println(red("no cold drives mounted"))
		return false
	}

	// union missions across all drives; hot wins as reference, cold fills gaps
	type refMission struct {
		dir     string
		vol     string
		coldVol string // non-empty when reference is a cold drive
	}
	refBySlug := make(map[string]refMission)
	for _, h := range hotDrives {
		hotYearDir := filepath.Join(h.basePath(), h.Root, yearStr)
		entries, err := os.ReadDir(hotYearDir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() && isNumberedMission(e.Name()) {
				if _, seen := refBySlug[e.Name()]; !seen {
					refBySlug[e.Name()] = refMission{
						dir: filepath.Join(hotYearDir, e.Name()),
						vol: h.name(),
					}
				}
			}
		}
	}
	for _, c := range coldDrives {
		coldYearDir := filepath.Join(c.basePath(), c.Root, yearStr)
		entries, err := os.ReadDir(coldYearDir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() && isNumberedMission(e.Name()) {
				if _, seen := refBySlug[e.Name()]; !seen {
					refBySlug[e.Name()] = refMission{
						dir:     filepath.Join(coldYearDir, e.Name()),
						vol:     c.name(),
						coldVol: c.name(),
					}
				}
			}
		}
	}

	var slugs []string
	for slug := range refBySlug {
		slugs = append(slugs, slug)
	}
	if len(slugs) == 0 {
		fmt.Printf(dim("no missions found for %d\n"), year)
		return true
	}
	sort.Strings(slugs)

	type gap struct {
		vol     string
		missing []string // missing from cold relative to reference
		extra   []string // extra on cold relative to reference
		ghosts  []string // in cold's checksums.b3 but absent from cold's disk
	}
	type missionReport struct {
		slug   string
		refVol string
		ghosts []string // in checksums.b3 but missing from disk on ref drive
		gaps   []gap
	}

	var reports []missionReport
	var totalMissing, totalExtra int

	for _, slug := range slugs {
		rm := refBySlug[slug]
		refFiles, err := findFiles(rm.dir)
		if err != nil {
			fmt.Printf("%s scanning %s on %s: %v\n", red("ERROR"), slug, rm.vol, err)
			continue
		}
		refSet := make(map[string]bool, len(refFiles))
		for _, f := range refFiles {
			refSet[f.rel] = true
		}

		// check manifest: files listed in checksums.b3 but absent from disk
		var ghosts []string
		manifest := readChecksumFile(filepath.Join(rm.dir, "checksums.b3"))
		for rel := range manifest {
			if !refSet[rel] {
				ghosts = append(ghosts, rel)
			}
		}
		sort.Strings(ghosts)

		var gaps []gap
		var coldChecked int
		for _, cold := range coldDrives {
			if cold.name() == rm.coldVol {
				continue // this drive is the reference, skip
			}
			coldChecked++
			coldDir := filepath.Join(cold.basePath(), cold.Root, yearStr, slug)
			if !dirExists(coldDir) {
				gaps = append(gaps, gap{
					vol:     cold.name(),
					missing: []string{"(mission directory missing)"},
				})
				totalMissing += len(refFiles)
				continue
			}
			coldFiles, err := findFiles(coldDir)
			if err != nil {
				fmt.Printf("%s scanning %s on %s: %v\n", red("ERROR"), slug, cold.name(), err)
				gaps = append(gaps, gap{
					vol:     cold.name(),
					missing: []string{"(scan error — could not verify)"},
				})
				continue
			}
			coldSet := make(map[string]bool, len(coldFiles))
			for _, f := range coldFiles {
				coldSet[f.rel] = true
			}

			// check cold drive's own manifest for files gone missing from its disk
			var coldGhosts []string
			for rel := range readChecksumFile(filepath.Join(coldDir, "checksums.b3")) {
				if !coldSet[rel] {
					coldGhosts = append(coldGhosts, rel)
				}
			}
			sort.Strings(coldGhosts)

			var missing, extra []string
			for _, f := range refFiles {
				if !coldSet[f.rel] {
					missing = append(missing, f.rel)
				}
			}
			for _, f := range coldFiles {
				if !refSet[f.rel] {
					extra = append(extra, f.rel)
				}
			}
			if len(missing) > 0 || len(extra) > 0 || len(coldGhosts) > 0 {
				gaps = append(gaps, gap{cold.name(), missing, extra, coldGhosts})
				totalMissing += len(missing)
				totalExtra += len(extra)
			}
		}

		if rm.coldVol != "" && coldChecked == 0 {
			gaps = append(gaps, gap{
				vol:     rm.vol,
				missing: []string{"(only copy — not found on any other mounted cold drive)"},
			})
		}

		if len(gaps) > 0 || len(ghosts) > 0 {
			reports = append(reports, missionReport{slug: slug, refVol: rm.vol, ghosts: ghosts, gaps: gaps})
		}
	}

	checked := len(slugs)
	incomplete := len(reports)
	fmt.Printf("checked %s in %d", bold(fmt.Sprintf("%d mission(s)", checked)), year)
	if incomplete == 0 {
		fmt.Printf("  %s\n", green("✓ all complete"))
		return true
	}
	fmt.Printf("  %s\n\n", yellow(fmt.Sprintf("%d incomplete", incomplete)))

	for _, r := range reports {
		fmt.Printf("  %s\n", bold(r.slug))
		if len(r.ghosts) > 0 {
			fmt.Printf("    %s\n", yellow(r.refVol+" — in checksums.b3 but missing from disk:"))
			for _, f := range r.ghosts {
				fmt.Printf("      %s %s\n", yellow("!"), f)
			}
		}
		for _, g := range r.gaps {
			fmt.Printf("    %s\n", yellow(g.vol))
			for _, f := range g.ghosts {
				fmt.Printf("      %s %s\n", yellow("!"), f)
			}
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
		fmt.Printf(dim("  run -sync to copy missing files to cold drives\n"))
	}
	return false
}
