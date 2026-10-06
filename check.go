package main

import (
	"fmt"
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

// hashConflict is one file whose recorded hash differs between two drives.
type hashConflict struct {
	rel       string
	refHash   string
	otherHash string
}

// manifestConflicts compares two checksums.b3 manifests, returning the files
// whose recorded hashes disagree. Only files listed in both are compared — one
// absent from either manifest is a missing, extra or ghost finding instead.
//
// This is the check -verify cannot make: it holds each drive to its own
// manifest, so two copies that differ but are each self-consistent both pass.
// Comparing the stored manifests costs nothing beyond reading them.
func manifestConflicts(ref, other map[string]string) []hashConflict {
	var out []hashConflict
	for rel, hash := range ref {
		if rel == "checksums.b3" {
			continue // a manifest never describes itself; legacy entries do not compare
		}
		if h, ok := other[rel]; ok && h != hash {
			out = append(out, hashConflict{rel, hash, h})
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].rel < out[j].rel })
	return out
}

// shortHash trims a hash for display; a manifest may hold an unexpected value.
func shortHash(h string) string {
	if len(h) > 8 {
		return h[:8]
	}
	return h
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
			fmt.Printf("%s mission %03d not found\n", red("ERROR"), missionNum)
			return false
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
		fmt.Printf("%s mission %03d not found\n", red("ERROR"), missionNum)
		return false
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
		fmt.Printf("%s mission %s not found on any drive\n", red("ERROR"), slug)
		return false
	}

	refFiles, err := contentFiles(refDir)
	if err != nil {
		fmt.Printf("%s scanning %s on %s: %v\n", red("ERROR"), slug, refVol, err)
		return false
	}

	// Name the mission the first time this run has something to report, so the
	// per-drive detail below is attributable when several missions are checked.
	// The all-clear line names the mission itself, so it needs no heading.
	headed := false
	header := func() {
		if !headed {
			headed = true
			fmt.Printf("%s\n", bold(slug))
		}
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
		header()
		fmt.Printf("  %s %s\n", yellow("!"), dim(refVol+" — in checksums.b3 but missing from disk:"))
		for _, f := range ghosts {
			fmt.Printf("    %s %s\n", yellow("!"), f)
		}
	}

	var totalMissing, totalExtra, scanErrors, coldChecked, coldGhostCount, totalConflicts, totalSized int
	for _, cold := range checkTargets(hotDrives, coldDrives, refVol, yearStr, slug) {
		if cold.Role == "cold" {
			coldChecked++
		}
		coldDir := filepath.Join(cold.basePath(), cold.Root, yearStr, slug)
		if !dirExists(coldDir) {
			header()
			fmt.Printf("  %s %s\n", yellow(cold.name()), dim("(mission directory missing)"))
			totalMissing += len(refFiles)
			continue
		}
		coldFiles, err := contentFiles(coldDir)
		if err != nil {
			header()
			fmt.Printf("%s scanning %s on %s: %v\n", red("ERROR"), slug, cold.name(), err)
			scanErrors++
			continue
		}
		coldSet := make(map[string]bool, len(coldFiles))
		for _, f := range coldFiles {
			coldSet[f.rel] = true
		}

		coldManifest := readChecksumFile(filepath.Join(coldDir, "checksums.b3"))

		// check cold drive's own manifest for files gone missing from its disk
		var coldGhosts []string
		for rel := range coldManifest {
			if !coldSet[rel] {
				coldGhosts = append(coldGhosts, rel)
			}
		}
		sort.Strings(coldGhosts)

		conflicts := manifestConflicts(manifest, coldManifest)

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
		// A file on both drives at different sizes is not the same file, and
		// presence alone used to read as complete.
		_, sized := planCopy(refFiles, coldFiles)
		if len(missing) > 0 || len(extra) > 0 || len(coldGhosts) > 0 || len(conflicts) > 0 || len(sized) > 0 {
			header()
			fmt.Printf("  %s\n", yellow(cold.name()))
			for _, c := range sized {
				fmt.Printf("    %s %s  %s\n", red("≠"), c.rel,
					dim(fmt.Sprintf("%s %d bytes · %s %d bytes", refVol, c.want, cold.name(), c.present)))
			}
			for _, f := range coldGhosts {
				fmt.Printf("    %s %s\n", yellow("!"), f)
			}
			for _, c := range conflicts {
				fmt.Printf("    %s %s  %s\n", red("≠"), c.rel,
					dim(fmt.Sprintf("%s %s · %s %s", refVol, shortHash(c.refHash), cold.name(), shortHash(c.otherHash))))
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
			totalConflicts += len(conflicts)
			totalSized += len(sized)
		}
	}

	if refColdVol != "" && coldChecked == 0 {
		fmt.Printf("%s %s exists on %s only — not found on any other mounted cold drive\n",
			yellow("!"), bold(slug), refVol)
		return false
	}
	if totalMissing == 0 && totalExtra == 0 && scanErrors == 0 && len(ghosts) == 0 && coldGhostCount == 0 && totalConflicts == 0 && totalSized == 0 {
		fmt.Printf("%s %s complete on every copy\n", green("✓"), bold(slug))
		return true
	}
	fmt.Println()
	if totalMissing > 0 {
		fmt.Printf("  %s file(s) missing from other copies", red(strconv.Itoa(totalMissing)))
		if totalExtra > 0 {
			fmt.Printf("  ·  %s extra file(s) on other copies", dim(strconv.Itoa(totalExtra)))
		}
		fmt.Println()
	} else if totalExtra > 0 {
		fmt.Printf("  %s extra file(s) on other copies\n", dim(strconv.Itoa(totalExtra)))
	}
	if totalConflicts > 0 || totalSized > 0 {
		fmt.Printf("  %s file(s) differ between drives\n", red(strconv.Itoa(totalConflicts+totalSized)))
		fmt.Printf("%s\n", dim(fmt.Sprintf("  run -verify %03d to find which copy is wrong", missionNum)))
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
		for _, slug := range missionDirs(hotYearDir) {
			if _, seen := refBySlug[slug]; !seen {
				refBySlug[slug] = refMission{
					dir: filepath.Join(hotYearDir, slug),
					vol: h.name(),
				}
			}
		}
	}
	for _, c := range coldDrives {
		coldYearDir := filepath.Join(c.basePath(), c.Root, yearStr)
		for _, slug := range missionDirs(coldYearDir) {
			if _, seen := refBySlug[slug]; !seen {
				refBySlug[slug] = refMission{
					dir:     filepath.Join(coldYearDir, slug),
					vol:     c.name(),
					coldVol: c.name(),
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
		vol       string
		missing   []string // missing from cold relative to reference
		extra     []string // extra on cold relative to reference
		ghosts    []string // in cold's checksums.b3 but absent from cold's disk
		conflicts []hashConflict
		sized     []sizeConflict // on both, at different sizes
	}
	type missionReport struct {
		slug   string
		refVol string
		ghosts []string // in checksums.b3 but missing from disk on ref drive
		gaps   []gap
	}

	var reports []missionReport
	var totalMissing, totalExtra, totalConflicts int

	for _, slug := range slugs {
		rm := refBySlug[slug]
		refFiles, err := contentFiles(rm.dir)
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
		for _, cold := range checkTargets(hotDrives, coldDrives, rm.vol, yearStr, slug) {
			if cold.Role == "cold" {
				coldChecked++
			}
			coldDir := filepath.Join(cold.basePath(), cold.Root, yearStr, slug)
			if !dirExists(coldDir) {
				gaps = append(gaps, gap{
					vol:     cold.name(),
					missing: []string{"(mission directory missing)"},
				})
				totalMissing += len(refFiles)
				continue
			}
			coldFiles, err := contentFiles(coldDir)
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

			coldManifest := readChecksumFile(filepath.Join(coldDir, "checksums.b3"))

			// check cold drive's own manifest for files gone missing from its disk
			var coldGhosts []string
			for rel := range coldManifest {
				if !coldSet[rel] {
					coldGhosts = append(coldGhosts, rel)
				}
			}
			sort.Strings(coldGhosts)

			conflicts := manifestConflicts(manifest, coldManifest)

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
			_, sized := planCopy(refFiles, coldFiles)
			if len(missing) > 0 || len(extra) > 0 || len(coldGhosts) > 0 || len(conflicts) > 0 || len(sized) > 0 {
				gaps = append(gaps, gap{cold.name(), missing, extra, coldGhosts, conflicts, sized})
				totalMissing += len(missing)
				totalExtra += len(extra)
				totalConflicts += len(conflicts) + len(sized)
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
			for _, c := range g.conflicts {
				fmt.Printf("      %s %s  %s\n", red("≠"), c.rel,
					dim(fmt.Sprintf("%s %s · %s %s", r.refVol, shortHash(c.refHash), g.vol, shortHash(c.otherHash))))
			}
			for _, c := range g.sized {
				fmt.Printf("      %s %s  %s\n", red("≠"), c.rel,
					dim(fmt.Sprintf("%s %d bytes · %s %d bytes", r.refVol, c.want, g.vol, c.present)))
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
		fmt.Printf("  %s file(s) missing from other copies", red(strconv.Itoa(totalMissing)))
		if totalExtra > 0 {
			fmt.Printf("  ·  %s extra file(s) on other copies", dim(strconv.Itoa(totalExtra)))
		}
		fmt.Println()
		fmt.Print(dim("  run -sync (cold) or -copy (hot) to fill in missing files\n"))
	}
	if totalConflicts > 0 {
		fmt.Printf("  %s file(s) differ between drives, by recorded hash or by size\n",
			red(strconv.Itoa(totalConflicts)))
		fmt.Print(dim("  run -verify on the affected missions to find which copy is wrong\n"))
	}
	return false
}

// checkTargets lists the copies a mission's reference copy is compared with:
// every other hot drive that holds the mission, then every cold drive scoped
// for the year.
//
// Only the cold drives used to be compared, so a second hot copy — T7 beside
// T9 — was never compared with anything: a file missing from it, or a
// different file under the same name, went unreported until it was the copy
// something was read from. A hot drive is not expected to hold every mission,
// so one without the mission is not a gap; one that holds part of it is.
func checkTargets(hot, cold []DriveConfig, refVol, yearStr, slug string) []DriveConfig {
	var out []DriveConfig
	for _, h := range hot {
		if h.name() != refVol && dirExists(filepath.Join(h.basePath(), h.Root, yearStr, slug)) {
			out = append(out, h)
		}
	}
	for _, c := range cold {
		if c.name() != refVol {
			out = append(out, c)
		}
	}
	return out
}
