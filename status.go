package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
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

	fmt.Println(bold("DRIVES"))
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
			dim(fmtSize(used)), dim(fmtSize(total)),
			tags)
	}

	// cards section
	fmt.Printf("\n%s\n", bold("CARDS"))
	cards := mountedCards(cfg)
	if len(cards) == 0 {
		fmt.Printf("  %s\n", dim("none mounted"))
	}
	for _, c := range cards {
		fmt.Printf("  %s  %s\n", c.Volume, dim("mounted"))
	}

	// missions section — same logic as runList
	fmt.Printf("\n%s  %d\n", bold("MISSIONS"), year)

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
			if !e.IsDir() || !isMissionDir(e.Name()) {
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
		fmt.Printf("  %s%-*s", bold(slug), maxSlug-len(slug), "")
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

// allYears returns all years found across all mounted drives, newest first.
func allYears(cfg Config) []int {
	yearSet := make(map[int]bool)
	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(base, d.Root))
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			if y, err := strconv.Atoi(e.Name()); err == nil && y >= 2000 && y <= 2099 {
				yearSet[y] = true
			}
		}
	}
	var years []int
	for y := range yearSet {
		years = append(years, y)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(years)))
	return years
}

// missionScan is what -list reports about one mission on one drive.
type missionScan struct {
	size        int64
	files       int
	checksummed bool // checksums.b3 exists and covers every file on this drive
}

// scanMissions walks each mission on each mounted drive, returning
// scan[slug][drive name]. Missions hold a handful of large video files rather
// than many small ones, so the walk stays cheap; the per-drive pools keep an
// HDD from being seek-thrashed by parallel walks of the same platter.
func scanMissions(drives []DriveConfig, yearStr string, slugs []string) map[string]map[string]missionScan {
	out := make(map[string]map[string]missionScan)
	var mu sync.Mutex
	var pools []*pool
	var submitters []func()
	for _, d := range drives {
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		wp := newPool(probeDrive(base).concurrency)
		pools = append(pools, wp)
		vol, root := d.name(), d.Root
		submitters = append(submitters, func() {
			for _, slug := range slugs {
				dir := filepath.Join(base, root, yearStr, slug)
				if !dirExists(dir) {
					continue
				}
				wp.run(func() {
					files, err := findFiles(dir)
					if err != nil {
						return
					}
					manifest := readChecksumFile(filepath.Join(dir, "checksums.b3"))
					sc := missionScan{checksummed: len(manifest) > 0}
					for _, f := range files {
						if f.rel == "checksums.b3" {
							continue // the manifest never lists itself
						}
						sc.size += f.size
						sc.files++
						if manifest[f.rel] == "" {
							sc.checksummed = false
						}
					}
					if sc.files == 0 {
						sc.checksummed = false
					}
					mu.Lock()
					if out[slug] == nil {
						out[slug] = make(map[string]missionScan)
					}
					out[slug][vol] = sc
					mu.Unlock()
				})
			}
		})
	}
	submitAll(submitters)
	for _, wp := range pools {
		wp.wait()
	}
	return out
}

// listCell centres a one-character marker in a column. The marker carries ANSI
// colour, so it is padded by hand — %-*s would count the escape bytes.
func listCell(marker string, width int) string {
	if width <= 1 {
		return marker
	}
	left := (width - 1) / 2
	return strings.Repeat(" ", left) + marker + strings.Repeat(" ", width-1-left)
}

// listMarker renders one mission/drive cell: checksummed, present but not
// checksummed, or absent.
func listMarker(sc missionScan, present, mounted bool) string {
	switch {
	case present && sc.checksummed:
		return green("✓")
	case present:
		return yellow("·")
	case mounted:
		return red("−")
	default:
		return dim("−")
	}
}

const listLegend = "✓ checksummed   · not checksummed   − absent"

func runListAll(cfg Config) {
	var driveNames []string
	mountedDrives := make(map[string]bool)
	for _, d := range cfg.Drives {
		driveNames = append(driveNames, d.name())
		if dirExists(d.basePath()) {
			mountedDrives[d.name()] = true
		}
	}

	years := allYears(cfg)
	if len(years) == 0 {
		fmt.Println(dim("no missions found"))
		return
	}

	// column width for drive names
	maxName := 0
	for _, name := range driveNames {
		if len(name) > maxName {
			maxName = len(name)
		}
	}

	for i, year := range years {
		yearStr := strconv.Itoa(year)

		missionDrives := make(map[string]map[string]bool)
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
			for _, e := range entries {
				if !e.IsDir() || !isMissionDir(e.Name()) {
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
			continue
		}
		sort.Strings(allSlugs)

		maxSlug := 0
		for _, s := range allSlugs {
			if len(s) > maxSlug {
				maxSlug = len(s)
			}
		}

		scans := scanMissions(cfg.Drives, yearStr, allSlugs)
		sizes := make(map[string]string, len(allSlugs))
		maxSize := 0
		var yearTotal int64
		for _, slug := range allSlugs {
			size := missionSize(scans[slug], driveNames)
			yearTotal += size
			sizes[slug] = fmtSize(uint64(size))
			if len(sizes[slug]) > maxSize {
				maxSize = len(sizes[slug])
			}
		}

		if i > 0 {
			fmt.Println()
		}
		fmt.Printf("%s  %s\n", bold(yearStr),
			dim(fmt.Sprintf("%d missions · %s", len(allSlugs), fmtSize(uint64(yearTotal)))))

		// header row
		fmt.Printf("  %-*s  %*s", maxSlug, "", maxSize, "size")
		for _, name := range driveNames {
			fmt.Printf("  %s", name)
		}
		fmt.Println()

		for _, slug := range allSlugs {
			drives := missionDrives[slug]
			allPresent := true
			for _, name := range driveNames {
				if mountedDrives[name] && !drives[name] {
					allPresent = false
					break
				}
			}
			label := bold(slug)
			if !allPresent {
				label = yellow(slug)
			}
			fmt.Printf("  %s%-*s  %s", label, maxSlug-len(slug), "",
				dim(fmt.Sprintf("%*s", maxSize, sizes[slug])))
			for _, name := range driveNames {
				fmt.Printf("  %s", listCell(listMarker(scans[slug][name], drives[name], mountedDrives[name]), len(name)))
			}
			fmt.Println()
		}
	}
	fmt.Printf("\n%s\n", dim(listLegend))
}

// missionSize returns the mission's size from the first drive that has it.
// Copies should agree; -check is the tool for finding out when they do not.
func missionSize(byDrive map[string]missionScan, order []string) int64 {
	for _, name := range order {
		if sc, ok := byDrive[name]; ok {
			return sc.size
		}
	}
	return 0
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
			if !e.IsDir() || !isMissionDir(e.Name()) {
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
	maxSlug := len("mission")
	for _, s := range allSlugs {
		if len(s) > maxSlug {
			maxSlug = len(s)
		}
	}

	scans := scanMissions(cfg.Drives, yearStr, allSlugs)
	sizes := make(map[string]string, len(allSlugs))
	maxSize := len("size")
	var total int64
	for _, slug := range allSlugs {
		size := missionSize(scans[slug], driveNames)
		total += size
		sizes[slug] = fmtSize(uint64(size))
		if len(sizes[slug]) > maxSize {
			maxSize = len(sizes[slug])
		}
	}

	header := fmt.Sprintf("%-*s  %*s  %s", maxSlug, "mission", maxSize, "size", strings.Join(driveNames, "  "))
	fmt.Println(header)
	fmt.Printf("%s\n", strings.Repeat("─", len(header)))
	for _, slug := range allSlugs {
		drives := missionDrives[slug]
		var cols []string
		for _, name := range driveNames {
			cols = append(cols, listCell(listMarker(scans[slug][name], drives[name], true), len(name)))
		}
		fmt.Printf("%-*s  %s  %s\n", maxSlug, slug,
			dim(fmt.Sprintf("%*s", maxSize, sizes[slug])), strings.Join(cols, "  "))
	}
	fmt.Printf("\n%s\n", dim(fmt.Sprintf("%d missions · %s", len(allSlugs), fmtSize(uint64(total)))))
	fmt.Printf("%s\n", dim(listLegend))
}
