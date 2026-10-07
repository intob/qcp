package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// verifiedNote renders a drive's verification summary for the DRIVES table.
func verifiedNote(summary string) string {
	if summary == "" {
		return ""
	}
	return "  " + dim("· "+summary)
}

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
			// Not mounted: the catalog's last reading, dimmed and dated, since
			// it is a memory of the drive rather than a reading of it.
			c := readCatalog(d.name())
			var verified string
			seen := c.Space.Seen
			if cy, ok := c.Years[yearStr]; ok {
				verified = cy.verified().String()
				if seen.IsZero() {
					seen = cy.Scanned
				}
			}
			state := "not mounted"
			if !seen.IsZero() {
				state += " · seen " + lastSeen(seen)
			}
			if sp := c.Space; sp.Total > 0 {
				fmt.Printf("  %s  %s  %s  %s%s\n", name,
					dim(driveSpaceBar(sp.used(), sp.Total, barWidth)),
					dim(fmt.Sprintf("%s / %s · %s", fmtSize(sp.used()), fmtSize(sp.Total), state)),
					tags, verifiedNote(verified))
				continue
			}
			fmt.Printf("  %s  %-*s  %s%s\n", name, barWidth, state, tags, verifiedNote(verified))
			continue
		}
		sp, err := readDriveSpace(base)
		if err != nil {
			fmt.Printf("  %s  %-*s  %s\n", name, barWidth, "?", tags)
			continue
		}
		bar := driveSpaceBar(sp.used(), sp.Total, barWidth)
		fmt.Printf("  %s  %s  %s / %s  %s%s\n",
			name, bar,
			dim(fmtSize(sp.used())), dim(fmtSize(sp.Total)),
			tags, verifiedNote(yearVerified(filepath.Join(base, d.Root, yearStr)).String()))
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

	away := catalogued(cfg, year)
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

	addCatalogued(away, missionDrives, nil)
	for slug := range missionDrives {
		if !seen[slug] {
			allSlugs = append(allSlugs, slug)
			seen[slug] = true
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
			_, isAway := away[name]
			switch {
			case drives[name] && isAway:
				fmt.Printf("  %s", dim(name)) // as last seen
			case drives[name]:
				fmt.Printf("  %-*s", len(name), name)
			default:
				fmt.Printf("  %-*s", len(name), "--")
			}
		}
		fmt.Println()
	}
	if note := catalogNote(away, driveNames); note != "" {
		fmt.Printf("\n  %s\n", dim(note))
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
// checksummed, or absent. A drive that is not mounted is shown as the catalog
// last saw it, dimmed, since that is a memory rather than a reading.
func listMarker(sc missionScan, present, mounted bool) string {
	switch {
	case present && !mounted && sc.checksummed:
		return dim("✓")
	case present && !mounted:
		return dim("·")
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

// catalogNote names the unmounted drives whose columns come from the catalog,
// and when each was last seen with the year.
func catalogNote(away map[string]catalogYear, order []string) string {
	var parts []string
	for _, name := range order {
		if cy, ok := away[name]; ok {
			parts = append(parts, fmt.Sprintf("%s as last seen %s", name, lastSeen(cy.Scanned)))
		}
	}
	if len(parts) == 0 {
		return ""
	}
	return "not mounted, dimmed: " + strings.Join(parts, ", ")
}

// addCatalogued folds the catalogued drives for a year into a listing's
// presence map and scans, and returns any missions only they hold.
func addCatalogued(away map[string]catalogYear, missionDrives map[string]map[string]bool,
	scans map[string]map[string]missionScan) {
	for name, cy := range away {
		for slug := range cy.Missions {
			if missionDrives[slug] == nil {
				missionDrives[slug] = make(map[string]bool)
			}
			missionDrives[slug][name] = true
			if scans != nil {
				sc, _ := cy.scan(slug)
				if scans[slug] == nil {
					scans[slug] = make(map[string]missionScan)
				}
				scans[slug][name] = sc
			}
		}
	}
}

func runListAll(cfg Config) {
	var driveNames []string
	mountedDrives := make(map[string]bool)
	for _, d := range cfg.Drives {
		driveNames = append(driveNames, d.name())
		if dirExists(d.basePath()) {
			mountedDrives[d.name()] = true
		}
	}

	// Years only an unmounted drive holds still get listed, from the catalog.
	yearSet := make(map[int]bool)
	for _, y := range allYears(cfg) {
		yearSet[y] = true
	}
	for _, d := range cfg.Drives {
		if mountedDrives[d.name()] {
			continue
		}
		for k, cy := range readCatalog(d.name()).Years {
			if y, err := strconv.Atoi(k); err == nil && len(cy.Missions) > 0 {
				yearSet[y] = true
			}
		}
	}
	var years []int
	for y := range yearSet {
		years = append(years, y)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(years)))
	if len(years) == 0 {
		fmt.Println(dim("no missions found"))
		return
	}
	var notes []string

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
		away := catalogued(cfg, year)
		addCatalogued(away, missionDrives, nil)
		for slug := range missionDrives {
			if !seen[slug] {
				allSlugs = append(allSlugs, slug)
				seen[slug] = true
			}
		}
		if len(allSlugs) == 0 {
			continue
		}
		sort.Strings(allSlugs)
		if note := catalogNote(away, driveNames); note != "" {
			notes = append(notes, yearStr+": "+note)
		}

		maxSlug := 0
		for _, s := range allSlugs {
			if len(s) > maxSlug {
				maxSlug = len(s)
			}
		}

		scans := scanMissions(cfg.Drives, yearStr, allSlugs)
		addCatalogued(away, map[string]map[string]bool{}, scans)
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
				_, known := away[name]
				if (mountedDrives[name] || known) && !drives[name] {
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
	for _, n := range notes {
		fmt.Printf("%s\n", dim(n))
	}
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

	away := catalogued(cfg, year)
	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			if _, ok := away[d.name()]; ok {
				driveNames = append(driveNames, d.name())
			}
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

	addCatalogued(away, missionDrives, nil)
	for slug := range missionDrives {
		if !seen[slug] {
			allSlugs = append(allSlugs, slug)
			seen[slug] = true
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
	addCatalogued(away, map[string]map[string]bool{}, scans)
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
			_, isAway := away[name]
			cols = append(cols, listCell(listMarker(scans[slug][name], drives[name], !isAway), len(name)))
		}
		fmt.Printf("%-*s  %s  %s\n", maxSlug, slug,
			dim(fmt.Sprintf("%*s", maxSize, sizes[slug])), strings.Join(cols, "  "))
	}
	fmt.Printf("\n%s\n", dim(fmt.Sprintf("%d missions · %s", len(allSlugs), fmtSize(uint64(total)))))
	fmt.Printf("%s\n", dim(listLegend))
	if note := catalogNote(away, driveNames); note != "" {
		fmt.Printf("%s\n", dim(note))
	}
}

// ── proxy coverage ──────────────────────────────────────────────────────────

// -proxies is the derived tier's counterpart to -list: it answers "what is
// browsable?". Footage wants a copy on every drive and -list marks a gap as a
// problem; proxies are regenerable and never archived, so a mission holding no
// tree is a fact, not a fault. What matters instead is how much of the mission
// a tree covers and whether the footage has moved on since it was built.

// proxyScan is one mission's proxy state, measured against the tree that covers
// the most clips — copies of a tree are not expected to agree, and the fullest
// one is what -index and Resolve would actually use.
type proxyScan struct {
	clips       int      // source clips, counted on the drive -proxy would read
	browse      int      // of those, the ones with a browse rendition on disk
	edit        int      // likewise for the edit tier
	staleBrowse int      // on disk, but -proxy would rebuild them
	staleEdit   int      // likewise
	drives      []string // every mounted drive holding a tree for this mission
	unchecked   bool     // footage not mounted, so counts come from the manifest
}

// scanProxyMission measures one mission. The verdict on what is out of date
// comes from planMission — the same function -proxy plans with — so the report
// cannot drift from what a run would actually do. Reproducing its rules here
// with a cheaper approximation was tried and was wrong on real trees: it missed
// every rendition whose transform had been reselected, which on a library with
// a `look` configured is the single largest cause of a rebuild.
//
// It stays cheap because planMission only reads a sidecar for a clip it cannot
// take from the manifest, and a clip that is merely out of date is still taken
// from the manifest. Missions with no tree at all skip planning entirely, which
// is where reading every sidecar would otherwise cost something.
func scanProxyMission(cfg Config, year int, yearStr, slug string, look colourTransform) proxyScan {
	var sc proxyScan

	// Find every tree, and keep the fullest to measure. A tree left behind by
	// an interrupted run has no manifest at all, so seed the best-so-far below
	// zero to make sure the first one still counts as found.
	bestClips, bestDir := -1, ""
	var bestMan proxyManifest
	for _, d := range cfg.Drives {
		if !dirExists(d.basePath()) {
			continue
		}
		dir := proxyMissionDir(d.basePath(), year, slug)
		if !dirExists(dir) {
			continue
		}
		sc.drives = append(sc.drives, d.name())
		if m := readProxyManifest(dir); len(m.Clips) > bestClips {
			bestClips, bestDir, bestMan = len(m.Clips), dir, m
		}
	}

	src, err := proxySourceForSlug(cfg, yearStr, slug)
	if err != nil {
		// Proxies outlive the footage they came from: the browse tier lives on
		// a hot drive precisely so a mission can be evicted to cold and stay
		// browsable. With nothing to count against, report what the manifest
		// claims and mark the row as unchecked rather than guessing.
		if bestDir == "" {
			return sc
		}
		sc.unchecked = true
		sc.clips = len(bestMan.Clips)
		for _, c := range bestMan.Clips {
			if c.Browse != "" && fileExists(filepath.Join(bestDir, c.Browse)) {
				sc.browse++
			}
			if c.Edit != "" && fileExists(filepath.Join(bestDir, c.Edit)) {
				sc.edit++
			}
		}
		return sc
	}

	sc.clips = len(src.clips)
	if bestDir == "" {
		return sc
	}
	// Both tiers, so the edit column reports on its own terms rather than
	// inheriting the browse tier's verdict. planMission keeps them independent.
	plan := planMission(src, bestDir, proxyTiers{browse: true, edit: true}, look)
	for _, j := range plan.jobs {
		// A rendition that is planned for work but is not on disk is missing,
		// not stale — it is already counted by the shortfall against clips.
		//
		// The poster and sprite count as part of the browse tier rather than as
		// a column of their own: they are generated with it, and the index
		// needs them to show the clip at all, so a video with no stills beside
		// it is not a browse tier anyone can use.
		if fileExists(filepath.Join(bestDir, proxyRel("browse", j.rel, ".mp4"))) {
			sc.browse++
			if j.needBrow || j.needStil {
				sc.staleBrowse++
			}
		}
		if fileExists(filepath.Join(bestDir, proxyRel("edit", j.rel, ".mov"))) {
			sc.edit++
			if j.needEdit {
				sc.staleEdit++
			}
		}
	}
	return sc
}

// proxyScanWorkers bounds how many missions are measured at once. Each mission
// walks the footage on every mounted drive, so this caps concurrent walks of
// the same platter rather than concurrent drives — a few is enough to hide the
// latency of the small reads without turning an archive HDD into a seek storm.
const proxyScanWorkers = 4

func scanProxies(cfg Config, year int, slugs []string, look colourTransform) map[string]proxyScan {
	yearStr := strconv.Itoa(year)
	out := make(map[string]proxyScan, len(slugs))
	var mu sync.Mutex
	wp := newPool(proxyScanWorkers)
	for _, slug := range slugs {
		wp.run(func() {
			sc := scanProxyMission(cfg, year, yearStr, slug, look)
			mu.Lock()
			out[slug] = sc
			mu.Unlock()
		})
	}
	wp.wait()
	return out
}

// reportLook resolves the configured look once for a whole report, the way a
// -proxy run does. A look that cannot be read is worth saying out loud rather
// than passing over: it is what -proxy would fail on, and every clip it should
// have applied to would otherwise be reported as needing a rebuild.
func reportLook(cfg Config) colourTransform {
	if cfg.Look == "" {
		return colourTransform{}
	}
	look, err := lookTransform(cfg.Look)
	if err != nil {
		fmt.Printf("  %s  %s\n", yellow("⚠"), dim(err.Error()))
		return colourTransform{}
	}
	return look
}

// proxyMissionSlugs lists every mission in the year that has footage or a proxy
// tree on a mounted drive. Both halves are needed: a mission evicted to an
// unmounted cold drive still has a browsable tree, and a mission just ingested
// has no tree yet — and each is something -proxies exists to show.
func proxyMissionSlugs(cfg Config, year int) []string {
	yearStr := strconv.Itoa(year)
	var slugs []string
	seen := make(map[string]bool)
	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		for _, dir := range []string{
			filepath.Join(base, d.Root, yearStr),
			filepath.Join(proxyRoot(base), yearStr),
		} {
			entries, err := os.ReadDir(dir)
			if err != nil {
				continue
			}
			for _, e := range entries {
				if !e.IsDir() || !isMissionDir(e.Name()) || seen[e.Name()] {
					continue
				}
				seen[e.Name()] = true
				slugs = append(slugs, e.Name())
			}
		}
	}
	sort.Strings(slugs)
	return slugs
}

// proxyYears is allYears widened to the years that exist only under proxies/,
// for the same reason proxyMissionSlugs looks there.
func proxyYears(cfg Config) []int {
	seen := make(map[int]bool)
	var years []int
	for _, y := range allYears(cfg) {
		seen[y] = true
		years = append(years, y)
	}
	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		entries, err := os.ReadDir(proxyRoot(base))
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() {
				continue
			}
			if y, err := strconv.Atoi(e.Name()); err == nil && y >= 2000 && y <= 2099 && !seen[y] {
				seen[y] = true
				years = append(years, y)
			}
		}
	}
	sort.Sort(sort.Reverse(sort.IntSlice(years)))
	return years
}

// proxyTierCell renders one tier's coverage as a count and a marker. The marker
// answers only "does every clip have one", because the count that matters for
// currency has a column of its own: a mission where one clip of 166 needs
// rebuilding and one where all 166 do are not the same state, and no single
// marker tells them apart. Stale work still tints the cell, so a fully covered
// tier that is entirely out of date cannot read as green and finished.
func proxyTierCell(have, total, stale int, unchecked bool) (plain, coloured string) {
	if have == 0 {
		return "−", dim("−")
	}
	marker, paint := "✓", green
	switch {
	case unchecked:
		marker, paint = "?", dim
	case have < total:
		marker, paint = "·", yellow
	case stale > 0:
		paint = yellow
	}
	plain = fmt.Sprintf("%d %s", have, marker)
	return plain, paint(plain)
}

// proxyStaleCell renders the rebuild count across both tiers.
func proxyStaleCell(n int) (plain, coloured string) {
	if n == 0 {
		return "−", dim("−")
	}
	plain = strconv.Itoa(n)
	return plain, yellow(plain)
}

// padLeft right-aligns a cell in width columns, measuring the uncoloured text
// so the escape codes do not count towards the field.
func padLeft(plain, coloured string, width int) string {
	if n := width - len([]rune(plain)); n > 0 {
		return strings.Repeat(" ", n) + coloured
	}
	return coloured
}

const proxyLegend = "✓ every clip has one   · some do   − none   " +
	"? footage not mounted   stale = how many -proxy would rebuild"

// printProxyYear prints one year's block, reporting whether it found anything.
func printProxyYear(cfg Config, year int, look colourTransform) bool {
	slugs := proxyMissionSlugs(cfg, year)
	if len(slugs) == 0 {
		return false
	}
	scans := scanProxies(cfg, year, slugs, look)

	type row struct {
		slug                       string
		clips, browse, edit, stale string
		browseCol, editCol         string
		staleCol, drives           string
	}
	var rows []row
	var withTree, totalClips, totalBrowse, totalEdit, totalStale int

	for _, slug := range slugs {
		sc := scans[slug]
		if len(sc.drives) > 0 {
			withTree++
		}
		totalClips += sc.clips
		totalBrowse += sc.browse
		totalEdit += sc.edit
		totalStale += sc.staleBrowse + sc.staleEdit

		bp, bc := proxyTierCell(sc.browse, sc.clips, sc.staleBrowse, sc.unchecked)
		ep, ec := proxyTierCell(sc.edit, sc.clips, sc.staleEdit, sc.unchecked)
		sp, scol := proxyStaleCell(sc.staleBrowse + sc.staleEdit)
		drives := "−"
		if len(sc.drives) > 0 {
			drives = strings.Join(sc.drives, " ")
		}
		clips := "−"
		if sc.clips > 0 {
			clips = strconv.Itoa(sc.clips)
		}
		rows = append(rows, row{
			slug: slug, clips: clips,
			browse: bp, browseCol: bc, edit: ep, editCol: ec,
			stale: sp, staleCol: scol, drives: drives,
		})
	}

	maxSlug, maxClips := len("mission"), len("clips")
	maxBrowse, maxEdit, maxStale := len("browse"), len("edit"), len("stale")
	for _, r := range rows {
		maxSlug = max(maxSlug, len(r.slug))
		maxClips = max(maxClips, len(r.clips))
		maxBrowse = max(maxBrowse, len([]rune(r.browse)))
		maxEdit = max(maxEdit, len([]rune(r.edit)))
		maxStale = max(maxStale, len([]rune(r.stale)))
	}

	summary := fmt.Sprintf("%d of %d missions proxied · %d of %d clips browsable",
		withTree, len(slugs), totalBrowse, totalClips)
	// The edit tier is an on-demand tool rather than a standing commitment, so
	// it is usually zero everywhere; say nothing about it until it exists.
	if totalEdit > 0 {
		summary += fmt.Sprintf(" · %d editable", totalEdit)
	}
	if totalStale > 0 {
		summary += fmt.Sprintf(" · %d to rebuild", totalStale)
	}
	fmt.Printf("%s  %s\n", bold(strconv.Itoa(year)), dim(summary))
	fmt.Printf("  %s\n", dim(fmt.Sprintf("%-*s  %*s  %*s  %*s  %*s  %s",
		maxSlug, "mission", maxClips, "clips", maxBrowse, "browse",
		maxEdit, "edit", maxStale, "stale", "proxies on")))
	for _, r := range rows {
		fmt.Printf("  %s%-*s  %s  %s  %s  %s  %s\n",
			bold(r.slug), maxSlug-len(r.slug), "",
			dim(fmt.Sprintf("%*s", maxClips, r.clips)),
			padLeft(r.browse, r.browseCol, maxBrowse),
			padLeft(r.edit, r.editCol, maxEdit),
			padLeft(r.stale, r.staleCol, maxStale),
			dim(r.drives))
	}
	return true
}

func runProxies(cfg Config, year int) {
	if !printProxyYear(cfg, year, reportLook(cfg)) {
		fmt.Printf("no missions found for %d\n", year)
		return
	}
	fmt.Printf("\n%s\n", dim(proxyLegend))
}

func runProxiesAll(cfg Config) {
	years := proxyYears(cfg)
	look := reportLook(cfg)
	printed := false
	for _, y := range years {
		if printed {
			fmt.Println()
		}
		if printProxyYear(cfg, y, look) {
			printed = true
		}
	}
	if !printed {
		fmt.Println(dim("no missions found"))
		return
	}
	fmt.Printf("\n%s\n", dim(proxyLegend))
}
