package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// datePatterns extracts YYYY MM DD from common camera filename conventions.
var datePatterns = []*regexp.Regexp{
	// 2023-04-15 or 2023_04_15 (with separators)
	regexp.MustCompile(`(\d{4})[_-](\d{2})[_-](\d{2})`),
	// 20230415 (8 consecutive digits, not part of a longer run)
	regexp.MustCompile(`(?:^|[^0-9])(\d{4})(\d{2})(\d{2})(?:[^0-9]|$)`),
}

type ffprobeOut struct {
	Format struct {
		Tags map[string]string `json:"tags"`
	} `json:"format"`
	Streams []struct {
		Tags map[string]string `json:"tags"`
	} `json:"streams"`
}

type fileWithDate struct {
	rel  string
	date time.Time
	src  string // "ffprobe", "filename", "mtime", or "" if none
}

type missionFile struct {
	f    fileWithDate
	dest string // flat filename within mission dir (collision-prefixed if needed)
}

type organiseMission struct {
	slug  string
	files []missionFile
	size  int64
}

type organisePlan struct {
	driveName string
	yearDir   string
	missions  []organiseMission
	unsorted  []fileWithDate
}

// seasonOrder returns the chronological position of a season within the year.
func seasonOrder(season string) int {
	return map[string]int{"Spring": 0, "Summer": 1, "Autumn": 2, "Winter": 3}[season]
}

// seasonKey returns the season name for a timestamp, e.g. "Summer".
// The year is omitted because it is already encoded in the year directory.
func seasonKey(t time.Time) string {
	switch {
	case t.Month() >= 3 && t.Month() <= 5:
		return "Spring"
	case t.Month() >= 6 && t.Month() <= 8:
		return "Summer"
	case t.Month() >= 9 && t.Month() <= 11:
		return "Autumn"
	default:
		return "Winter"
	}
}

func runOrganise(cfg Config, year int, skipConf bool, regroup bool) {
	yearStr := strconv.Itoa(year)

	seq, err := readSeq()
	if err != nil {
		exit(1, "err reading seq: %v", err)
	}

	// phase 1: find mounted drives that have a year directory
	type driveYear struct {
		d       DriveConfig
		yearDir string
	}
	var drives []driveYear
	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			fmt.Printf("%s %s %s\n", yellow("warning:"), bold(d.name()), dim("not mounted, skipping"))
			continue
		}
		yearDir := filepath.Join(base, d.Root, yearStr)
		if dirExists(yearDir) {
			drives = append(drives, driveYear{d, yearDir})
		}
	}
	if len(drives) == 0 {
		exit(1, "no drives with a %s directory mounted", yearStr)
	}

	// phase 2: scan all drives for unorganised files, collect unique seasons
	type driveFiles struct {
		driveYear
		files []fileWithDate
	}
	allSeasons := make(map[string]bool)
	var allDriveFiles []driveFiles
	for _, dy := range drives {
		fmt.Printf("%s %s\n", dim("scanning"), bold(dy.d.name()))
		files, err := scanUnorganised(dy.yearDir, regroup)
		if err != nil {
			fmt.Printf("%s scanning %s: %v\n", red("ERROR"), dy.d.name(), err)
			continue
		}
		allDriveFiles = append(allDriveFiles, driveFiles{dy, files})
	}
	sets := make([][]fileWithDate, len(allDriveFiles))
	for i := range allDriveFiles {
		sets[i] = allDriveFiles[i].files
	}
	agreeOnDates(sets)
	for _, df := range allDriveFiles {
		for _, f := range df.files {
			if f.src != "" {
				allSeasons[seasonKey(f.date.Local())] = true
			}
		}
	}

	if len(allSeasons) == 0 {
		fmt.Println("nothing to organise")
		return
	}

	// phase 3: assign slugs starting after the highest mission already in this year.
	nextNum := seq[year] + 1
	for _, df := range allDriveFiles {
		probe := make(map[int]int)
		scanYearDir(df.yearDir, year, probe)
		if probe[year]+1 > nextNum {
			nextNum = probe[year] + 1
		}
	}
	var seasons []string
	for s := range allSeasons {
		seasons = append(seasons, s)
	}
	sort.Slice(seasons, func(i, j int) bool {
		return seasonOrder(seasons[i]) < seasonOrder(seasons[j])
	})
	seasonSlug := make(map[string]string, len(seasons))
	for i, s := range seasons {
		seasonSlug[s] = fmt.Sprintf("%03d_%s", nextNum+i, s)
	}

	// phase 4: build and display the plan for every drive
	var plans []organisePlan
	for _, df := range allDriveFiles {
		p := buildOrganisePlan(df.yearDir, df.d.name(), df.files, seasonSlug)
		if len(p.missions) == 0 && len(p.unsorted) == 0 {
			fmt.Printf("%s: nothing to organise\n", bold(df.d.name()))
			continue
		}
		fmt.Printf("\n%s\n\n", bold(df.d.name()))
		printOrganisePlan(p)
		plans = append(plans, p)
	}

	if len(plans) == 0 {
		fmt.Println("nothing to organise")
		return
	}

	totalMissions := 0
	for _, p := range plans {
		totalMissions += len(p.missions)
	}
	fmt.Printf("\n%d drive(s), %d mission(s) total\n", len(plans), totalMissions)
	if !skipConf && !confirm() {
		return
	}

	// phase 5: execute on every drive
	var anyFailed bool
	for _, p := range plans {
		if !executeOrganisePlan(p) {
			anyFailed = true
		}
	}
	if anyFailed {
		fmt.Println(yellow("some files could not be moved — check errors above"))
	}

	if anyFailed {
		fmt.Println(yellow("seq not updated — re-run -organise after resolving errors"))
	} else {
		seq[year] = nextNum + len(seasons) - 1
		if err := writeSeq(seq); err != nil {
			fmt.Printf("%s writing seq: %v\n", red("ERROR"), err)
		}
	}
}

// scanUnorganised walks yearDir and extracts dates from all files that are not
// already inside a mission or underscore-prefixed folder — see skipOrganise for
// which of those a regroup descends into.
func scanUnorganised(yearDir string, regroup bool) ([]fileWithDate, error) {
	type rawFile struct{ path, rel, name string }
	var rawFiles []rawFile
	err := filepath.WalkDir(yearDir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			if path == yearDir {
				return nil
			}
			name := d.Name()
			if junkDirs[name] {
				return filepath.SkipDir
			}
			rel := strings.TrimPrefix(path, yearDir+string(os.PathSeparator))
			top := strings.SplitN(rel, string(os.PathSeparator), 2)[0]
			if strings.HasPrefix(top, "_") || skipOrganise(top, regroup) {
				return filepath.SkipDir
			}
			return nil
		}
		rel := strings.TrimPrefix(path, yearDir+string(os.PathSeparator))
		for _, part := range strings.Split(rel, string(os.PathSeparator)) {
			if strings.HasPrefix(part, ".") || junkDirs[part] {
				return nil
			}
		}
		if junkFiles[d.Name()] || metadataFiles[d.Name()] {
			return nil
		}
		rawFiles = append(rawFiles, rawFile{path, rel, d.Name()})
		return nil
	})
	if err != nil || len(rawFiles) == 0 {
		return nil, err
	}

	fmt.Printf("  extracting dates from %d files...\n", len(rawFiles))
	type scanResult struct {
		idx int
		fileWithDate
	}
	resultCh := make(chan scanResult, len(rawFiles))
	sem := make(chan struct{}, 8)
	var swg sync.WaitGroup
	for i, rf := range rawFiles {
		swg.Add(1)
		i, rf := i, rf
		go func() {
			defer swg.Done()
			sem <- struct{}{}
			defer func() { <-sem }()
			t, src := extractFileDate(rf.path, rf.name)
			resultCh <- scanResult{i, fileWithDate{rel: rf.rel, date: t, src: src}}
		}()
	}
	go func() { swg.Wait(); close(resultCh) }()

	files := make([]fileWithDate, len(rawFiles))
	var done int
	for r := range resultCh {
		files[r.idx] = r.fileWithDate
		done++
		fmt.Printf("\r  %d/%d", done, len(rawFiles))
	}
	fmt.Println()
	return files, nil
}

func buildOrganisePlan(yearDir, driveName string, files []fileWithDate, seasonSlug map[string]string) organisePlan {
	missionMap := make(map[string]*organiseMission)
	var missionOrder []string
	var unsorted []fileWithDate

	for _, f := range files {
		if f.src == "" {
			unsorted = append(unsorted, f)
			continue
		}
		slug := seasonSlug[seasonKey(f.date.Local())]
		if missionMap[slug] == nil {
			missionMap[slug] = &organiseMission{slug: slug}
			missionOrder = append(missionOrder, slug)
		}
		m := missionMap[slug]
		m.files = append(m.files, missionFile{f: f})
		if info, err := os.Stat(filepath.Join(yearDir, f.rel)); err == nil {
			m.size += info.Size()
		}
	}
	sort.Strings(missionOrder)

	// compute flat dest names per mission with collision detection
	var missions []organiseMission
	for _, slug := range missionOrder {
		m := missionMap[slug]
		count := make(map[string]int, len(m.files))
		for _, mf := range m.files {
			count[filepath.Base(mf.f.rel)]++
		}
		used := make(map[string]bool)
		for i, mf := range m.files {
			base := filepath.Base(mf.f.rel)
			if count[base] > 1 {
				// prefix with the full parent directory path to disambiguate
				if dir := filepath.Dir(mf.f.rel); dir != "." {
					base = strings.ReplaceAll(dir, string(os.PathSeparator), "_") + "_" + base
				}
			}
			// resolve any remaining collision (e.g. dir name contains _ and aligns
			// with a separator-replaced path) with a numeric suffix
			if used[base] {
				ext := filepath.Ext(base)
				stem := strings.TrimSuffix(base, ext)
				for n := 2; ; n++ {
					candidate := fmt.Sprintf("%s_%d%s", stem, n, ext)
					if !used[candidate] {
						base = candidate
						break
					}
				}
			}
			used[base] = true
			m.files[i].dest = base
		}
		missions = append(missions, *m)
	}
	return organisePlan{driveName: driveName, yearDir: yearDir, missions: missions, unsorted: unsorted}
}

func printOrganisePlan(p organisePlan) {
	for _, m := range p.missions {
		fmt.Printf("  %s  (%d files, %s)\n", bold(m.slug), len(m.files), dim(fmtSize(uint64(m.size))))
		shown := m.files
		if len(shown) > 4 {
			shown = shown[:4]
		}
		for _, mf := range shown {
			fmt.Printf("    [%-8s] %s\n", mf.f.src, mf.dest)
		}
		if len(m.files) > 4 {
			fmt.Printf("    ... and %d more\n", len(m.files)-4)
		}
	}
	if len(p.unsorted) > 0 {
		fmt.Printf("  _unsorted  (%d files — no date resolved)\n", len(p.unsorted))
		for _, f := range p.unsorted {
			fmt.Printf("    %s\n", filepath.Base(f.rel))
		}
	}
}

// manifestMoves carries checksums.b3 entries along with the files -organise
// moves. It used to delete the manifest of every directory a file moved into or
// out of, which threw away the hashes recorded when the footage was known good —
// for the files that moved and for every file that stayed — and the next
// -checksum recorded whatever was on disk by then. A rename does not change a
// file's content, so its recorded hash is still its hash under the new name.
//
// A manifest that cannot be read is never rewritten — that would keep only the
// part before the failed read — so its entries are not carried and it is left
// exactly as it was, and the run reports it.
type manifestMoves struct {
	yearDir   string
	manifests map[string]map[string]string // directory → its checksums.b3
	changed   map[string]bool
	broken    map[string]error // directory → why its checksums.b3 could not be read
}

func newManifestMoves(yearDir string) *manifestMoves {
	return &manifestMoves{yearDir, map[string]map[string]string{}, map[string]bool{}, map[string]error{}}
}

func (mm *manifestMoves) load(dir string) map[string]string {
	m, ok := mm.manifests[dir]
	if !ok {
		var err error
		m, err = readChecksums(filepath.Join(dir, "checksums.b3"))
		if err != nil {
			mm.broken[dir] = err
		}
		delete(m, "checksums.b3")
		mm.manifests[dir] = m
	}
	return m
}

// moved records that the file at rel (relative to the year directory) is now
// destDir/destName. A manifest describes the top-level directory it sits in,
// so the entry is looked up there, under the rest of the path.
func (mm *manifestMoves) moved(rel, destDir, destName string) {
	parts := strings.SplitN(rel, string(os.PathSeparator), 2)
	if len(parts) < 2 {
		return // loose at the top of the year: no manifest describes it
	}
	srcDir := filepath.Join(mm.yearDir, parts[0])
	src := mm.load(srcDir)
	mm.load(destDir)
	if mm.broken[srcDir] != nil || mm.broken[destDir] != nil {
		return
	}
	h, ok := src[parts[1]]
	if !ok {
		return
	}
	delete(src, parts[1])
	mm.load(destDir)[destName] = h
	mm.changed[srcDir] = true
	mm.changed[destDir] = true
}

// flush writes every manifest that changed. One left empty is removed, so the
// directory it was in can be collapsed if nothing else is left there.
func (mm *manifestMoves) flush() bool {
	ok := true
	for _, err := range mm.broken {
		fmt.Printf("%s %v — left as it was; its entries were not carried\n", red("ERROR"), err)
		ok = false
	}
	for dir := range mm.changed {
		// A verified stamp covers the manifest it was verified against, which
		// this rewrites, so it no longer counts; left behind it would also keep
		// an emptied mission's directory from being removed.
		if err := os.Remove(filepath.Join(dir, verifiedFileName)); err != nil && !os.IsNotExist(err) {
			fmt.Printf("%s %v\n", yellow("warning:"), err)
		}
		path := filepath.Join(dir, "checksums.b3")
		m := mm.manifests[dir]
		var err error
		if len(m) == 0 {
			if err = os.Remove(path); os.IsNotExist(err) {
				err = nil
			}
		} else {
			lines := make([]string, 0, len(m))
			for rel, h := range m {
				lines = append(lines, fmt.Sprintf("%s  %s", h, rel))
			}
			err = writeChecksums(path, lines)
		}
		if err != nil {
			fmt.Printf("%s updating %s: %v\n", red("ERROR"), path, err)
			ok = false
		}
	}
	return ok
}

func executeOrganisePlan(p organisePlan) bool {
	manifests := newManifestMoves(p.yearDir)
	var moveFailed int

	for _, m := range p.missions {
		destDir := filepath.Join(p.yearDir, m.slug)
		for _, mf := range m.files {
			src := filepath.Join(p.yearDir, mf.f.rel)
			dst := filepath.Join(destDir, mf.dest)
			if src == dst {
				continue
			}
			if err := os.MkdirAll(filepath.Dir(dst), 0777); err != nil {
				fmt.Printf("%s mkdir: %v\n", red("ERROR"), err)
				continue
			}
			if err := moveNoReplace(src, dst); err != nil {
				fmt.Printf("%s move %s: %v\n", red("ERROR"), mf.f.rel, err)
				moveFailed++
				continue
			}
			manifests.moved(mf.f.rel, destDir, mf.dest)
		}
	}
	unsortedUsed := make(map[string]bool)
	for _, f := range p.unsorted {
		src := filepath.Join(p.yearDir, f.rel)
		// use full rel path (separators replaced) to avoid basename collisions;
		// if separator-replacement itself creates a collision, add a numeric suffix
		safeName := strings.ReplaceAll(f.rel, string(os.PathSeparator), "_")
		conflicts := func(name string) bool {
			if unsortedUsed[name] {
				return true
			}
			_, err := os.Stat(filepath.Join(p.yearDir, "_unsorted", name))
			return err == nil
		}
		if conflicts(safeName) {
			ext := filepath.Ext(safeName)
			stem := strings.TrimSuffix(safeName, ext)
			for n := 2; ; n++ {
				candidate := fmt.Sprintf("%s_%d%s", stem, n, ext)
				if !conflicts(candidate) {
					safeName = candidate
					break
				}
			}
		}
		unsortedUsed[safeName] = true
		dst := filepath.Join(p.yearDir, "_unsorted", safeName)
		if src == dst {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0777); err != nil {
			fmt.Printf("%s mkdir: %v\n", red("ERROR"), err)
			continue
		}
		if err := moveNoReplace(src, dst); err != nil {
			fmt.Printf("%s move %s: %v\n", red("ERROR"), f.rel, err)
			moveFailed++
			continue
		}
		manifests.moved(f.rel, filepath.Join(p.yearDir, "_unsorted"), safeName)
	}

	if !manifests.flush() {
		moveFailed++
	}

	removeEmptyDirs(p.yearDir)
	if moveFailed > 0 {
		fmt.Printf("%s %s: organised %d mission(s), %d file(s) failed to move\n",
			yellow("!"), bold(p.driveName), len(p.missions), moveFailed)
		return false
	}
	fmt.Printf("%s %s: organised %d mission(s)\n", green("✓"), bold(p.driveName), len(p.missions))
	return true
}

func extractFileDate(path, name string) (time.Time, string) {
	if t, ok := ffprobeDate(path); ok {
		return t, "ffprobe"
	}
	if t, ok := filenameDate(strings.TrimSuffix(name, filepath.Ext(name))); ok {
		return t, "filename"
	}
	if info, err := os.Stat(path); err == nil {
		return info.ModTime(), "mtime"
	}
	return time.Time{}, ""
}

func ffprobeDate(path string) (time.Time, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "ffprobe",
		"-v", "quiet",
		"-print_format", "json",
		"-show_entries", "format_tags:stream_tags",
		path,
	).Output()
	if err != nil {
		return time.Time{}, false
	}
	var result ffprobeOut
	if err := json.Unmarshal(out, &result); err != nil {
		return time.Time{}, false
	}
	sources := []map[string]string{result.Format.Tags}
	for _, s := range result.Streams {
		sources = append(sources, s.Tags)
	}
	for _, tags := range sources {
		ct := tags["creation_time"]
		if ct == "" {
			continue
		}
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02T15:04:05.000000Z"} {
			if t, err := time.Parse(layout, ct); err == nil {
				return t, true
			}
		}
	}
	return time.Time{}, false
}

func filenameDate(base string) (time.Time, bool) {
	for _, pat := range datePatterns {
		m := pat.FindStringSubmatch(base)
		if m == nil {
			continue
		}
		sub := m[len(m)-3:]
		year, _ := strconv.Atoi(sub[0])
		month, _ := strconv.Atoi(sub[1])
		day, _ := strconv.Atoi(sub[2])
		if year < 1990 || year > 2100 || month < 1 || month > 12 || day < 1 || day > 31 {
			continue
		}
		return time.Date(year, time.Month(month), day, 0, 0, 0, 0, time.UTC), true
	}
	return time.Time{}, false
}

// skipOrganise reports whether a directory under the year is a mission the
// scan must leave alone rather than take apart into loose files.
//
// -organise groups what is *not* yet in a mission, so every mission is off
// limits to it — the predicate here was isNumberedMission, which requires
// n > 0, so 000_Edits read as loose files and had its contents dated by mtime
// and moved into NNN_Season. -reorganise does re-bucket missions, which is what
// it is for, but 000_* sits outside the numbering by construction — it is a
// named mission rather than a season's worth of footage — so it is left alone
// by both.
func skipOrganise(name string, regroup bool) bool {
	n, ok := parseMissionNum(name)
	if !ok {
		return false
	}
	return !regroup || n == 0
}

// isNumberedMission reports whether name can be addressed by a mission number.
// 000_* directories are missions but not addressable, so they fail this and
// pass isMissionDir.
func isNumberedMission(name string) bool {
	n, ok := parseMissionNum(name)
	return ok && n > 0
}

// isMissionDir reports whether name is a mission directory of any number,
// 000_* included. Listings use this so that a stray directory under the year —
// _unsorted, say — is not shown as a mission while 000_Edits still is.
func isMissionDir(name string) bool {
	_, ok := parseMissionNum(name)
	return ok
}

// missionDirs lists the mission directories directly under a year directory,
// sorted, 000_* included. Every command that enumerates whatever is on the
// drive goes through this, so they all agree on what a mission is; only the
// ones that resolve a mission *number* use isNumberedMission instead. An
// unreadable directory yields nothing, matching the per-site ReadDir errors it
// replaces.
func missionDirs(yearDir string) []string {
	entries, err := os.ReadDir(yearDir)
	if err != nil {
		return nil
	}
	var slugs []string
	for _, e := range entries {
		if e.IsDir() && isMissionDir(e.Name()) {
			slugs = append(slugs, e.Name())
		}
	}
	sort.Strings(slugs)
	return slugs
}

func parseMissionNum(name string) (int, bool) {
	parts := strings.SplitN(name, "_", 2)
	if len(parts) < 2 {
		return 0, false
	}
	n, err := strconv.Atoi(parts[0])
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}

func removeEmptyDirs(root string) {
	var dirs []string
	filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err == nil && d.IsDir() && path != root {
			dirs = append(dirs, path)
		}
		return nil
	})
	// deepest first so nested empty dirs collapse correctly
	sort.Slice(dirs, func(i, j int) bool { return len(dirs[i]) > len(dirs[j]) })
	for _, d := range dirs {
		entries, err := os.ReadDir(d)
		if err == nil && len(entries) == 0 {
			os.Remove(d)
		}
	}
}

// moveNoReplace renames src to dst, refusing if dst already exists. os.Rename
// silently replaces an existing file, and the plan only guards against
// collisions among the files it is moving, not with what is already on disk.
func moveNoReplace(src, dst string) error {
	if _, err := os.Lstat(dst); err == nil {
		return fmt.Errorf("%s already exists", dst)
	} else if !os.IsNotExist(err) {
		return err
	}
	return os.Rename(src, dst)
}

// agreeOnDates gives every copy of a file — the same path under the year on
// several drives — the same date, so that each drive files it into the same
// mission.
//
// Each drive was dated on its own, and the last fallback is the file's mtime,
// which belongs to the copy rather than the footage: copies did not keep the
// source's mtime, so a cold copy was dated by when it was synced. The hot and
// cold copies of one clip could then land in different seasons, and the drives
// disagreed about which mission held it from then on. The best-sourced date
// wins — ffprobe, then the filename, then the mtime — and among mtimes the
// earliest, which is the one nearest the recording.
func agreeOnDates(sets [][]fileWithDate) {
	rank := map[string]int{"ffprobe": 3, "filename": 2, "mtime": 1}
	best := make(map[string]fileWithDate)
	for _, files := range sets {
		for _, f := range files {
			b, ok := best[f.rel]
			switch {
			case !ok, rank[f.src] > rank[b.src]:
				best[f.rel] = f
			case rank[f.src] == rank[b.src] && f.src == "mtime" && f.date.Before(b.date):
				best[f.rel] = f
			}
		}
	}
	for _, files := range sets {
		for i, f := range files {
			b := best[f.rel]
			files[i].date, files[i].src = b.date, b.src
		}
	}
}
