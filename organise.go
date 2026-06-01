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
		for _, f := range files {
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
	for _, p := range plans {
		executeOrganisePlan(p)
	}

	seq[year] = nextNum + len(seasons) - 1
	if err := writeSeq(seq); err != nil {
		fmt.Printf("%s writing seq: %v\n", red("ERROR"), err)
	}
}

// scanUnorganised walks yearDir and extracts dates from all files that are not
// already inside a numbered or underscore-prefixed folder.
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
			if strings.HasPrefix(top, "_") || (!regroup && isNumberedMission(top)) {
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
		if junkFiles[d.Name()] {
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
		for i, mf := range m.files {
			base := filepath.Base(mf.f.rel)
			if count[base] > 1 {
				// prefix with the top-level source subfolder if one exists
				if parts := strings.SplitN(mf.f.rel, string(os.PathSeparator), 2); len(parts) > 1 {
					base = parts[0] + "_" + base
				}
			}
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

func executeOrganisePlan(p organisePlan) {
	// track dirs that have files moved in or out — their checksums.b3 becomes stale
	affected := make(map[string]bool)

	markAffected := func(rel, destDir string) {
		// source: the top-level mission dir the file came from (if any)
		if parts := strings.SplitN(rel, string(os.PathSeparator), 2); len(parts) > 1 {
			affected[filepath.Join(p.yearDir, parts[0])] = true
		}
		// destination mission dir
		affected[destDir] = true
	}

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
			if err := os.Rename(src, dst); err != nil {
				fmt.Printf("%s move %s: %v\n", red("ERROR"), mf.f.rel, err)
				continue
			}
			markAffected(mf.f.rel, destDir)
		}
	}
	for _, f := range p.unsorted {
		src := filepath.Join(p.yearDir, f.rel)
		dst := filepath.Join(p.yearDir, "_unsorted", filepath.Base(f.rel))
		if src == dst {
			continue
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0777); err != nil {
			fmt.Printf("%s mkdir: %v\n", red("ERROR"), err)
			continue
		}
		if err := os.Rename(src, dst); err != nil {
			fmt.Printf("%s move %s: %v\n", red("ERROR"), f.rel, err)
			continue
		}
		markAffected(f.rel, filepath.Join(p.yearDir, "_unsorted"))
	}

	// remove stale checksums.b3 from any dir that had files moved in or out
	for dir := range affected {
		cPath := filepath.Join(dir, "checksums.b3")
		if err := os.Remove(cPath); err == nil {
			fmt.Printf("removed stale checksums: %s\n", cPath)
		}
	}

	removeEmptyDirs(p.yearDir)
	fmt.Printf("%s %s: organised %d mission(s)\n", green("✓"), bold(p.driveName), len(p.missions))
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

func isNumberedMission(name string) bool {
	parts := strings.SplitN(name, "_", 2)
	if len(parts) < 2 {
		return false
	}
	n, err := strconv.Atoi(parts[0])
	return err == nil && n > 0
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
