package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/vbauerster/mpb/v8"
)

func runVerify(cfg Config, missionNum int, year int) {
	yearStr := strconv.Itoa(year)

	slug, err := findMissionSlug(cfg.Drives, yearStr, missionNum)
	if err != nil {
		exit(1, "mission %03d not found: %v", missionNum, err)
	}

	type dirEntry struct {
		vol  string
		dir  string
		base string
	}
	var dirs []dirEntry
	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		dir := filepath.Join(base, d.Root, yearStr, slug)
		if !dirExists(dir) {
			fmt.Printf("%s mission %03d not found on %s\n", yellow("warning:"), missionNum, bold(d.name()))
			continue
		}
		dirs = append(dirs, dirEntry{d.name(), dir, base})
	}
	if len(dirs) == 0 {
		exit(1, "mission %03d not found on any mounted drive", missionNum)
	}

	type entry struct{ hash, rel string }
	type dirJob struct {
		dirEntry
		entries   []entry
		totalSize int64
	}

	var jobs []dirJob
	for _, de := range dirs {
		cPath := filepath.Join(de.dir, "checksums.b3")
		var entries []entry
		var totalSize int64
		f, err := os.Open(cPath)
		if err != nil {
			fmt.Printf("%s cannot open %s: %v\n", yellow("warning:"), cPath, err)
			continue
		}
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			parts := strings.SplitN(scanner.Text(), "  ", 2)
			if len(parts) == 2 {
				entries = append(entries, entry{parts[0], parts[1]})
				if info, err := os.Stat(filepath.Join(de.dir, parts[1])); err == nil {
					totalSize += info.Size()
				}
			}
		}
		f.Close()
		jobs = append(jobs, dirJob{de, entries, totalSize})
	}
	if len(jobs) == 0 {
		exit(1, "no checksums.b3 found for mission %03d", missionNum)
	}

	fmt.Printf("%s mission %s on %d drive(s)\n", dim("verifying"), bold(fmt.Sprintf("%03d", missionNum)), len(jobs))
	volInfos := make(map[string]driveInfo)
	for _, job := range jobs {
		info := probeDrive(job.base)
		volInfos[job.vol] = info
		fmt.Printf("  %s: %s\n", bold(job.vol), info)
	}
	fmt.Println()

	p := mpb.New(mpb.WithWidth(64))
	var failed atomic.Int64
	var trackers []*barTracker
	var jobPools []*pool

	for _, job := range jobs {
		bar := addBar(p, job.vol, job.totalSize)
		trackers = append(trackers, bar)
		wp := newPool(volInfos[job.vol].concurrency)
		jobPools = append(jobPools, wp)
		for _, e := range job.entries {
			e, dir, b := e, job.dir, bar
			wp.run(func() {
				got, err := hashFile(filepath.Join(dir, e.rel), b)
				if err != nil {
					fmt.Printf("\n%s %v\n", red("ERROR:"), err)
					failed.Add(1)
					return
				}
				if got != e.hash {
					fmt.Printf("\n%s [%s]: %s\n", red("FAIL"), filepath.Base(dir), e.rel)
					failed.Add(1)
				}
			})
		}
	}
	for _, wp := range jobPools {
		wp.wait()
	}
	for _, t := range trackers {
		t.flush()
	}
	p.Wait()

	if n := failed.Load(); n > 0 {
		exit(1, "%d file(s) failed", n)
	}
	total := 0
	for _, j := range jobs {
		total += len(j.entries)
	}
	fmt.Printf("\n%s all %d files ok across %d drive(s)\n", green("✓"), total, len(jobs))
}

func runVerifyAll(cfg Config) bool {
	years := allYears(cfg)
	if len(years) == 0 {
		fmt.Println(dim("no missions found"))
		return true
	}
	ok := true
	for _, year := range years {
		fmt.Printf("%s\n\n", bold(strconv.Itoa(year)))
		if !runVerifyYear(cfg, year) {
			ok = false
		}
		fmt.Println()
	}
	return ok
}

func runVerifyYear(cfg Config, year int) bool {
	yearStr := strconv.Itoa(year)

	slugSet := make(map[string]bool)
	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		entries, err := os.ReadDir(filepath.Join(base, d.Root, yearStr))
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() && isNumberedMission(e.Name()) {
				slugSet[e.Name()] = true
			}
		}
	}
	var slugs []string
	for s := range slugSet {
		slugs = append(slugs, s)
	}
	if len(slugs) == 0 {
		fmt.Println(dim("no missions found"))
		return true
	}
	sort.Strings(slugs)

	// probe each mounted drive once so verifySlug doesn't repeat diskutil calls
	volInfos := make(map[string]driveInfo)
	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		if _, seen := volInfos[d.name()]; !seen {
			volInfos[d.name()] = probeDrive(base)
		}
	}

	ok := true
	for _, slug := range slugs {
		if !verifySlug(cfg, slug, yearStr, volInfos) {
			ok = false
		}
	}
	return ok
}

// verifySlug verifies all checksums.b3 entries for a slug across all drives.
// It is the batch-mode counterpart to runVerify: no progress bars, one line of
// output per mission, continues on failure rather than calling exit.
func verifySlug(cfg Config, slug, yearStr string, volInfos map[string]driveInfo) bool {
	type entry struct{ hash, rel string }
	type driveJob struct {
		vol     string
		dir     string
		entries []entry
	}

	var jobs []driveJob
	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		dir := filepath.Join(base, d.Root, yearStr, slug)
		if !dirExists(dir) {
			continue
		}
		f, err := os.Open(filepath.Join(dir, "checksums.b3"))
		if err != nil {
			continue
		}
		var entries []entry
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			parts := strings.SplitN(scanner.Text(), "  ", 2)
			if len(parts) == 2 {
				entries = append(entries, entry{parts[0], parts[1]})
			}
		}
		f.Close()
		if len(entries) > 0 {
			jobs = append(jobs, driveJob{d.name(), dir, entries})
		}
	}

	if len(jobs) == 0 {
		fmt.Printf("  %s %s\n", dim("—"), dim(slug+" (no checksums.b3)"))
		return true
	}

	type failure struct {
		vol string
		rel string
		err error
	}
	var mu sync.Mutex
	var failures []failure

	var drivePools []*pool
	for _, job := range jobs {
		info := volInfos[job.vol]
		if info.concurrency == 0 {
			info.concurrency = 1
		}
		wp := newPool(info.concurrency)
		drivePools = append(drivePools, wp)
		for _, e := range job.entries {
			e, j := e, job
			wp.run(func() {
				got, err := hashFile(filepath.Join(j.dir, e.rel), nil)
				if err != nil || got != e.hash {
					mu.Lock()
					failures = append(failures, failure{j.vol, e.rel, err})
					mu.Unlock()
				}
			})
		}
	}
	for _, wp := range drivePools {
		wp.wait()
	}

	if len(failures) > 0 {
		fmt.Printf("  %s %s\n", red("✗"), bold(slug))
		sort.Slice(failures, func(i, j int) bool {
			if failures[i].vol != failures[j].vol {
				return failures[i].vol < failures[j].vol
			}
			return failures[i].rel < failures[j].rel
		})
		for _, f := range failures {
			if f.err != nil {
				fmt.Printf("      [%s] %s: %v\n", f.vol, f.rel, f.err)
			} else {
				fmt.Printf("      %s [%s] %s\n", red("FAIL"), f.vol, f.rel)
			}
		}
		return false
	}

	total := 0
	for _, j := range jobs {
		total += len(j.entries)
	}
	fmt.Printf("  %s %s\n", green("✓"), dim(fmt.Sprintf("%s (%d files, %d drive(s))", slug, total, len(jobs))))
	return true
}
