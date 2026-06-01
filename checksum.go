package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/inneslabs/fnpool"
	"github.com/vbauerster/mpb/v8"
)

func runChecksumYear(cfg Config, year int) {
	yearStr := strconv.Itoa(year)

	// collect all missions in the year across all mounted drives
	type driveYear struct {
		d       DriveConfig
		yearDir string
	}
	var drives []driveYear
	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
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

	allSlugs := make(map[string]bool)
	for _, dy := range drives {
		entries, err := os.ReadDir(dy.yearDir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() && isNumberedMission(e.Name()) {
				allSlugs[e.Name()] = true
			}
		}
	}
	var slugs []string
	for s := range allSlugs {
		slugs = append(slugs, s)
	}
	sort.Strings(slugs)

	// build per-mission jobs: only drives that need checksumming
	type missionDrive struct {
		vol    string
		dir    string
		base   string
		hashes map[string]string
	}
	type missionJob struct {
		slug   string
		drives []missionDrive
		files  []fileEntry
		size   int64
	}
	var jobs []missionJob
	for _, slug := range slugs {
		var mDrives []missionDrive
		for _, dy := range drives {
			dir := filepath.Join(dy.yearDir, slug)
			if !dirExists(dir) {
				continue
			}
			if _, err := os.Stat(filepath.Join(dir, "checksums.b3")); err == nil {
				continue // already checksummed
			}
			mDrives = append(mDrives, missionDrive{
				vol:    dy.d.name(),
				dir:    dir,
				base:   dy.d.basePath(),
				hashes: make(map[string]string),
			})
		}
		if len(mDrives) == 0 {
			continue
		}
		files, size, err := missionFiles(mDrives[0].dir)
		if err != nil {
			fmt.Printf("warning: error scanning %s: %v\n", slug, err)
			continue
		}
		jobs = append(jobs, missionJob{slug, mDrives, files, size})
	}

	already := len(slugs) - len(jobs)
	if len(jobs) == 0 {
		fmt.Printf("all %d mission(s) already checksummed\n", already)
		return
	}

	// total bytes per drive across all missions
	sizeByVol := make(map[string]int64)
	for _, j := range jobs {
		for _, md := range j.drives {
			sizeByVol[md.vol] += j.size
		}
	}

	fmt.Printf("checksumming %d mission(s) in %d", len(jobs), year)
	if already > 0 {
		fmt.Printf(" (%d already done)", already)
	}
	fmt.Println()

	// probe each drive once
	volInfo := make(map[string]driveInfo)
	var volOrder []string
	for _, j := range jobs {
		for _, md := range j.drives {
			if _, ok := volInfo[md.vol]; !ok {
				volInfo[md.vol] = probeDrive(md.base)
				volOrder = append(volOrder, md.vol)
				fmt.Printf("  %s: %s\n", md.vol, volInfo[md.vol])
			}
		}
	}
	fmt.Println()

	// set up progress bars — one per drive, labels update dynamically
	var labelVal atomic.Value
	labelVal.Store("[0/0]")
	label := func() string { v, _ := labelVal.Load().(string); return v }

	p := mpb.New(mpb.WithWidth(56))
	bars := make(map[string]*barTracker, len(volOrder))
	for _, vol := range volOrder {
		bars[vol] = addBarDynamic(p, vol, sizeByVol[vol], label)
	}

	var totalFailed atomic.Int64
	for i, j := range jobs {
		labelVal.Store(fmt.Sprintf("[%d/%d]", i+1, len(jobs)))

		var wg sync.WaitGroup
		var mu sync.Mutex
		var failed atomic.Int64

		for k := range j.drives {
			md := &j.drives[k]
			pool := fnpool.NewPool(volInfo[md.vol].concurrency)
			for _, f := range j.files {
				wg.Add(1)
				f := f
				pool.Dispatch(func() {
					defer wg.Done()
					hash, err := hashFile(filepath.Join(md.dir, f.rel), bars[md.vol])
					if err != nil {
						fmt.Printf("\nERROR [%s] %s: %v\n", md.vol, f.rel, err)
						failed.Add(1)
						return
					}
					mu.Lock()
					md.hashes[f.rel] = hash
					mu.Unlock()
				})
			}
		}
		wg.Wait()

		if failed.Load() > 0 {
			totalFailed.Add(failed.Load())
			fmt.Printf("\nskipping %s: %d error(s)\n", j.slug, failed.Load())
			continue
		}

		// cross-check
		var conflicts int
		for _, f := range j.files {
			ref := j.drives[0].hashes[f.rel]
			for _, md := range j.drives[1:] {
				if md.hashes[f.rel] != ref {
					fmt.Printf("\nCONFLICT [%s vs %s]: %s\n", j.drives[0].vol, md.vol, f.rel)
					conflicts++
				}
			}
		}
		if conflicts > 0 {
			totalFailed.Add(int64(conflicts))
			fmt.Printf("\nskipping %s: %d conflict(s)\n", j.slug, conflicts)
			continue
		}

		// write checksums.b3
		for _, md := range j.drives {
			var lines []string
			for _, f := range j.files {
				lines = append(lines, fmt.Sprintf("%s  %s", md.hashes[f.rel], f.rel))
			}
			sort.Strings(lines)
			cPath := filepath.Join(md.dir, "checksums.b3")
			if err := os.WriteFile(cPath, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
				fmt.Printf("\nERROR writing %s: %v\n", cPath, err)
			}
		}
	}

	for _, t := range bars {
		t.flush()
	}
	p.Wait()

	if n := totalFailed.Load(); n > 0 {
		exit(1, "%d error(s) — some missions may be incomplete", n)
	}
	fmt.Printf("\n%d mission(s) checksummed\n", len(jobs))
}

func runChecksum(cfg Config, missionNum int, year int) {
	yearStr := strconv.Itoa(year)
	slug, err := findMissionSlug(cfg.Drives, yearStr, missionNum)
	if err != nil {
		exit(1, "mission %03d not found: %v", missionNum, err)
	}

	type driveHashes struct {
		vol    string
		dir    string
		base   string
		hashes map[string]string // rel → hash
	}

	// find mission on all mounted drives
	var drives []driveHashes
	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		dir := filepath.Join(base, d.Root, yearStr, slug)
		if !dirExists(dir) {
			fmt.Printf("warning: mission not found on %s, skipping\n", d.name())
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, "checksums.b3")); err == nil {
			fmt.Printf("warning: %s already has checksums.b3, skipping\n", d.name())
			continue
		}
		drives = append(drives, driveHashes{vol: d.name(), dir: dir, base: base, hashes: make(map[string]string)})
	}
	if len(drives) == 0 {
		exit(1, "no drives to checksum (all missing or already have checksums.b3)")
	}

	// collect file list from first drive (all should be identical post-sync)
	files, totalSize, err := missionFiles(drives[0].dir)
	if err != nil {
		exit(1, "error scanning %s: %v", drives[0].vol, err)
	}

	fmt.Printf("checksumming mission %03d (%s) on %d drive(s)\n", missionNum, slug, len(drives))
	driveInfos := make(map[string]driveInfo)
	for _, d := range drives {
		info := probeDrive(d.base)
		driveInfos[d.vol] = info
		fmt.Printf("  %s: %s\n", d.vol, info)
	}
	fmt.Println()

	// hash all files on all drives in parallel, per-drive concurrency
	p := mpb.New(mpb.WithWidth(64))
	var mu sync.Mutex
	var failed atomic.Int64
	var wg sync.WaitGroup
	var trackers []*barTracker

	for i := range drives {
		d := &drives[i]
		bar := addBar(p, d.vol, totalSize)
		trackers = append(trackers, bar)
		pool := fnpool.NewPool(driveInfos[d.vol].concurrency)
		for _, f := range files {
			wg.Add(1)
			f := f
			pool.Dispatch(func() {
				defer wg.Done()
				hash, err := hashFile(filepath.Join(d.dir, f.rel), bar)
				if err != nil {
					fmt.Printf("\nERROR [%s]: %v\n", d.vol, err)
					failed.Add(1)
					return
				}
				mu.Lock()
				d.hashes[f.rel] = hash
				mu.Unlock()
			})
		}
	}
	wg.Wait()
	for _, t := range trackers {
		t.flush()
	}
	p.Wait()

	if failed.Load() > 0 {
		exit(1, "%d file(s) could not be hashed", failed.Load())
	}

	// cross-check: every drive must agree on every file
	var conflicts int
	for _, f := range files {
		ref := drives[0].hashes[f.rel]
		for _, d := range drives[1:] {
			if d.hashes[f.rel] != ref {
				fmt.Printf("CONFLICT: %s — %s=%s  %s=%s\n",
					f.rel, drives[0].vol, ref[:8], d.vol, d.hashes[f.rel][:8])
				conflicts++
			}
		}
	}
	if conflicts > 0 {
		exit(1, "%d conflict(s) found — checksums.b3 not written", conflicts)
	}

	// all drives agree — write checksums.b3 to each
	for _, d := range drives {
		var lines []string
		for _, f := range files {
			lines = append(lines, fmt.Sprintf("%s  %s", d.hashes[f.rel], f.rel))
		}
		sort.Strings(lines)
		cPath := filepath.Join(d.dir, "checksums.b3")
		if err := os.WriteFile(cPath, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
			fmt.Printf("ERROR writing %s: %v\n", cPath, err)
		} else {
			fmt.Printf("wrote %s (%d files)\n", cPath, len(lines))
		}
	}
}
