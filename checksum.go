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
		fileSet := make(map[string]fileEntry)
		for _, md := range mDrives {
			fs, _, err := missionFiles(md.dir)
			if err != nil {
				fmt.Printf("%s error scanning %s on %s: %v\n", yellow("warning:"), slug, md.vol, err)
				continue
			}
			for _, f := range fs {
				if _, exists := fileSet[f.rel]; !exists {
					fileSet[f.rel] = f
				}
			}
		}
		if len(fileSet) == 0 {
			fmt.Printf("%s no files found for %s\n", yellow("warning:"), slug)
			continue
		}
		var files []fileEntry
		var size int64
		for _, f := range fileSet {
			files = append(files, f)
			size += f.size
		}
		sort.Slice(files, func(a, b int) bool { return files[a].rel < files[b].rel })
		jobs = append(jobs, missionJob{slug, mDrives, files, size})
	}

	already := len(slugs) - len(jobs)
	if len(jobs) == 0 {
		fmt.Printf("%s\n", dim(fmt.Sprintf("all %d mission(s) already checksummed", already)))
		return
	}

	// total bytes per drive across all missions
	sizeByVol := make(map[string]int64)
	for _, j := range jobs {
		for _, md := range j.drives {
			sizeByVol[md.vol] += j.size
		}
	}

	fmt.Printf("checksumming %s mission(s) in %d", bold(strconv.Itoa(len(jobs))), year)
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
				fmt.Printf("  %s: %s\n", bold(md.vol), volInfo[md.vol])
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

		var mu sync.Mutex
		var failed atomic.Int64
		var drivePools []*pool

		for k := range j.drives {
			md := &j.drives[k]
			dp := newPool(volInfo[md.vol].concurrency)
			drivePools = append(drivePools, dp)
			for _, f := range j.files {
				f := f
				dp.run(func() {
					hash, err := hashFile(filepath.Join(md.dir, f.rel), bars[md.vol])
					if err != nil {
						fmt.Printf("\n%s %s: %v\n", red(fmt.Sprintf("ERROR [%s]", md.vol)), f.rel, err)
						failed.Add(1)
						return
					}
					mu.Lock()
					md.hashes[f.rel] = hash
					mu.Unlock()
				})
			}
		}
		for _, dp := range drivePools {
			dp.wait()
		}

		if failed.Load() > 0 {
			totalFailed.Add(failed.Load())
			fmt.Printf("\n%s %s: %d error(s)\n", dim("skipping"), j.slug, failed.Load())
			continue
		}

		// cross-check
		var conflicts int
		for _, f := range j.files {
			ref := j.drives[0].hashes[f.rel]
			for _, md := range j.drives[1:] {
				if md.hashes[f.rel] != ref {
					fmt.Printf("\n%s [%s vs %s]: %s\n", red("CONFLICT"), j.drives[0].vol, md.vol, f.rel)
					conflicts++
				}
			}
		}
		if conflicts > 0 {
			totalFailed.Add(int64(conflicts))
			fmt.Printf("\n%s %s: %d conflict(s)\n", dim("skipping"), j.slug, conflicts)
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
				fmt.Printf("\n%s writing %s: %v\n", red("ERROR"), cPath, err)
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
	fmt.Printf("\n%s %d mission(s) checksummed\n", green("✓"), len(jobs))
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
			fmt.Printf("%s mission not found on %s, %s\n", yellow("warning:"), bold(d.name()), dim("skipping"))
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, "checksums.b3")); err == nil {
			fmt.Printf("%s %s already has checksums.b3, %s\n", yellow("warning:"), bold(d.name()), dim("skipping"))
			continue
		}
		drives = append(drives, driveHashes{vol: d.name(), dir: dir, base: base, hashes: make(map[string]string)})
	}
	if len(drives) == 0 {
		exit(1, "no drives to checksum (all missing or already have checksums.b3)")
	}

	// union file lists from all drives so files absent from drives[0] are not silently omitted
	fileSet := make(map[string]fileEntry)
	for _, d := range drives {
		fs, _, err := missionFiles(d.dir)
		if err != nil {
			fmt.Printf("%s error scanning %s: %v\n", yellow("warning:"), d.vol, err)
			continue
		}
		for _, f := range fs {
			if _, exists := fileSet[f.rel]; !exists {
				fileSet[f.rel] = f
			}
		}
	}
	if len(fileSet) == 0 {
		exit(1, "no files found for mission %03d", missionNum)
	}
	var files []fileEntry
	var totalSize int64
	for _, f := range fileSet {
		files = append(files, f)
		totalSize += f.size
	}
	sort.Slice(files, func(i, j int) bool { return files[i].rel < files[j].rel })

	fmt.Printf("checksumming mission %s (%s) on %s drive(s)\n", bold(fmt.Sprintf("%03d", missionNum)), slug, bold(strconv.Itoa(len(drives))))
	driveInfos := make(map[string]driveInfo)
	for _, d := range drives {
		info := probeDrive(d.base)
		driveInfos[d.vol] = info
		fmt.Printf("  %s: %s\n", bold(d.vol), info)
	}
	fmt.Println()

	// hash all files on all drives in parallel, per-drive concurrency
	p := mpb.New(mpb.WithWidth(64))
	var mu sync.Mutex
	var failed atomic.Int64
	var trackers []*barTracker
	var drivePools []*pool

	for i := range drives {
		d := &drives[i]
		bar := addBar(p, d.vol, totalSize)
		trackers = append(trackers, bar)
		dp := newPool(driveInfos[d.vol].concurrency)
		drivePools = append(drivePools, dp)
		for _, f := range files {
			f := f
			dp.run(func() {
				hash, err := hashFile(filepath.Join(d.dir, f.rel), bar)
				if err != nil {
					fmt.Printf("\n%s %v\n", red(fmt.Sprintf("ERROR [%s]:", d.vol)), err)
					failed.Add(1)
					return
				}
				mu.Lock()
				d.hashes[f.rel] = hash
				mu.Unlock()
			})
		}
	}
	for _, dp := range drivePools {
		dp.wait()
	}
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
				fmt.Printf("%s %s — %s=%s  %s=%s\n",
					red("CONFLICT:"), f.rel, drives[0].vol, ref[:8], d.vol, d.hashes[f.rel][:8])
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
			fmt.Printf("%s writing %s: %v\n", red("ERROR"), cPath, err)
		} else {
			fmt.Printf("%s wrote %s (%d files)\n", green("✓"), cPath, len(lines))
		}
	}
}
