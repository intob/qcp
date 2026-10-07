package main

import (
	"fmt"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vbauerster/mpb/v8"
)

func runChecksumAll(cfg Config) bool {
	years := allYears(cfg)
	if len(years) == 0 {
		fmt.Println(dim("no missions found"))
		return true
	}
	ok := true
	for _, year := range years {
		fmt.Printf("%s\n\n", bold(strconv.Itoa(year)))
		if !runChecksumYear(cfg, year) {
			ok = false
		}
		fmt.Println()
	}
	return ok
}

func runChecksumYear(cfg Config, year int) bool {
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
		fmt.Printf(red("no drives with a %s directory mounted\n"), yearStr)
		return false
	}

	allSlugs := make(map[string]bool)
	for _, dy := range drives {
		for _, slug := range missionDirs(dy.yearDir) {
			allSlugs[slug] = true
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
		slug     string
		drives   []missionDrive
		files    []fileEntry
		size     int64
		recorded map[string]map[string]string
	}
	var jobs []missionJob
	var ghostedMissions int
	for _, slug := range slugs {
		var mDrives []missionDrive
		for _, dy := range drives {
			dir := filepath.Join(dy.yearDir, slug)
			if !dirExists(dir) {
				continue
			}
			if isFullyChecksummed(dir) {
				continue // already fully checksummed
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
		// build file union from unchecksummed drives only — workers hash paths
		// under md.dir, so files that only exist on already-checksummed drives
		// cannot be hashed and must not be included in the job
		fileSet := make(map[string]fileEntry)
		var ghosted bool
		for _, md := range mDrives {
			fs, _, ghosts, err := missionFiles(md.dir)
			if err != nil {
				fmt.Printf("%s error scanning %s on %s: %v\n", yellow("warning:"), slug, md.vol, err)
				continue
			}
			if ghosts > 0 {
				ghosted = true
			}
			for _, f := range fs {
				if _, exists := fileSet[f.rel]; !exists {
					fileSet[f.rel] = f
				}
			}
		}
		// warn about files present on checksummed drives but absent from unchecksummed ones
		for _, dy := range drives {
			dir := filepath.Join(dy.yearDir, slug)
			if !dirExists(dir) || !isFullyChecksummed(dir) {
				continue
			}
			fs, _, _, _ := missionFiles(dir)
			for _, f := range fs {
				if _, exists := fileSet[f.rel]; !exists {
					fmt.Printf("%s %s: %s only on %s — run -sync to copy to other drives\n",
						yellow("warning:"), slug, f.rel, dy.d.name())
				}
			}
		}
		// Rewriting the manifest would drop the entries for the missing files,
		// and with them the only sign that anything went missing.
		if ghosted {
			fmt.Printf("%s %s: files in checksums.b3 are missing from disk — skipped; restore them first\n", red("ERROR"), slug)
			ghostedMissions++
			continue
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
		recorded, err := recordedHashes(cfg, yearStr, slug)
		if err != nil {
			fmt.Printf("%s %s: %v — skipped\n", red("ERROR"), slug, err)
			ghostedMissions++
			continue
		}
		jobs = append(jobs, missionJob{slug, mDrives, files, size, recorded})
	}

	already := len(slugs) - len(jobs) - ghostedMissions
	if len(jobs) == 0 {
		if ghostedMissions > 0 {
			return false
		}
		fmt.Printf("%s\n", dim(fmt.Sprintf("all %d mission(s) already checksummed", already)))
		return true
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
		began := time.Now()

		var mu sync.Mutex
		var failed atomic.Int64
		var drivePools []*pool
		var submitters []func()

		for k := range j.drives {
			md := &j.drives[k]
			dp := newPool(volInfo[md.vol].concurrency)
			drivePools = append(drivePools, dp)
			submitters = append(submitters, func() {
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
			})
		}
		submitAll(submitters)
		for _, dp := range drivePools {
			dp.wait()
		}

		if failed.Load() > 0 {
			totalFailed.Add(failed.Load())
			fmt.Printf("\n%s %s: %d error(s)\n", dim("skipping"), j.slug, failed.Load())
			continue
		}

		// every hash already recorded for the mission, on any mounted drive, stands
		var conflicts int
		for _, md := range j.drives {
			for _, c := range recordedConflicts(md.vol, md.hashes, j.recorded) {
				fmt.Printf("\n%s %s\n", red("CONFLICT"), c)
				conflicts++
			}
		}

		// cross-check
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
				if f.rel == "checksums.b3" {
					continue // a manifest never lists itself
				}
				lines = append(lines, fmt.Sprintf("%s  %s", md.hashes[f.rel], f.rel))
			}
			cPath := filepath.Join(md.dir, "checksums.b3")
			if err := writeChecksums(cPath, lines); err != nil {
				fmt.Printf("\n%s writing %s: %v\n", red("ERROR"), cPath, err)
			} else {
				stampChecksummed(md.vol, md.dir, len(lines), began)
			}
		}
	}

	for _, t := range bars {
		t.stop()
	}
	p.Wait()

	if n := totalFailed.Load(); n > 0 || ghostedMissions > 0 {
		fmt.Printf("\n%s %d error(s), %d mission(s) with missing files — some missions may be incomplete\n",
			red("ERROR:"), n, ghostedMissions)
		return false
	}
	fmt.Printf("\n%s %d mission(s) checksummed\n", green("✓"), len(jobs))
	return true
}

// runChecksum hashes one mission on every mounted drive that has it, cross-checks
// the drives against each other and writes checksums.b3. It reports problems and
// returns false rather than exiting, so callers can do several missions in a row.
func runChecksum(cfg Config, missionNum int, year int) bool {
	yearStr := strconv.Itoa(year)
	slug, err := findMissionSlug(cfg.Drives, yearStr, missionNum)
	if err != nil {
		fmt.Printf("%s mission %03d not found: %v\n", red("ERROR"), missionNum, err)
		return false
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
		if isFullyChecksummed(dir) {
			fmt.Printf("%s %s already fully checksummed, %s\n", yellow("warning:"), bold(d.name()), dim("skipping"))
			continue
		}
		drives = append(drives, driveHashes{vol: d.name(), dir: dir, base: base, hashes: make(map[string]string)})
	}
	if len(drives) == 0 {
		// every mounted copy is already checksummed (or absent) — a no-op, not a
		// failure; the loop above has already said which drives were skipped
		fmt.Printf("%s\n", dim(fmt.Sprintf("mission %03d: nothing to checksum", missionNum)))
		return true
	}

	// union file lists from all drives so files absent from drives[0] are not silently omitted
	fileSet := make(map[string]fileEntry)
	for _, d := range drives {
		fs, _, ghosts, err := missionFiles(d.dir)
		if err != nil {
			fmt.Printf("%s error scanning %s: %v\n", yellow("warning:"), d.vol, err)
			continue
		}
		// Rewriting the manifest would drop the entries for the missing files,
		// and with them the only sign that anything went missing.
		if ghosts > 0 {
			fmt.Printf("%s %s: %d file(s) in checksums.b3 are missing from disk — checksums.b3 not written; restore them first\n",
				red("ERROR"), d.vol, ghosts)
			return false
		}
		for _, f := range fs {
			if _, exists := fileSet[f.rel]; !exists {
				fileSet[f.rel] = f
			}
		}
	}
	if len(fileSet) == 0 {
		fmt.Printf("%s no files found for mission %03d\n", red("ERROR"), missionNum)
		return false
	}
	var files []fileEntry
	var totalSize int64
	for _, f := range fileSet {
		files = append(files, f)
		totalSize += f.size
	}
	sort.Slice(files, func(i, j int) bool { return files[i].rel < files[j].rel })

	fmt.Printf("checksumming %s on %s drive(s)\n", bold(slug), bold(strconv.Itoa(len(drives))))
	driveInfos := make(map[string]driveInfo)
	for _, d := range drives {
		info := probeDrive(d.base)
		driveInfos[d.vol] = info
		fmt.Printf("  %s: %s\n", bold(d.vol), info)
	}
	fmt.Println()

	// hash all files on all drives in parallel, per-drive concurrency
	began := time.Now()
	p := mpb.New(mpb.WithWidth(64))
	var mu sync.Mutex
	var failed atomic.Int64
	var trackers []*barTracker
	var drivePools []*pool

	var submitters []func()
	for i := range drives {
		d := &drives[i]
		bar := addBar(p, d.vol, totalSize)
		trackers = append(trackers, bar)
		dp := newPool(driveInfos[d.vol].concurrency)
		drivePools = append(drivePools, dp)
		submitters = append(submitters, func() {
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
		})
	}
	submitAll(submitters)
	for _, dp := range drivePools {
		dp.wait()
	}
	for _, t := range trackers {
		t.stop()
	}
	p.Wait()

	if failed.Load() > 0 {
		fmt.Printf("\n%s %d file(s) could not be hashed\n", red("ERROR"), failed.Load())
		return false
	}

	// every hash already recorded for the mission, on any mounted drive, stands
	var conflicts int
	recorded, err := recordedHashes(cfg, yearStr, slug)
	if err != nil {
		fmt.Printf("%s %v — checksums.b3 not written\n", red("ERROR"), err)
		return false
	}
	for _, d := range drives {
		for _, c := range recordedConflicts(d.vol, d.hashes, recorded) {
			fmt.Printf("%s %s\n", red("CONFLICT:"), c)
			conflicts++
		}
	}

	// cross-check: every drive must agree on every file
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
		fmt.Printf("%s %d conflict(s) found — checksums.b3 not written; run -verify %03d\n", red("ERROR"), conflicts, missionNum)
		return false
	}

	// all drives agree — write checksums.b3 to each
	for _, d := range drives {
		var lines []string
		for _, f := range files {
			if f.rel == "checksums.b3" {
				continue // a manifest never lists itself
			}
			lines = append(lines, fmt.Sprintf("%s  %s", d.hashes[f.rel], f.rel))
		}
		cPath := filepath.Join(d.dir, "checksums.b3")
		if err := writeChecksums(cPath, lines); err != nil {
			fmt.Printf("%s writing %s: %v\n", red("ERROR"), cPath, err)
		} else {
			fmt.Printf("%s wrote %s (%d files)\n", green("✓"), cPath, len(lines))
			stampChecksummed(d.vol, d.dir, len(lines), began)
		}
	}
	return true
}

// recordedConflicts compares hashes just computed on one drive with every hash
// already recorded for the mission — that drive's own checksums.b3 and those of
// every other mounted copy — and describes each disagreement.
//
// -checksum used to replace checksums.b3 with whatever was on disk, so a file
// that had rotted since ingest had its good hash overwritten by the bad one and
// the run reported success. A mission only has to gain one unrecorded file —
// an append — to be rehashed, and a copy that was already fully checksummed was
// skipped and never used as a reference. A recorded hash is the evidence that
// catches corruption, so a disagreement with one is a conflict to investigate
// with -verify, never something to write over.
func recordedConflicts(vol string, fresh map[string]string, recorded map[string]map[string]string) []string {
	var out []string
	rels := make([]string, 0, len(fresh))
	for rel := range fresh {
		rels = append(rels, rel)
	}
	sort.Strings(rels)
	vols := make([]string, 0, len(recorded))
	for v := range recorded {
		vols = append(vols, v)
	}
	sort.Strings(vols)
	for _, rel := range rels {
		for _, v := range vols {
			want := recorded[v][rel]
			if want == "" || want == fresh[rel] {
				continue
			}
			if v == vol {
				out = append(out, fmt.Sprintf("%s on %s no longer matches its checksums.b3", rel, vol))
			} else {
				out = append(out, fmt.Sprintf("%s on %s does not match the hash recorded on %s", rel, vol, v))
			}
		}
	}
	return out
}

// recordedHashes reads the checksums.b3 of every mounted copy of a mission,
// keyed by drive name. A manifest that cannot be read is an error: what it
// records is exactly what must not be written over.
func recordedHashes(cfg Config, yearStr, slug string) (map[string]map[string]string, error) {
	out := make(map[string]map[string]string)
	for _, d := range cfg.Drives {
		dir := filepath.Join(d.basePath(), d.Root, yearStr, slug)
		if !dirExists(dir) {
			continue
		}
		m, err := readChecksums(filepath.Join(dir, "checksums.b3"))
		if err != nil {
			return nil, fmt.Errorf("%s: %w", d.name(), err)
		}
		if len(m) > 0 {
			out[d.name()] = m
		}
	}
	return out, nil
}
