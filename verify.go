package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vbauerster/mpb/v8"
)

// runVerify verifies one mission against checksums.b3 on every mounted drive
// that has it, with a progress bar per drive. It reports problems and returns
// false rather than exiting, so callers can verify several missions in a row.
func runVerify(cfg Config, missionNum int, year int) bool {
	yearStr := strconv.Itoa(year)

	slug, err := findMissionSlug(cfg.Drives, yearStr, missionNum)
	if err != nil {
		fmt.Printf("%s mission %03d not found: %v\n", red("ERROR"), missionNum, err)
		return false
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
		fmt.Printf("%s mission %03d not found on any mounted drive\n", red("ERROR"), missionNum)
		return false
	}

	type dirJob struct {
		dirEntry
		verifyCopy
		totalSize int64
	}

	var jobs []dirJob
	var unverifiable int
	for _, de := range dirs {
		vc := planVerifyCopy(de.dir, missionNum)
		if vc.problem != "" {
			fmt.Printf("%s %s: %s\n", red("ERROR"), bold(de.vol), vc.problem)
			unverifiable++
			continue
		}
		var totalSize int64
		for _, e := range vc.entries {
			if info, err := os.Stat(filepath.Join(de.dir, e.rel)); err == nil {
				totalSize += info.Size()
			}
		}
		jobs = append(jobs, dirJob{de, vc, totalSize})
	}
	if len(jobs) == 0 {
		fmt.Printf("%s no copy of mission %03d could be verified\n", red("ERROR"), missionNum)
		return false
	}

	fmt.Printf("%s mission %s on %d drive(s)\n", dim("verifying"), bold(fmt.Sprintf("%03d", missionNum)), len(jobs))
	volInfos := make(map[string]driveInfo)
	for _, job := range jobs {
		info := probeDrive(job.base)
		volInfos[job.vol] = info
		fmt.Printf("  %s: %s\n", bold(job.vol), info)
	}
	fmt.Println()

	began := time.Now()
	p := mpb.New(mpb.WithWidth(64))
	var failed atomic.Int64
	failedOn := make(map[string]*atomic.Int64, len(jobs)) // per drive, to stamp the copies that passed
	for _, job := range jobs {
		failedOn[job.vol] = new(atomic.Int64)
	}
	var trackers []*barTracker
	var jobPools []*pool

	var submitters []func()
	for _, job := range jobs {
		bar := addBar(p, job.vol, job.totalSize)
		trackers = append(trackers, bar)
		wp := newPool(volInfos[job.vol].concurrency)
		jobPools = append(jobPools, wp)
		submitters = append(submitters, func() {
			for _, e := range job.entries {
				e, dir, vol, b := e, job.dir, job.vol, bar
				wp.run(func() {
					got, err := hashFile(filepath.Join(dir, e.rel), b)
					if err != nil {
						fmt.Printf("\n%s [%s] %v\n", red("ERROR:"), vol, err)
						failed.Add(1)
						failedOn[vol].Add(1)
						return
					}
					if got != e.hash {
						fmt.Printf("\n%s [%s] %s\n", red("FAIL"), vol, e.rel)
						failed.Add(1)
						failedOn[vol].Add(1)
					}
				})
			}
		})
	}
	submitAll(submitters)
	for _, wp := range jobPools {
		wp.wait()
	}
	for _, t := range trackers {
		t.stop()
	}
	p.Wait()

	var unrecorded int
	for _, j := range jobs {
		if n := len(j.unrecorded); n > 0 {
			fmt.Printf("\n%s %d file(s) on %s are not in checksums.b3 and were not verified, first: %s\n",
				yellow("!"), n, bold(j.vol), j.unrecorded[0])
			unrecorded += n
		}
	}
	if unrecorded > 0 {
		fmt.Printf("%s\n", dim(fmt.Sprintf("  run -checksum %03d to record them", missionNum)))
	}

	// Each copy that passed in full is stamped, even when another copy failed:
	// the stamp is about that copy alone.
	for _, j := range jobs {
		if failedOn[j.vol].Load() == 0 && len(j.unrecorded) == 0 {
			recordVerified(j.vol, j.dir, j.verifyCopy, began)
		}
	}

	if n := failed.Load(); n > 0 {
		fmt.Printf("\n%s %d file(s) failed\n", red("ERROR"), n)
		return false
	}
	if unverifiable > 0 || unrecorded > 0 {
		fmt.Printf("\n%s recorded files ok, but not every copy or file could be verified\n", red("✗"))
		return false
	}
	total := 0
	for _, j := range jobs {
		total += len(j.entries)
	}
	fmt.Printf("\n%s all %d files ok across %d drive(s)\n", green("✓"), total, len(jobs))
	return true
}

// verifyEntry is one file a manifest records.
type verifyEntry struct{ hash, rel string }

// verifyCopy is what one copy of a mission is verified against: the files its
// checksums.b3 records, the files on disk it does not, and — when the copy
// cannot be verified at all — why.
type verifyCopy struct {
	entries    []verifyEntry
	unrecorded []string
	problem    string
	digest     string // of checksums.b3 as read here, for the verified stamp
}

// planVerifyCopy reads one copy's manifest and lists what it leaves out.
//
// -verify used to skip a copy with no checksums.b3 with a warning, pass a
// mission whose files were not all recorded with "all N files ok", and in
// year mode pass a mission it could not verify at all. Each of those is a copy
// or file nothing has vouched for, so each is now a failure, named.
func planVerifyCopy(dir string, num int) verifyCopy {
	var vc verifyCopy
	manifest, err := readChecksums(filepath.Join(dir, "checksums.b3"))
	if err != nil {
		vc.problem = fmt.Sprintf("checksums.b3 could not be read: %v", err)
		return vc
	}
	delete(manifest, "checksums.b3") // a manifest cannot describe itself; see mergeChecksums
	if len(manifest) == 0 {
		vc.problem = fmt.Sprintf("no checksums.b3 — run -checksum %03d", num)
		return vc
	}
	// Taken before any file is read, so a manifest rewritten while this copy
	// is being verified leaves a stamp that no longer matches, never one that
	// vouches for files nobody read.
	vc.digest, _ = manifestDigest(dir)
	for rel, hash := range manifest {
		vc.entries = append(vc.entries, verifyEntry{hash, rel})
	}
	sort.Slice(vc.entries, func(i, j int) bool { return vc.entries[i].rel < vc.entries[j].rel })
	files, err := contentFiles(dir)
	if err != nil {
		vc.problem = fmt.Sprintf("could not be scanned: %v", err)
		return vc
	}
	for _, f := range files {
		if _, ok := manifest[f.rel]; !ok {
			vc.unrecorded = append(vc.unrecorded, f.rel)
		}
	}
	sort.Strings(vc.unrecorded)
	return vc
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
		for _, slug := range missionDirs(filepath.Join(base, d.Root, yearStr)) {
			slugSet[slug] = true
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
	return verifySlugOn(cfg.Drives, slug, yearStr, volInfos, slug)
}

// verifySlugOn is verifySlug limited to the given drives, reported as label.
func verifySlugOn(drives []DriveConfig, slug, yearStr string, volInfos map[string]driveInfo, label string) bool {
	type driveJob struct {
		vol string
		dir string
		verifyCopy
	}

	num, _ := parseMissionNum(slug)
	var jobs []driveJob
	var problems []string
	for _, d := range drives {
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		dir := filepath.Join(base, d.Root, yearStr, slug)
		if !dirExists(dir) {
			continue
		}
		vc := planVerifyCopy(dir, num)
		if vc.problem != "" {
			problems = append(problems, fmt.Sprintf("[%s] %s", d.name(), vc.problem))
			continue
		}
		for _, rel := range vc.unrecorded {
			problems = append(problems, fmt.Sprintf("[%s] %s not in checksums.b3", d.name(), rel))
		}
		jobs = append(jobs, driveJob{d.name(), dir, vc})
	}

	type failure struct {
		vol string
		rel string
		err error
	}
	var mu sync.Mutex
	var failures []failure

	began := time.Now()
	var drivePools []*pool
	var submitters []func()
	for _, job := range jobs {
		info := volInfos[job.vol]
		if info.concurrency == 0 {
			info.concurrency = 1
		}
		wp := newPool(info.concurrency)
		drivePools = append(drivePools, wp)
		submitters = append(submitters, func() {
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
		})
	}
	submitAll(submitters)
	for _, wp := range drivePools {
		wp.wait()
	}

	failedOn := make(map[string]bool)
	for _, f := range failures {
		failedOn[f.vol] = true
	}
	for _, j := range jobs {
		if !failedOn[j.vol] && len(j.unrecorded) == 0 {
			recordVerified(j.vol, j.dir, j.verifyCopy, began)
		}
	}

	if len(failures) > 0 || len(problems) > 0 {
		fmt.Printf("  %s %s\n", red("✗"), bold(label))
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
		for _, p := range problems {
			fmt.Printf("      %s %s\n", yellow("!"), p)
		}
		return false
	}

	total := 0
	for _, j := range jobs {
		total += len(j.entries)
	}
	fmt.Printf("  %s %s\n", green("✓"), dim(fmt.Sprintf("%s (%d files, %d drive(s))", label, total, len(jobs))))
	return true
}
