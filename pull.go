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

func runPull(cfg Config, missionNum int, year int, sub string, skipConf bool) {
	yearStr := strconv.Itoa(year)
	slug, err := findMissionSlug(cfg.Drives, yearStr, missionNum)
	if err != nil {
		exit(1, "mission %03d not found: %v", missionNum, err)
	}

	// find source on a cold drive — prefer the drive with the most files to
	// avoid silently pulling from a partially-synced cold drive
	var srcDir, srcVol string
	var srcCount int
	for _, d := range cfg.Drives {
		if d.Role != "cold" {
			continue
		}
		base := d.basePath()
		dir := filepath.Join(base, d.Root, yearStr, slug)
		if !dirExists(dir) {
			continue
		}
		files, err := findFiles(dir)
		if err != nil {
			continue
		}
		if len(files) > srcCount {
			srcDir = dir
			srcVol = d.name()
			srcCount = len(files)
		}
	}
	if srcDir == "" {
		exit(1, "mission %03d not found on any cold drive", missionNum)
	}

	// scan source files, filtered by sub if set
	var files []fileEntry
	if sub != "" {
		subDir := filepath.Join(srcDir, sub)
		if !dirExists(subDir) {
			exit(1, "subfolder %q not found in mission %03d", sub, missionNum)
		}
		subFiles, err := findFiles(subDir)
		if err != nil {
			exit(1, "error scanning %s: %v", subDir, err)
		}
		for _, f := range subFiles {
			files = append(files, fileEntry{rel: filepath.Join(sub, f.rel), size: f.size})
		}
	} else {
		files, err = findFiles(srcDir)
		if err != nil {
			exit(1, "error scanning source: %v", err)
		}
	}
	if len(files) == 0 {
		exit(1, "no files found in mission %03d", missionNum)
	}

	// find destination hot drives where pull is allowed
	type pullDst struct {
		name    string
		base    string
		dir     string
		missing []fileEntry
		size    int64
	}
	var dsts []pullDst
	for _, d := range cfg.Drives {
		if d.Role != "hot" || !d.pullAllowed() {
			continue
		}
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		dir := filepath.Join(base, d.Root, yearStr, slug)
		existing, scanErr := findFiles(dir)
		if scanErr != nil {
			fmt.Printf("%s scanning %s: %v\n", red("ERROR"), d.name(), scanErr)
			continue
		}
		existingSet := make(map[string]bool, len(existing))
		for _, f := range existing {
			existingSet[f.rel] = true
		}
		var missing []fileEntry
		var size int64
		for _, f := range files {
			if !existingSet[f.rel] {
				missing = append(missing, f)
				size += f.size
			}
		}
		dsts = append(dsts, pullDst{d.name(), base, dir, missing, size})
	}
	if len(dsts) == 0 {
		exit(1, "no pull-enabled hot drives mounted")
	}

	// size warning
	var totalSrc int64
	for _, f := range files {
		totalSrc += f.size
	}
	fmt.Printf("pull: %s from %s (%s total)\n\n", bold(slug), bold(srcVol), dim(fmtSize(uint64(totalSrc))))
	allUpToDate := true
	for _, d := range dsts {
		avail := availableBytes(d.base)
		alreadyBytes := totalSrc - d.size
		if d.size == 0 {
			fmt.Printf("  %-12s %s\n", d.name, dim("already up to date"))
			continue
		}
		allUpToDate = false
		warn := ""
		if d.size > int64(avail) {
			warn = " ⚠ insufficient space"
		}
		fmt.Printf("  %-12s %s to copy", d.name, dim(fmtSize(uint64(d.size))))
		if alreadyBytes > 0 {
			fmt.Printf(", %s already present", dim(fmtSize(uint64(alreadyBytes))))
		}
		fmt.Printf("  (%s available)%s\n", dim(fmtSize(avail)), warn)
	}
	if allUpToDate {
		fmt.Println(dim("all hot drives already up to date"))
		return
	}
	fmt.Println()
	if !skipConf && !confirm() {
		exit(0, "aborted")
	}

	// copy
	fmt.Printf("\n%s\n\n", dim("copying..."))
	p1 := mpb.New(mpb.WithWidth(64))
	var results []*result
	var resultsMu sync.Mutex
	var trackers []*barTracker
	var copyPools []*pool

	for _, d := range dsts {
		if len(d.missing) == 0 {
			continue
		}
		info := probeDrive(d.base)
		bar := addBar(p1, d.name, d.size)
		trackers = append(trackers, bar)
		wp := newPool(info.concurrency)
		copyPools = append(copyPools, wp)
		for _, f := range d.missing {
			f, dstRoot, b := f, d.dir, bar
			src := filepath.Join(srcDir, f.rel)
			dst := filepath.Join(dstRoot, f.rel)
			wp.run(func() {
				r := job(src, dst, b)
				r.dst = dst
				r.rel = f.rel
				r.dstRoot = dstRoot
				resultsMu.Lock()
				results = append(results, r)
				resultsMu.Unlock()
				if r.err != nil {
					fmt.Printf("\n%s %v\n", red("ERROR:"), r.err)
				}
			})
		}
	}
	for _, wp := range copyPools {
		wp.wait()
	}
	for _, t := range trackers {
		t.flush()
	}
	p1.Wait()

	var copyFailed int
	for _, r := range results {
		if r.err != nil {
			copyFailed++
		}
	}
	if copyFailed > 0 {
		exit(1, "%d file(s) failed to copy", copyFailed)
	}

	// verify
	fmt.Printf("\n%s\n\n", dim("verifying..."))
	p2 := mpb.New(mpb.WithWidth(64))
	verifyTrackers := make(map[string]*barTracker)
	verifySize := make(map[string]int64)
	dstRootToName := make(map[string]string)
	dstRootToBase := make(map[string]string)
	for _, d := range dsts {
		dstRootToName[d.dir] = d.name
		dstRootToBase[d.dir] = d.base
	}
	for _, r := range results {
		if r.err == nil {
			verifySize[r.dstRoot] += r.n
		}
	}
	for dstRoot, size := range verifySize {
		verifyTrackers[dstRoot] = addBar(p2, dstRootToName[dstRoot], size)
	}

	var verifyFailed atomic.Int64
	newHashes := make(map[string][]string)
	var newHashesMu sync.Mutex
	resultsByDst := make(map[string][]*result)
	for _, r := range results {
		if r.err == nil {
			resultsByDst[r.dstRoot] = append(resultsByDst[r.dstRoot], r)
		}
	}
	var verifyPools []*pool
	for dstRoot, rs := range resultsByDst {
		info := probeDrive(dstRootToBase[dstRoot])
		wp := newPool(info.concurrency)
		verifyPools = append(verifyPools, wp)
		for _, r := range rs {
			r := r
			wp.run(func() {
				got, err := hashFile(r.dst, verifyTrackers[r.dstRoot])
				if err != nil || got != r.srcHash {
					fmt.Printf("\n%s %s\n", red("FAIL:"), r.dst)
					verifyFailed.Add(1)
					return
				}
				newHashesMu.Lock()
				newHashes[r.dstRoot] = append(newHashes[r.dstRoot], fmt.Sprintf("%s  %s", got, r.rel))
				newHashesMu.Unlock()
			})
		}
	}
	for _, wp := range verifyPools {
		wp.wait()
	}
	for _, t := range verifyTrackers {
		t.flush()
	}
	p2.Wait()

	if verifyFailed.Load() > 0 {
		exit(1, "%d file(s) failed verification", verifyFailed.Load())
	}

	// merge into checksums.b3
	for dstRoot, lines := range newHashes {
		cPath := filepath.Join(dstRoot, "checksums.b3")
		lines = mergeChecksums(cPath, lines)
		sort.Strings(lines)
		if err := os.WriteFile(cPath, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
			fmt.Printf("%s writing checksums: %v\n", red("ERROR"), err)
		}
	}

	total := 0
	for _, r := range results {
		if r.err == nil {
			total++
		}
	}
	fmt.Printf("\n%s %d file(s) copied and verified\n", green("✓"), total)
}
