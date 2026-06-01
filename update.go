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

func runUpdate(cfg Config, missionNum int, year int, skipConf bool) {
	yearStr := strconv.Itoa(year)
	slug, err := findMissionSlug(cfg.Drives, yearStr, missionNum)
	if err != nil {
		exit(1, "mission %03d not found: %v", missionNum, err)
	}

	// find primary and archive drives
	var primaryDir string
	type archiveDrive struct {
		vol  string
		dir  string
		base string
	}
	var archives []archiveDrive
	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		dir := filepath.Join(base, d.Root, yearStr, slug)
		if d.Role == "hot" {
			if !dirExists(dir) {
				exit(1, "mission %03d not found on hot drive %s", missionNum, d.name())
			}
			primaryDir = dir
		} else {
			if dirExists(dir) {
				archives = append(archives, archiveDrive{d.name(), dir, base})
			} else {
				fmt.Printf("warning: mission not found on %s, skipping\n", d.name())
			}
		}
	}
	if primaryDir == "" {
		exit(1, "no hot drive mounted")
	}
	if len(archives) == 0 {
		exit(1, "no cold drives mounted with this mission")
	}

	// index all files on primary
	primaryFiles, err := findFiles(primaryDir)
	if err != nil {
		exit(1, "error scanning primary: %v", err)
	}
	primarySet := make(map[string]int64, len(primaryFiles))
	for _, f := range primaryFiles {
		primarySet[f.rel] = f.size
	}

	// find missing files per archive
	type archiveJob struct {
		archiveDrive
		missing []fileEntry
		size    int64
	}
	var jobs []archiveJob
	for _, a := range archives {
		existing, err := findFiles(a.dir)
		if err != nil {
			fmt.Printf("warning: error scanning %s: %v\n", a.vol, err)
			continue
		}
		existingSet := make(map[string]bool, len(existing))
		for _, f := range existing {
			existingSet[f.rel] = true
		}
		var missing []fileEntry
		var size int64
		for _, f := range primaryFiles {
			if !existingSet[f.rel] {
				missing = append(missing, f)
				size += f.size
			}
		}
		if len(missing) > 0 {
			jobs = append(jobs, archiveJob{a, missing, size})
		} else {
			fmt.Printf("%s: already up to date\n", a.vol)
		}
	}
	if len(jobs) == 0 {
		fmt.Println("all archive drives are up to date")
		return
	}

	for _, j := range jobs {
		fmt.Printf("update: %d file(s) (%s) → %s\n", len(j.missing), fmtSize(uint64(j.size)), j.vol)
	}
	if !skipConf && !confirm() {
		exit(0, "aborted")
	}

	// copy missing files per archive drive
	fmt.Printf("\ncopying...\n\n")
	p1 := mpb.New(mpb.WithWidth(64))
	var copyResults []struct {
		dst     string
		rel     string
		dstRoot string
		vol     string
		srcHash string
		err     error
	}
	var resultsMu sync.Mutex
	var trackers []*barTracker
	var copyPools []*pool

	for _, j := range jobs {
		info := probeDrive(j.base)
		bar := addBar(p1, j.vol, j.size)
		trackers = append(trackers, bar)
		wp := newPool(info.concurrency)
		copyPools = append(copyPools, wp)
		for _, f := range j.missing {
			f, dstRoot := f, j.dir
			src := filepath.Join(primaryDir, f.rel)
			dst := filepath.Join(dstRoot, f.rel)
			wp.run(func() {
				r := job(src, dst, bar)
				resultsMu.Lock()
				copyResults = append(copyResults, struct {
					dst     string
					rel     string
					dstRoot string
					vol     string
					srcHash string
					err     error
				}{dst, f.rel, dstRoot, j.vol, r.srcHash, r.err})
				resultsMu.Unlock()
				if r.err != nil {
					fmt.Printf("\nERROR: %v\n", r.err)
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
	for _, r := range copyResults {
		if r.err != nil {
			copyFailed++
		}
	}
	if copyFailed > 0 {
		exit(1, "%d file(s) failed to copy", copyFailed)
	}

	// verify copied files
	fmt.Printf("\nverifying...\n\n")
	p2 := mpb.New(mpb.WithWidth(64))
	verifyTrackers := make(map[string]*barTracker)
	verifySize := make(map[string]int64)
	for _, r := range copyResults {
		if r.err == nil {
			verifySize[r.vol] += primarySet[r.rel]
		}
	}
	for vol, size := range verifySize {
		verifyTrackers[vol] = addBar(p2, vol, size)
	}

	var verifyFailed atomic.Int64
	newHashes := make(map[string][]string) // dstRoot → "hash  rel" lines
	var newHashesMu sync.Mutex

	type verifyItem struct{ dst, rel, dstRoot, srcHash string }
	resultsByVol := make(map[string][]verifyItem)
	for _, r := range copyResults {
		if r.err == nil {
			resultsByVol[r.vol] = append(resultsByVol[r.vol], verifyItem{r.dst, r.rel, r.dstRoot, r.srcHash})
		}
	}
	archiveBaseByVol := make(map[string]string)
	for _, a := range archives {
		archiveBaseByVol[a.vol] = a.base
	}
	updateVolInfos := make(map[string]driveInfo)
	for vol := range resultsByVol {
		updateVolInfos[vol] = probeDrive(archiveBaseByVol[vol])
	}

	var verifyPools []*pool
	for vol, rs := range resultsByVol {
		wp := newPool(updateVolInfos[vol].concurrency)
		verifyPools = append(verifyPools, wp)
		for _, r := range rs {
			r := r
			wp.run(func() {
				got, err := hashFile(r.dst, verifyTrackers[vol])
				if err != nil || got != r.srcHash {
					fmt.Printf("\nFAIL: %s\n", r.dst)
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

	// append new entries to checksums.b3 on each archive drive
	for dstRoot, lines := range newHashes {
		cPath := filepath.Join(dstRoot, "checksums.b3")
		existing, _ := os.ReadFile(cPath)
		all := append(strings.Split(strings.TrimRight(string(existing), "\n"), "\n"), lines...)
		// remove empty entries that may result from empty file
		var clean []string
		for _, l := range all {
			if l != "" {
				clean = append(clean, l)
			}
		}
		sort.Strings(clean)
		if err := os.WriteFile(cPath, []byte(strings.Join(clean, "\n")+"\n"), 0644); err != nil {
			fmt.Printf("ERROR writing checksums: %v\n", err)
		}
	}

	total := 0
	for _, j := range jobs {
		total += len(j.missing)
	}
	fmt.Printf("\n%d file(s) copied and verified\n", total)
}
