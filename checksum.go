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
