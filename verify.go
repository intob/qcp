package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/inneslabs/fnpool"
	"github.com/vbauerster/mpb/v8"
)

func runVerify(cfg Config, missionNum int, year int) {
	yearStr := strconv.Itoa(year)

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
		slug, err := findMissionSlug(cfg.Drives, yearStr, missionNum)
		if err != nil {
			fmt.Printf("warning: mission %03d not found on %s\n", missionNum, d.name())
			continue
		}
		dirs = append(dirs, dirEntry{d.name(), filepath.Join(base, d.Root, yearStr, slug), base})
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
			fmt.Printf("warning: cannot open %s: %v\n", cPath, err)
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

	fmt.Printf("verifying mission %03d on %d drive(s)\n", missionNum, len(jobs))
	volInfos := make(map[string]driveInfo)
	for _, job := range jobs {
		info := probeDrive(job.base)
		volInfos[job.vol] = info
		fmt.Printf("  %s: %s\n", job.vol, info)
	}
	fmt.Println()

	p := mpb.New(mpb.WithWidth(64))
	var failed atomic.Int64
	var wg sync.WaitGroup
	var trackers []*barTracker

	for _, job := range jobs {
		bar := addBar(p, job.vol, job.totalSize)
		trackers = append(trackers, bar)
		pool := fnpool.NewPool(volInfos[job.vol].concurrency)
		for _, e := range job.entries {
			wg.Add(1)
			e, dir, b := e, job.dir, bar
			pool.Dispatch(func() {
				defer wg.Done()
				got, err := hashFile(filepath.Join(dir, e.rel), b)
				if err != nil {
					fmt.Printf("\nERROR: %v\n", err)
					failed.Add(1)
					return
				}
				if got != e.hash {
					fmt.Printf("\nFAIL [%s]: %s\n", filepath.Base(dir), e.rel)
					failed.Add(1)
				}
			})
		}
	}
	wg.Wait()
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
	fmt.Printf("\nall %d files ok across %d drive(s)\n", total, len(jobs))
}
