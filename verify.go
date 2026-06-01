package main

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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
