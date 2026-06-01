package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/vbauerster/mpb/v8"
)

func runReplicateAll(cfg Config, skipConf bool) bool {
	years := allYears(cfg)
	if len(years) == 0 {
		fmt.Println(dim("no missions found"))
		return true
	}
	ok := true
	for _, year := range years {
		fmt.Printf("%s\n\n", bold(strconv.Itoa(year)))
		if !runReplicate(cfg, year, skipConf) {
			ok = false
		}
		fmt.Println()
	}
	return ok
}

func runReplicate(cfg Config, year int, skipConf bool) bool {
	yearStr := strconv.Itoa(year)

	type driveState struct {
		DriveConfig
		yearDir  string
		missions map[string]bool
	}

	scanDrive := func(d DriveConfig) driveState {
		yearDir := filepath.Join(d.basePath(), d.Root, yearStr)
		missions := make(map[string]bool)
		if entries, err := os.ReadDir(yearDir); err == nil {
			for _, e := range entries {
				if e.IsDir() {
					missions[e.Name()] = true
				}
			}
		}
		return driveState{d, yearDir, missions}
	}

	// collect all mounted cold drives; any can be a source regardless of scope
	var coldDrives []driveState
	for _, d := range cfg.Drives {
		if d.Role != "cold" {
			continue
		}
		if !dirExists(d.basePath()) {
			if d.coversYear(year) {
				fmt.Printf("%s %s %s\n", yellow("warning:"), bold(d.name()), dim("not mounted, skipping"))
			}
			continue
		}
		coldDrives = append(coldDrives, scanDrive(d))
	}

	if len(coldDrives) == 0 {
		fmt.Println(red("no cold drives mounted"))
		return false
	}

	// destination drives: mounted cold drives scoped for this year
	var dstDrives []driveState
	for _, c := range coldDrives {
		if c.coversYear(year) {
			dstDrives = append(dstDrives, c)
		}
	}
	if len(dstDrives) == 0 {
		fmt.Printf(dim("no cold drives are scoped for %d\n"), year)
		return true
	}
	if len(dstDrives) == 1 {
		// check if any other cold drive could serve as a second copy
		fmt.Println(dim("only one cold drive scoped for this year — nothing to replicate"))
		return true
	}

	// build mission → source map: first cold drive that has it wins
	type missionSource struct {
		srcVol string
		srcDir string
		files  []fileEntry
		size   int64
	}
	missionSources := make(map[string]missionSource)
	for _, c := range coldDrives {
		for slug := range c.missions {
			if _, seen := missionSources[slug]; seen {
				continue
			}
			srcDir := filepath.Join(c.yearDir, slug)
			files, size, err := missionFiles(srcDir)
			if err != nil {
				fmt.Printf("%s scanning %s on %s: %v\n", red("ERROR"), slug, bold(c.name()), err)
				continue
			}
			missionSources[slug] = missionSource{c.name(), srcDir, files, size}
		}
	}

	// build jobs: for each in-scope destination, find what it's missing
	type replicateJob struct {
		slug    string
		srcDir  string
		dstDir  string
		dstVol  string
		dstBase string
		files   []fileEntry
		size    int64
	}
	var jobs []replicateJob

	var slugs []string
	for slug := range missionSources {
		slugs = append(slugs, slug)
	}
	sort.Strings(slugs)

	for _, dst := range dstDrives {
		for _, slug := range slugs {
			ms := missionSources[slug]
			dstDir := filepath.Join(dst.yearDir, slug)
			if dstDir == ms.srcDir {
				continue // source and destination are the same drive
			}
			var missing []fileEntry
			var missingSize int64
			if !dst.missions[slug] {
				missing = ms.files
				missingSize = ms.size
			} else {
				dstFiles, err := findFiles(dstDir)
				if err != nil {
					fmt.Printf("%s scanning %s on %s: %v\n", red("ERROR"), slug, bold(dst.name()), err)
					continue
				}
				dstSet := make(map[string]bool, len(dstFiles))
				for _, f := range dstFiles {
					dstSet[f.rel] = true
				}
				for _, f := range ms.files {
					if !dstSet[f.rel] {
						missing = append(missing, f)
						missingSize += f.size
					}
				}
			}
			if len(missing) > 0 {
				jobs = append(jobs, replicateJob{
					slug:    slug,
					srcDir:  ms.srcDir,
					dstDir:  dstDir,
					dstVol:  dst.name(),
					dstBase: dst.basePath(),
					files:   missing,
					size:    missingSize,
				})
			}
		}
	}

	if len(jobs) == 0 {
		fmt.Println(dim("cold drives are in sync"))
		return true
	}

	for _, j := range jobs {
		fmt.Printf("replicate: %s → %s\n", j.slug, bold(j.dstVol))
	}
	fmt.Printf("\n%d mission(s) to replicate\n", len(jobs))
	if !skipConf && !confirm() {
		exit(0, "aborted")
	}

	// interrupt handler
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	var partialDirs []string
	for _, j := range jobs {
		partialDirs = append(partialDirs, j.dstDir)
	}
	go func() {
		<-sigCh
		signal.Stop(sigCh)
		cancel()
		time.Sleep(150 * time.Millisecond)
		fmt.Print("\r\033[2K\ninterrupted — delete partial replicate dirs? (y/n): ")
		reader := bufio.NewReader(os.Stdin)
		var resp string
		for resp != "y" && resp != "n" {
			line, _ := reader.ReadString('\n')
			resp = strings.TrimSpace(line)
		}
		if resp == "y" {
			for _, d := range partialDirs {
				os.RemoveAll(d)
				fmt.Printf("removed: %s\n", d)
			}
		}
		os.Exit(130)
	}()

	archiveSize := make(map[string]int64)
	for _, j := range jobs {
		archiveSize[j.dstVol] += j.size
	}

	dstBaseByVol := make(map[string]string)
	for _, j := range jobs {
		dstBaseByVol[j.dstVol] = j.dstBase
	}
	volInfo := make(map[string]driveInfo)
	fmt.Println()
	for vol := range archiveSize {
		info := probeDrive(dstBaseByVol[vol])
		volInfo[vol] = info
		fmt.Printf("  %s: %s\n", bold(vol), info)
	}

	// Phase 1: copy
	fmt.Printf("\n%s\n\n", dim("copying..."))
	p1 := mpb.NewWithContext(ctx, mpb.WithWidth(64))
	copyBars := make(map[string]*barTracker)
	for vol, size := range archiveSize {
		copyBars[vol] = addBar(p1, vol, size)
	}

	opsByVol := make(map[string][]*op)
	for _, j := range jobs {
		opsByVol[j.dstVol] = append(opsByVol[j.dstVol],
			buildSyncOps(j.files, j.srcDir, j.dstDir, copyBars[j.dstVol])...)
	}

	var results []*result
	var resultsMu sync.Mutex
	var total atomic.Int64
	var copyPools []*pool
	for vol, ops := range opsByVol {
		wp := newPool(volInfo[vol].concurrency)
		copyPools = append(copyPools, wp)
		for _, o := range ops {
			o := o
			wp.run(func() {
				if ctx.Err() != nil {
					return
				}
				r := <-o.do()
				resultsMu.Lock()
				results = append(results, r)
				resultsMu.Unlock()
				if r.err != nil {
					fmt.Printf("\n%s %v\n", red("ERROR:"), r.err)
				} else {
					total.Add(r.n)
				}
			})
		}
	}
	for _, wp := range copyPools {
		wp.wait()
	}
	for _, t := range copyBars {
		t.flush()
	}
	p1.Wait()
	if ctx.Err() != nil {
		select {}
	}

	var copyFailed int
	for _, r := range results {
		if r != nil && r.err != nil {
			copyFailed++
		}
	}
	if copyFailed > 0 {
		exit(1, "%d file(s) failed to copy", copyFailed)
	}

	// Phase 2: verify
	fmt.Printf("\n%s\n\n", dim("verifying..."))
	p2 := mpb.NewWithContext(ctx, mpb.WithWidth(64))
	verifyBars := make(map[string]*barTracker)
	for vol, size := range archiveSize {
		verifyBars[vol] = addBar(p2, vol, size)
	}

	dstDirToVol := make(map[string]string)
	for _, j := range jobs {
		dstDirToVol[j.dstDir] = j.dstVol
	}

	var mu sync.Mutex
	checksums := make(map[string][]string)
	var verifyFailed atomic.Int64

	resultsByVol := make(map[string][]*result)
	for _, r := range results {
		if r == nil || r.err != nil {
			continue
		}
		resultsByVol[dstDirToVol[r.dstRoot]] = append(resultsByVol[dstDirToVol[r.dstRoot]], r)
	}

	var verifyPools []*pool
	for vol, rs := range resultsByVol {
		wp := newPool(volInfo[vol].concurrency)
		verifyPools = append(verifyPools, wp)
		for _, r := range rs {
			r := r
			wp.run(func() {
				if ctx.Err() != nil {
					return
				}
				got, err := hashFile(r.dst, verifyBars[dstDirToVol[r.dstRoot]])
				if err != nil || got != r.srcHash {
					fmt.Printf("\n%s %s\n", red("FAIL:"), r.dst)
					verifyFailed.Add(1)
					return
				}
				mu.Lock()
				checksums[r.dstRoot] = append(checksums[r.dstRoot],
					fmt.Sprintf("%s  %s", got, r.rel))
				mu.Unlock()
			})
		}
	}
	for _, wp := range verifyPools {
		wp.wait()
	}
	for _, t := range verifyBars {
		t.flush()
	}
	p2.Wait()

	if verifyFailed.Load() > 0 {
		exit(1, "%d file(s) failed verification", verifyFailed.Load())
	}

	for dstRoot, lines := range checksums {
		cPath := filepath.Join(dstRoot, "checksums.b3")
		lines = mergeChecksums(cPath, lines)
		sort.Strings(lines)
		if err := os.WriteFile(cPath, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
			fmt.Printf("%s writing checksums: %v\n", red("ERROR"), err)
		}
	}

	perDrive := fmtSize(uint64(total.Load()) / uint64(len(archiveSize)))
	fmt.Printf("\n%s %s replicated to %d cold drive(s)\n", green("✓"), perDrive, len(archiveSize))
	return true
}
