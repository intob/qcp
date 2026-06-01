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

func runSync(cfg Config, year int, skipConf bool) {
	yearStr := strconv.Itoa(year)

	// find mounted drives and their mission sets
	type driveState struct {
		DriveConfig
		yearDir  string
		missions map[string]bool
	}

	scanDrive := func(d DriveConfig) (driveState, bool) {
		base := d.basePath()
		if !dirExists(base) {
			return driveState{}, false
		}
		yearDir := filepath.Join(base, d.Root, yearStr)
		missions := make(map[string]bool)
		if entries, err := os.ReadDir(yearDir); err == nil {
			for _, e := range entries {
				if e.IsDir() {
					missions[e.Name()] = true
				}
			}
		}
		return driveState{d, yearDir, missions}, true
	}

	var primaries []driveState
	var archives []driveState
	for _, d := range cfg.Drives {
		switch d.Role {
		case "hot":
			if !dirExists(d.basePath()) {
				fmt.Printf("warning: primary %s not mounted, skipping\n", d.name())
				continue
			}
			state, _ := scanDrive(d)
			primaries = append(primaries, state)
		case "cold":
			state, ok := scanDrive(d)
			if !ok {
				fmt.Printf("warning: archive %s not mounted, skipping\n", d.name())
				continue
			}
			archives = append(archives, state)
		}
	}
	if len(primaries) == 0 {
		exit(1, "no hot drives mounted")
	}
	if len(archives) == 0 {
		exit(1, "no cold drives mounted")
	}

	// build mission → source map across all primaries.
	// missions found on multiple primaries are cross-checked by file manifest;
	// conflicts are skipped with a warning.
	type missionSource struct {
		srcVol string
		srcDir string
		files  []fileEntry
		size   int64
	}
	missionSources := make(map[string]missionSource)
	conflicted := make(map[string]bool)

	for _, p := range primaries {
		for slug := range p.missions {
			if conflicted[slug] {
				continue
			}
			srcDir := filepath.Join(p.yearDir, slug)
			files, size, err := missionFiles(srcDir)
			if err != nil {
				fmt.Printf("ERROR scanning %s on %s: %v\n", slug, p.Volume, err)
				continue
			}
			if existing, ok := missionSources[slug]; ok {
				if !missionManifestsMatch(existing.files, files) {
					fmt.Printf("WARNING: %s differs between %s and %s — skipping\n",
						slug, existing.srcVol, p.Volume)
					delete(missionSources, slug)
					conflicted[slug] = true
				}
				// identical on both primaries — keep existing source
				continue
			}
			missionSources[slug] = missionSource{p.Volume, srcDir, files, size}
		}
	}

	// find what primaries have that each archive doesn't
	type syncJob struct {
		slug    string
		srcDir  string
		dstDir  string
		dstVol  string // display name
		dstBase string // base path for probeDrive
		files   []fileEntry
		size    int64
	}
	var jobs []syncJob
	for _, dst := range archives {
		var slugs []string
		for slug := range missionSources {
			if !dst.missions[slug] {
				slugs = append(slugs, slug)
			}
		}
		sort.Strings(slugs)
		for _, slug := range slugs {
			ms := missionSources[slug]
			jobs = append(jobs, syncJob{
				slug:    slug,
				srcDir:  ms.srcDir,
				dstDir:  filepath.Join(dst.yearDir, slug),
				dstVol:  dst.name(),
				dstBase: dst.basePath(),
				files:   ms.files,
				size:    ms.size,
			})
		}
	}

	if len(jobs) == 0 {
		fmt.Println("all drives are in sync")
		return
	}

	for _, j := range jobs {
		fmt.Printf("sync: %s → %s\n", j.slug, j.dstVol)
	}
	primaryNames := make([]string, len(primaries))
	for i, p := range primaries {
		primaryNames[i] = p.Volume
	}
	fmt.Printf("\n%d mission(s) to sync from %s\n", len(jobs), strings.Join(primaryNames, ", "))
	if !skipConf && !confirm() {
		exit(0, "aborted")
	}

	// interrupt handler — clean up any dstDirs we create
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	var dstDirs []string
	for _, j := range jobs {
		dstDirs = append(dstDirs, j.dstDir)
	}
	go func() {
		<-sigCh
		signal.Stop(sigCh)
		cancel()
		time.Sleep(150 * time.Millisecond)
		fmt.Print("\r\033[2K\ninterrupted — delete partial sync dirs? (y/n): ")
		reader := bufio.NewReader(os.Stdin)
		var resp string
		for resp != "y" && resp != "n" {
			line, _ := reader.ReadString('\n')
			resp = strings.TrimSpace(line)
		}
		if resp == "y" {
			for _, d := range dstDirs {
				os.RemoveAll(d)
				fmt.Printf("removed: %s\n", d)
			}
		}
		os.Exit(130)
	}()

	// total bytes per archive drive (for bar totals)
	archiveSize := make(map[string]int64)
	for _, j := range jobs {
		archiveSize[j.dstVol] += j.size
	}

	// probe each archive drive once and report
	volInfo := make(map[string]driveInfo)
	dstBaseByVol := make(map[string]string) // vol display name → base path
	for _, j := range jobs {
		dstBaseByVol[j.dstVol] = j.dstBase
	}
	fmt.Println()
	for vol := range archiveSize {
		info := probeDrive(dstBaseByVol[vol])
		volInfo[vol] = info
		fmt.Printf("  %s: %s\n", vol, info)
	}

	// Phase 1: copy — one bar per archive, all missions in parallel
	fmt.Printf("\ncopying...\n\n")
	p1 := mpb.NewWithContext(ctx, mpb.WithWidth(64))
	copyBars := make(map[string]*barTracker)
	for vol, size := range archiveSize {
		copyBars[vol] = addBar(p1, vol, size)
	}

	// group ops by destination volume; each volume gets its own pool sized by drive type
	opsByVol := make(map[string][]*op)
	for _, j := range jobs {
		opsByVol[j.dstVol] = append(opsByVol[j.dstVol], buildSyncOps(j.files, j.srcDir, j.dstDir, copyBars[j.dstVol])...)
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
					fmt.Printf("\nERROR: %v\n", r.err)
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
		select {} // interrupt handler will os.Exit after user responds
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

	// Phase 2: verify — one bar per archive
	fmt.Printf("\nverifying...\n\n")
	p2 := mpb.NewWithContext(ctx, mpb.WithWidth(64))
	verifyBars := make(map[string]*barTracker)
	for vol, size := range archiveSize {
		verifyBars[vol] = addBar(p2, vol, size)
	}

	dstDirToVol := make(map[string]string) // dstDir → display name
	for _, j := range jobs {
		dstDirToVol[j.dstDir] = j.dstVol
	}

	var mu sync.Mutex
	checksums := make(map[string][]string)
	var verifyFailed atomic.Int64

	// group results by destination volume for per-drive concurrency
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
					fmt.Printf("\nFAIL: %s\n", r.dst)
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
		sort.Strings(lines)
		os.WriteFile(filepath.Join(dstRoot, "checksums.b3"),
			[]byte(strings.Join(lines, "\n")+"\n"), 0644)
	}

	perArchive := fmtSize(uint64(total.Load()) / uint64(len(archiveSize)))
	fmt.Printf("\n%s synced to %d archive(s)\n", perArchive, len(archiveSize))
}

func buildSyncOps(files []fileEntry, srcDir, dstDir string, bar *barTracker) []*op {
	var ops []*op
	for _, f := range files {
		src := filepath.Join(srcDir, f.rel)
		dst := filepath.Join(dstDir, f.rel)
		ops = append(ops, &op{src: src, dst: dst, do: prepJob(src, dst, f.rel, dstDir, bar)})
	}
	return ops
}
