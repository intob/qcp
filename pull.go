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

// pullSource is one resolved mission on the cold drive it will be pulled from.
type pullSource struct {
	num     int
	slug    string
	srcDir  string
	srcVol  string
	srcBase string
	files   []fileEntry
	size    int64
}

// pullJob is the set of files one mission is missing on one hot drive.
type pullJob struct {
	slug    string
	srcDir  string
	srcVol  string
	srcBase string
	dstDir  string
	dstVol  string
	files   []fileEntry
	size    int64
}

func runPull(cfg Config, missions []int, year int, sub string, skipConf bool) {
	yearStr := strconv.Itoa(year)

	// resolve every requested mission before copying anything, so typos and
	// unmounted archives surface up front rather than mid-batch
	var sources []pullSource
	unresolved := 0
	for _, num := range missions {
		s, err := resolvePullSource(cfg, yearStr, num, sub)
		if err != nil {
			fmt.Printf("%s mission %03d: %v\n", red("ERROR"), num, err)
			unresolved++
			continue
		}
		sources = append(sources, s)
	}
	if len(sources) == 0 {
		exit(1, "nothing to pull")
	}

	// find destination hot drives where pull is allowed, and work out what
	// each is missing across the whole batch
	var jobs []pullJob
	var dstVols []string // display names, in config order
	dstBase := make(map[string]string)
	toCopy := make(map[string]int64)
	already := make(map[string]int64)
	for _, d := range cfg.Drives {
		if d.Role != "hot" || !d.pullAllowed() {
			continue
		}
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		vol := d.name()
		dstVols = append(dstVols, vol)
		dstBase[vol] = base
		for _, s := range sources {
			dir := filepath.Join(base, d.Root, yearStr, s.slug)
			existing := make(map[string]bool)
			if dirExists(dir) {
				found, scanErr := findFiles(dir)
				if scanErr != nil {
					fmt.Printf("%s scanning %s on %s: %v\n", red("ERROR"), s.slug, bold(vol), scanErr)
					unresolved++
					continue
				}
				for _, f := range found {
					existing[f.rel] = true
				}
			}
			var missing []fileEntry
			var size int64
			for _, f := range s.files {
				if existing[f.rel] {
					already[vol] += f.size
					continue
				}
				missing = append(missing, f)
				size += f.size
			}
			toCopy[vol] += size
			if len(missing) > 0 {
				jobs = append(jobs, pullJob{s.slug, s.srcDir, s.srcVol, s.srcBase, dir, vol, missing, size})
			}
		}
	}
	if len(dstVols) == 0 {
		exit(1, "no pull-enabled hot drives mounted")
	}

	printPullPlan(sources, dstVols, dstBase, toCopy, already)
	if len(jobs) == 0 {
		fmt.Println(dim("all hot drives already up to date"))
		if unresolved > 0 {
			os.Exit(1)
		}
		return
	}
	fmt.Println()
	if !skipConf && !confirm() {
		exit(0, "aborted")
	}

	// interrupt handler — only offer to delete dirs this run will create,
	// not dirs that already existed (partial-diff jobs)
	ctx, cancel := context.WithCancel(context.Background())
	var newDirs []string
	for _, j := range jobs {
		if !dirExists(j.dstDir) {
			newDirs = append(newDirs, j.dstDir)
		}
	}
	watchInterrupt(ctx, cancel, newDirs)

	// bar totals are per drive across the whole batch
	sizeByVol := make(map[string]int64)
	for _, j := range jobs {
		sizeByVol[j.dstVol] += j.size
	}
	var volOrder []string
	for _, vol := range dstVols {
		if sizeByVol[vol] > 0 {
			volOrder = append(volOrder, vol)
		}
	}

	// probe every drive involved once, and limit reads per source drive
	srcLimit := newSourceLimiter()
	for _, j := range jobs {
		srcLimit.add(j.srcVol, j.srcBase)
	}
	volInfo := make(map[string]driveInfo)
	for _, vol := range volOrder {
		volInfo[vol] = probeDrive(dstBase[vol])
	}
	fmt.Println()
	srcLimit.report()
	for _, vol := range volOrder {
		fmt.Printf("  %s %s: %s\n", dim("to  "), bold(vol), volInfo[vol])
	}

	// copy — one bar per drive, all missions in parallel
	fmt.Printf("\n%s\n\n", dim("copying..."))
	p1 := mpb.NewWithContext(ctx, mpb.WithWidth(64))
	copyBars := make(map[string]*barTracker, len(volOrder))
	for _, vol := range volOrder {
		copyBars[vol] = addBar(p1, vol, sizeByVol[vol])
	}

	jobsByVol := make(map[string][]pullJob)
	for _, j := range jobs {
		jobsByVol[j.dstVol] = append(jobsByVol[j.dstVol], j)
	}

	var results []*result
	var resultsMu sync.Mutex
	var copyPools []*pool
	var copySubmit []func()
	for vol, volJobs := range jobsByVol {
		bar := copyBars[vol]
		wp := newPool(volInfo[vol].concurrency)
		copyPools = append(copyPools, wp)
		copySubmit = append(copySubmit, func() {
			for _, j := range volJobs {
				for _, f := range j.files {
					f, srcDir, srcVol, dstRoot := f, j.srcDir, j.srcVol, j.dstDir
					wp.run(func() {
						if ctx.Err() != nil {
							return
						}
						release := srcLimit.acquire(srcVol)
						defer release()
						if ctx.Err() != nil {
							return
						}
						dst := filepath.Join(dstRoot, f.rel)
						r := job(filepath.Join(srcDir, f.rel), dst, bar)
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
		})
	}
	submitAll(copySubmit)
	for _, wp := range copyPools {
		wp.wait()
	}
	for _, t := range copyBars {
		t.flush()
	}
	p1.Wait()
	if ctx.Err() != nil {
		select {} // interrupt handler will os.Exit once the user responds
	}

	var copyFailed int
	for _, r := range results {
		if r.err != nil {
			copyFailed++
		}
	}
	if copyFailed > 0 {
		exit(1, "%d file(s) failed to copy", copyFailed)
	}

	// verify — one bar per drive
	fmt.Printf("\n%s\n\n", dim("verifying..."))
	p2 := mpb.NewWithContext(ctx, mpb.WithWidth(64))
	dstRootToVol := make(map[string]string)
	for _, j := range jobs {
		dstRootToVol[j.dstDir] = j.dstVol
	}
	verifySize := make(map[string]int64)
	for _, r := range results {
		if r.err == nil {
			verifySize[dstRootToVol[r.dstRoot]] += r.n
		}
	}
	verifyBars := make(map[string]*barTracker, len(volOrder))
	for _, vol := range volOrder {
		verifyBars[vol] = addBar(p2, vol, verifySize[vol])
	}

	var verifyFailed atomic.Int64
	newHashes := make(map[string][]string)
	var newHashesMu sync.Mutex
	resultsByVol := make(map[string][]*result)
	for _, r := range results {
		if r.err == nil {
			resultsByVol[dstRootToVol[r.dstRoot]] = append(resultsByVol[dstRootToVol[r.dstRoot]], r)
		}
	}
	var verifyPools []*pool
	var verifySubmit []func()
	for vol, rs := range resultsByVol {
		bar := verifyBars[vol]
		wp := newPool(volInfo[vol].concurrency)
		verifyPools = append(verifyPools, wp)
		verifySubmit = append(verifySubmit, func() {
			for _, r := range rs {
				r := r
				wp.run(func() {
					if ctx.Err() != nil {
						return
					}
					got, err := hashFile(r.dst, bar)
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
		})
	}
	submitAll(verifySubmit)
	for _, wp := range verifyPools {
		wp.wait()
	}
	for _, t := range verifyBars {
		t.flush()
	}
	p2.Wait()
	if ctx.Err() != nil {
		select {} // interrupt handler will os.Exit once the user responds
	}

	if verifyFailed.Load() > 0 {
		exit(1, "%d file(s) failed verification", verifyFailed.Load())
	}

	// merge into each mission's checksums.b3
	for dstRoot, lines := range newHashes {
		cPath := filepath.Join(dstRoot, "checksums.b3")
		lines = mergeChecksums(cPath, lines)
		sort.Strings(lines)
		if err := os.WriteFile(cPath, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
			fmt.Printf("%s writing checksums: %v\n", red("ERROR"), err)
		}
	}

	pulled := make(map[string]bool)
	for _, j := range jobs {
		pulled[j.slug] = true
	}
	total := 0
	for _, r := range results {
		if r.err == nil {
			total++
		}
	}
	if len(pulled) == 1 {
		fmt.Printf("\n%s %d file(s) copied and verified\n", green("✓"), total)
	} else {
		fmt.Printf("\n%s %d file(s) copied and verified across %d mission(s)\n", green("✓"), total, len(pulled))
	}
	if unresolved > 0 {
		os.Exit(1)
	}
}

// resolvePullSource locates a mission on the fullest cold drive holding it and
// lists the files to pull, restricted to sub if set.
func resolvePullSource(cfg Config, yearStr string, num int, sub string) (pullSource, error) {
	slug, err := findMissionSlug(cfg.Drives, yearStr, num)
	if err != nil {
		return pullSource{}, err
	}

	// prefer the cold drive with the most files, so a partially-synced drive
	// is never silently used as the source
	var srcDir, srcVol, srcBase string
	var files []fileEntry
	for _, d := range cfg.Drives {
		if d.Role != "cold" {
			continue
		}
		base := d.basePath()
		dir := filepath.Join(base, d.Root, yearStr, slug)
		if !dirExists(dir) {
			continue
		}
		found, err := findFiles(dir)
		if err != nil {
			continue
		}
		if len(found) > len(files) {
			srcDir, srcVol, srcBase, files = dir, d.name(), base, found
		}
	}
	if srcDir == "" {
		return pullSource{}, fmt.Errorf("not found on any cold drive")
	}

	if sub != "" {
		if !dirExists(filepath.Join(srcDir, sub)) {
			return pullSource{}, fmt.Errorf("subfolder %q not found", sub)
		}
		prefix := sub + string(os.PathSeparator)
		var filtered []fileEntry
		for _, f := range files {
			if strings.HasPrefix(f.rel, prefix) {
				filtered = append(filtered, f)
			}
		}
		files = filtered
	}
	if len(files) == 0 {
		return pullSource{}, fmt.Errorf("no files found")
	}

	var size int64
	for _, f := range files {
		size += f.size
	}
	return pullSource{num, slug, srcDir, srcVol, srcBase, files, size}, nil
}

func printPullPlan(sources []pullSource, dstVols []string, dstBase map[string]string, toCopy, already map[string]int64) {
	var totalSrc int64
	for _, s := range sources {
		totalSrc += s.size
	}
	if len(sources) == 1 {
		s := sources[0]
		fmt.Printf("pull: %s from %s (%s total)\n\n", bold(s.slug), bold(s.srcVol), dim(fmtSize(uint64(s.size))))
	} else {
		fmt.Printf("pull: %s mission(s) (%s total)\n\n", bold(strconv.Itoa(len(sources))), dim(fmtSize(uint64(totalSrc))))
		width := 0
		for _, s := range sources {
			width = max(width, len(s.slug))
		}
		for _, s := range sources {
			fmt.Printf("  %-*s  %8s  %s\n", width, s.slug, fmtSize(uint64(s.size)), dim("from "+s.srcVol))
		}
		fmt.Println()
	}
	for _, vol := range dstVols {
		if toCopy[vol] == 0 {
			fmt.Printf("  %-12s %s\n", vol, dim("already up to date"))
			continue
		}
		avail := availableBytes(dstBase[vol])
		warn := ""
		if toCopy[vol] > int64(avail) {
			warn = " ⚠ insufficient space"
		}
		fmt.Printf("  %-12s %s to copy", vol, dim(fmtSize(uint64(toCopy[vol]))))
		if already[vol] > 0 {
			fmt.Printf(", %s already present", dim(fmtSize(uint64(already[vol]))))
		}
		fmt.Printf("  (%s available)%s\n", dim(fmtSize(avail)), warn)
	}
}

// watchInterrupt cancels ctx on SIGINT, then offers to remove the directories
// this run created before exiting.
func watchInterrupt(ctx context.Context, cancel context.CancelFunc, newDirs []string) {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		signal.Stop(sigCh)
		cancel()
		time.Sleep(150 * time.Millisecond)
		if len(newDirs) == 0 {
			fmt.Print("\r\033[2K\ninterrupted\n")
			os.Exit(130)
		}
		fmt.Print("\r\033[2K\ninterrupted — delete partial mission dirs? (y/n): ")
		reader := bufio.NewReader(os.Stdin)
		var resp string
		for resp != "y" && resp != "n" {
			line, err := reader.ReadString('\n')
			resp = strings.TrimSpace(line)
			if err != nil {
				break // stdin closed; don't delete
			}
		}
		if resp == "y" {
			for _, d := range newDirs {
				os.RemoveAll(d)
				fmt.Printf("removed: %s\n", d)
			}
		}
		os.Exit(130)
	}()
}
