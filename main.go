package main

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/signal"
	"os/user"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/inneslabs/fnpool"
	"github.com/inneslabs/jfmt"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"lukechampine.com/blake3"
)

const seqPath = "~/.qcp_seq"

type Config struct {
	Cards  []CardConfig  `json:"cards"`
	Drives []DriveConfig `json:"drives"`
}

type CardConfig struct {
	Volume string `json:"volume"`
	Sub    string `json:"sub"`
}

type DriveConfig struct {
	Volume string `json:"volume"`
	Root   string `json:"root"`
	Role   string `json:"role"` // "hot" or "cold"
}

type mountedCard struct {
	CardConfig
	src string
}

type fileEntry struct {
	rel  string
	size int64
}

type op struct {
	src, dst string
	do       func() <-chan *result
}

type result struct {
	err     error
	n       int64
	srcHash string
	dst     string
	rel     string
	dstRoot string
}

const ewmaWindow = 250 * time.Millisecond

// barTracker aggregates increments from concurrent goroutines and feeds the
// EWMA with a single coherent elapsed-time stream. Each bar gets one tracker;
// all goroutines writing to that bar share it via incr().
type barTracker struct {
	bar     *mpb.Bar
	mu      sync.Mutex
	pending int64
	last    time.Time
}

func (t *barTracker) incr(n int) {
	if n <= 0 {
		return
	}
	t.mu.Lock()
	t.pending += int64(n)
	now := time.Now()
	if t.last.IsZero() {
		t.last = now
		t.mu.Unlock()
		return
	}
	if elapsed := now.Sub(t.last); elapsed >= ewmaWindow {
		t.bar.EwmaIncrBy(int(t.pending), elapsed)
		t.pending = 0
		t.last = now
	}
	t.mu.Unlock()
}

// flush sends any remaining pending bytes after all goroutines have finished.
func (t *barTracker) flush() {
	t.mu.Lock()
	if t.pending > 0 && !t.last.IsZero() {
		t.bar.EwmaIncrBy(int(t.pending), time.Since(t.last))
		t.pending = 0
	}
	t.mu.Unlock()
}

type progressWriter struct {
	w       io.Writer
	tracker *barTracker
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n, err := pw.w.Write(p)
	if pw.tracker != nil {
		pw.tracker.incr(n)
	}
	return n, err
}

type progressReader struct {
	r       io.Reader
	tracker *barTracker
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.r.Read(p)
	if pr.tracker != nil {
		pr.tracker.incr(n)
	}
	return n, err
}

func main() {
	skipConf := flag.Bool("y", false, "skip confirmation")
	missionFlag := flag.String("mission", "", "mission name (e.g. \"Altissimo with Anton\")")
	year := flag.Int("year", time.Now().Year(), "year override")
	toMission := flag.Int("to", 0, "append to existing mission number")
	verifyMission := flag.Int("verify", 0, "re-verify mission number across all mounted drives")
	checksumMission := flag.Int("checksum", 0, "generate checksums.b3 for a mission by cross-verifying all mounted drives")
	updateMission := flag.Int("update", 0, "copy files missing from cold drives for an existing mission")
	doSync := flag.Bool("sync", false, "sync missions from hot drives to cold drives")
	flag.Parse()

	cfg := loadConfig()

	if *doSync {
		runSync(cfg, *year, *skipConf)
		return
	}

	if *updateMission > 0 {
		runUpdate(cfg, *updateMission, *year, *skipConf)
		return
	}

	if *verifyMission > 0 {
		runVerify(cfg, *verifyMission)
		return
	}

	if *checksumMission > 0 {
		runChecksum(cfg, *checksumMission, *year)
		return
	}

	cards := mountedCards(cfg.Cards)
	if len(cards) == 0 {
		exit(1, "no configured cards mounted")
	}

	yearStr := strconv.Itoa(*year)
	var missionSlug string
	var isAppend bool
	var missionNum int

	if *toMission > 0 {
		isAppend = true
		slug, err := findMissionSlug(cfg.Drives, yearStr, *toMission)
		if err != nil {
			exit(2, "mission %03d not found: %v", *toMission, err)
		}
		missionSlug = slug
	} else {
		if *missionFlag == "" {
			exit(3, "-mission is required")
		}
		num, err := peekMission(*year)
		if err != nil {
			exit(4, "err reading mission counter: %v", err)
		}
		missionNum = num
		missionSlug = fmt.Sprintf("%03d_%s", num, sanitizeMission(*missionFlag))
	}

	var dstRoots []string
	for _, d := range cfg.Drives {
		vol := filepath.Join("/Volumes", d.Volume)
		if !dirExists(vol) {
			fmt.Printf("warning: %s not mounted, skipping\n", d.Volume)
			continue
		}
		dstRoots = append(dstRoots, filepath.Join(vol, d.Root, yearStr, missionSlug))
	}
	if len(dstRoots) == 0 {
		exit(5, "no destination drives mounted")
	}

	// scan cards for file lists + sizes
	var scanned []scannedCard
	var totalSize int64
	var totalFiles int
	for _, card := range cards {
		files, err := findFiles(card.src)
		if err != nil {
			exit(6, "err scanning %s: %v", card.Volume, err)
		}
		scanned = append(scanned, scannedCard{card, files})
		for _, f := range files {
			totalSize += f.size
		}
		totalFiles += len(files)
	}
	if totalFiles == 0 {
		exit(7, "no files found on mounted cards")
	}

	// print plan from scanned data
	for _, sc := range scanned {
		for _, f := range sc.files {
			src := filepath.Join(sc.src, f.rel)
			for _, dstRoot := range dstRoots {
				dst := filepath.Join(dstRoot, sc.Volume, f.rel)
				fmt.Printf("plan: %s\n      -> %s\n", src, dst)
			}
		}
	}

	fmt.Printf("\nmission:      %s\n", missionSlug)
	fmt.Printf("destinations: %s\n", strings.Join(dstRoots, "\n              "))
	if !*skipConf && !confirm() {
		exit(8, "aborted by user")
	}

	if !isAppend {
		if err := commitMission(*year, missionNum); err != nil {
			exit(9, "err updating mission counter: %v", err)
		}
	}

	// set up interrupt handler — from this point we have created dirs / committed seq
	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		signal.Stop(sigCh)
		cancel()                          // stop mpb rendering via context
		time.Sleep(150 * time.Millisecond) // let mpb goroutine exit
		reader := bufio.NewReader(os.Stdin)
		var resp string
		for resp != "y" && resp != "n" {
			fmt.Print("\r\033[2K\ninterrupted — delete partial mission and revert counter? (y/n): ")
			line, _ := reader.ReadString('\n')
			resp = strings.TrimSpace(line)
		}
		if resp == "y" {
			for _, d := range dstRoots {
				os.RemoveAll(d)
				fmt.Printf("removed: %s\n", d)
			}
			if !isAppend {
				if err := revertMission(*year); err != nil {
					fmt.Printf("err reverting counter: %v\n", err)
				} else {
					fmt.Println("mission counter reverted")
				}
			}
		}
		os.Exit(130)
	}()

	sizeStr := jfmt.FmtSize64(uint64(totalSize))
	fmt.Printf("\ncopying %d files (%s) to %d drive(s)\n\n", totalFiles, sizeStr, len(dstRoots))

	// Phase 1: copy
	p1 := mpb.NewWithContext(ctx, mpb.WithWidth(64))
	copyBars := make(map[string]*barTracker)
	for _, dstRoot := range dstRoots {
		copyBars[dstRoot] = addBar(p1, volName(dstRoot), totalSize)
	}
	ops := buildOps(scanned, dstRoots, copyBars)

	results := make([]*result, len(ops))
	pool := fnpool.NewPool(runtime.NumCPU())
	var total atomic.Int64
	var wg sync.WaitGroup
	for i, op := range ops {
		wg.Add(1)
		i, op := i, op
		pool.Dispatch(func() {
			defer wg.Done()
			if ctx.Err() != nil {
				return
			}
			r := <-op.do()
			results[i] = r
			if r.err != nil {
				fmt.Printf("\nERROR copy: %v\n", r.err)
				return
			}
			total.Add(r.n)
		})
	}
	wg.Wait()
	for _, t := range copyBars {
		t.flush()
	}
	p1.Wait()

	var copyFailed int
	for _, r := range results {
		if r != nil && r.err != nil {
			copyFailed++
		}
	}
	if copyFailed > 0 {
		exit(10, "%d file(s) failed to copy", copyFailed)
	}

	// Phase 2: verify
	fmt.Printf("\nverifying...\n\n")
	p2 := mpb.NewWithContext(ctx, mpb.WithWidth(64))
	verifyBars := make(map[string]*barTracker)
	for _, dstRoot := range dstRoots {
		verifyBars[dstRoot] = addBar(p2, volName(dstRoot), totalSize)
	}

	var mu sync.Mutex
	newChecksums := make(map[string][]string)
	var verifyFailed atomic.Int64
	var wg2 sync.WaitGroup
	pool2 := fnpool.NewPool(runtime.NumCPU())
	for _, r := range results {
		if r == nil {
			continue
		}
		wg2.Add(1)
		r := r
		pool2.Dispatch(func() {
			defer wg2.Done()
			if ctx.Err() != nil {
				return
			}
			got, err := hashFile(r.dst, verifyBars[r.dstRoot])
			if err != nil {
				fmt.Printf("\nERROR verify: %v\n", err)
				verifyFailed.Add(1)
				return
			}
			if got != r.srcHash {
				fmt.Printf("\nMISMATCH: %s\n", r.dst)
				verifyFailed.Add(1)
				return
			}
			mu.Lock()
			newChecksums[r.dstRoot] = append(newChecksums[r.dstRoot],
				fmt.Sprintf("%s  %s", got, r.rel))
			mu.Unlock()
		})
	}
	wg2.Wait()
	for _, t := range verifyBars {
		t.flush()
	}
	p2.Wait()

	if verifyFailed.Load() > 0 {
		exit(11, "%d file(s) failed verification", verifyFailed.Load())
	}

	for dstRoot, lines := range newChecksums {
		cPath := filepath.Join(dstRoot, "checksums.b3")
		if isAppend {
			lines = mergeChecksums(cPath, lines)
		}
		sort.Strings(lines)
		if err := os.WriteFile(cPath, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
			fmt.Printf("ERROR writing checksums: %v\n", err)
		}
	}

	perDrive := jfmt.FmtSize64(uint64(total.Load()) / uint64(len(dstRoots)))
	fmt.Printf("\n%s copied and verified → %s\n", perDrive, missionSlug)
}

func addBar(p *mpb.Progress, name string, total int64) *barTracker {
	bar := p.AddBar(total,
		mpb.PrependDecorators(
			decor.Name(fmt.Sprintf("%-12s", name)),
			decor.CountersKibiByte("% .1f / % .1f  "),
		),
		mpb.AppendDecorators(
			decor.EwmaSpeed(decor.SizeB1024(0), "% .1f  ", 30),
			decor.OnComplete(decor.EwmaETA(decor.ET_STYLE_GO, 150), "✓"),
		),
	)
	return &barTracker{bar: bar}
}

type driveInfo struct {
	concurrency int
	kind        string // "SSD" or "HDD"
	protocol    string // e.g. "USB", "Thunderbolt", "NVMe", "SATA"
}

func (d driveInfo) String() string {
	return fmt.Sprintf("%s · %s · %d worker(s)", d.kind, d.protocol, d.concurrency)
}

// probeDrive queries diskutil for drive type and protocol, then picks
// an appropriate concurrency (SSDs benefit from queue depth; HDDs need sequential I/O).
func probeDrive(volPath string) driveInfo {
	out, err := exec.Command("diskutil", "info", volPath).Output()
	if err != nil {
		return driveInfo{concurrency: 1, kind: "HDD", protocol: "unknown"}
	}
	s := string(out)

	kind := "HDD"
	concurrency := 1
	if strings.Contains(s, "Solid State:               Yes") {
		kind = "SSD"
		concurrency = 4
	}

	protocol := "unknown"
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "Protocol:") {
			protocol = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
			break
		}
	}

	return driveInfo{concurrency: concurrency, kind: kind, protocol: protocol}
}

func volName(dstRoot string) string {
	parts := strings.SplitN(dstRoot, string(os.PathSeparator), 4)
	if len(parts) >= 3 {
		return parts[2]
	}
	return dstRoot
}

func runSync(cfg Config, year int, skipConf bool) {
	yearStr := strconv.Itoa(year)

	// find mounted drives and their mission sets
	type driveState struct {
		DriveConfig
		yearDir  string
		missions map[string]bool
	}

	scanDrive := func(d DriveConfig) (driveState, bool) {
		vol := filepath.Join("/Volumes", d.Volume)
		if !dirExists(vol) {
			return driveState{}, false
		}
		yearDir := filepath.Join(vol, d.Root, yearStr)
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
			if !dirExists(filepath.Join("/Volumes", d.Volume)) {
				fmt.Printf("warning: primary %s not mounted, skipping\n", d.Volume)
				continue
			}
			state, _ := scanDrive(d)
			primaries = append(primaries, state)
		case "cold":
			state, ok := scanDrive(d)
			if !ok {
				fmt.Printf("warning: archive %s not mounted, skipping\n", d.Volume)
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
		slug   string
		srcDir string
		dstDir string
		dstVol string
		files  []fileEntry
		size   int64
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
				slug:   slug,
				srcDir: ms.srcDir,
				dstDir: filepath.Join(dst.yearDir, slug),
				dstVol: dst.Volume,
				files:  ms.files,
				size:   ms.size,
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
	fmt.Println()
	for vol := range archiveSize {
		info := probeDrive("/Volumes/" + vol)
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
	var wg sync.WaitGroup
	for vol, ops := range opsByVol {
		pool := fnpool.NewPool(volInfo[vol].concurrency)
		for _, op := range ops {
			wg.Add(1)
			op := op
			pool.Dispatch(func() {
				defer wg.Done()
				if ctx.Err() != nil {
					return
				}
				r := <-op.do()
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
	wg.Wait()
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

	var mu sync.Mutex
	checksums := make(map[string][]string)
	var verifyFailed atomic.Int64

	// group results by destination volume for per-drive concurrency
	resultsByVol := make(map[string][]*result)
	for _, r := range results {
		if r == nil || r.err != nil {
			continue
		}
		vol := volName(r.dstRoot)
		resultsByVol[vol] = append(resultsByVol[vol], r)
	}

	var wg2 sync.WaitGroup
	for vol, rs := range resultsByVol {
		pool2 := fnpool.NewPool(volInfo[vol].concurrency)
		for _, r := range rs {
			wg2.Add(1)
			r := r
			pool2.Dispatch(func() {
				defer wg2.Done()
				if ctx.Err() != nil {
					return
				}
				got, err := hashFile(r.dst, verifyBars[volName(r.dstRoot)])
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
	wg2.Wait()
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

	perArchive := jfmt.FmtSize64(uint64(total.Load()) / uint64(len(archiveSize)))
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

// missionFiles returns files for a mission dir, using checksums.b3 as the
// manifest if present (preserves sizes for progress), otherwise walks the dir.
func missionFiles(dir string) ([]fileEntry, int64, error) {
	cPath := filepath.Join(dir, "checksums.b3")
	f, err := os.Open(cPath)
	if err == nil {
		defer f.Close()
		var files []fileEntry
		var total int64
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			parts := strings.SplitN(scanner.Text(), "  ", 2)
			if len(parts) != 2 {
				continue
			}
			rel := parts[1]
			info, err := os.Stat(filepath.Join(dir, rel))
			if err != nil {
				continue
			}
			files = append(files, fileEntry{rel: rel, size: info.Size()})
			total += info.Size()
		}
		return files, total, scanner.Err()
	}
	// fallback: walk
	files, err := findFiles(dir)
	if err != nil {
		return nil, 0, err
	}
	var total int64
	for _, f := range files {
		total += f.size
	}
	return files, total, nil
}

type scannedCard struct {
	mountedCard
	files []fileEntry
}

func buildOps(scanned []scannedCard, dstRoots []string, bars map[string]*barTracker) []*op {
	var ops []*op
	for _, sc := range scanned {
		for _, f := range sc.files {
			src := filepath.Join(sc.src, f.rel)
			dstRel := filepath.Join(sc.Volume, f.rel)
			for _, dstRoot := range dstRoots {
				dst := filepath.Join(dstRoot, dstRel)
				ops = append(ops, &op{src: src, dst: dst, do: prepJob(src, dst, dstRel, dstRoot, bars[dstRoot])})
			}
		}
	}
	return ops
}

func runVerify(cfg Config, missionNum int) {
	yearStr := strconv.Itoa(time.Now().Year())

	type dirEntry struct {
		vol string
		dir string
	}
	var dirs []dirEntry
	for _, d := range cfg.Drives {
		vol := filepath.Join("/Volumes", d.Volume)
		if !dirExists(vol) {
			continue
		}
		slug, err := findMissionSlug(cfg.Drives, yearStr, missionNum)
		if err != nil {
			fmt.Printf("warning: mission %03d not found on %s\n", missionNum, d.Volume)
			continue
		}
		dirs = append(dirs, dirEntry{d.Volume, filepath.Join(vol, d.Root, yearStr, slug)})
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
		info := probeDrive("/Volumes/" + job.vol)
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

func runUpdate(cfg Config, missionNum int, year int, skipConf bool) {
	yearStr := strconv.Itoa(year)
	slug, err := findMissionSlug(cfg.Drives, yearStr, missionNum)
	if err != nil {
		exit(1, "mission %03d not found: %v", missionNum, err)
	}

	// find primary and archive drives
	var primaryDir string
	type archiveDrive struct {
		vol string
		dir string
	}
	var archives []archiveDrive
	for _, d := range cfg.Drives {
		vol := filepath.Join("/Volumes", d.Volume)
		if !dirExists(vol) {
			continue
		}
		dir := filepath.Join(vol, d.Root, yearStr, slug)
		if d.Role == "hot" {
			if !dirExists(dir) {
				exit(1, "mission %03d not found on hot drive %s", missionNum, d.Volume)
			}
			primaryDir = dir
		} else {
			if dirExists(dir) {
				archives = append(archives, archiveDrive{d.Volume, dir})
			} else {
				fmt.Printf("warning: mission not found on %s, skipping\n", d.Volume)
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
		fmt.Printf("update: %d file(s) (%s) → %s\n", len(j.missing), jfmt.FmtSize64(uint64(j.size)), j.vol)
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
	var wg sync.WaitGroup
	var trackers []*barTracker

	for _, j := range jobs {
		info := probeDrive("/Volumes/" + j.vol)
		bar := addBar(p1, j.vol, j.size)
		trackers = append(trackers, bar)
		pool := fnpool.NewPool(info.concurrency)
		for _, f := range j.missing {
			wg.Add(1)
			f, dstRoot := f, j.dir
			src := filepath.Join(primaryDir, f.rel)
			dst := filepath.Join(dstRoot, f.rel)
			pool.Dispatch(func() {
				defer wg.Done()
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
	wg.Wait()
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
	var wg2 sync.WaitGroup
	newHashes := make(map[string][]string) // dstRoot → "hash  rel" lines
	var newHashesMu sync.Mutex

	for vol, rs := range func() map[string][]struct {
		dst, rel, dstRoot, srcHash string
	} {
		m := make(map[string][]struct{ dst, rel, dstRoot, srcHash string })
		for _, r := range copyResults {
			if r.err == nil {
				m[r.vol] = append(m[r.vol], struct{ dst, rel, dstRoot, srcHash string }{r.dst, r.rel, r.dstRoot, r.srcHash})
			}
		}
		return m
	}() {
		info := probeDrive("/Volumes/" + vol)
		pool2 := fnpool.NewPool(info.concurrency)
		for _, r := range rs {
			wg2.Add(1)
			r := r
			pool2.Dispatch(func() {
				defer wg2.Done()
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
	wg2.Wait()
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

func runChecksum(cfg Config, missionNum int, year int) {
	yearStr := strconv.Itoa(year)
	slug, err := findMissionSlug(cfg.Drives, yearStr, missionNum)
	if err != nil {
		exit(1, "mission %03d not found: %v", missionNum, err)
	}

	type driveHashes struct {
		vol  string
		dir  string
		hashes map[string]string // rel → hash
	}

	// find mission on all mounted drives
	var drives []driveHashes
	for _, d := range cfg.Drives {
		vol := filepath.Join("/Volumes", d.Volume)
		if !dirExists(vol) {
			continue
		}
		dir := filepath.Join(vol, d.Root, yearStr, slug)
		if !dirExists(dir) {
			fmt.Printf("warning: mission not found on %s, skipping\n", d.Volume)
			continue
		}
		if _, err := os.Stat(filepath.Join(dir, "checksums.b3")); err == nil {
			fmt.Printf("warning: %s already has checksums.b3, skipping\n", d.Volume)
			continue
		}
		drives = append(drives, driveHashes{vol: d.Volume, dir: dir, hashes: make(map[string]string)})
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
		info := probeDrive("/Volumes/" + d.vol)
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

func findFiles(root string) ([]fileEntry, error) {
	var files []fileEntry
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d == nil || d.IsDir() {
			return nil
		}
		rel := strings.TrimPrefix(path, root+string(os.PathSeparator))
		for _, part := range strings.Split(rel, string(os.PathSeparator)) {
			if strings.HasPrefix(part, ".") {
				return nil
			}
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		files = append(files, fileEntry{rel: rel, size: info.Size()})
		return nil
	})
	return files, err
}

func prepJob(src, dst, rel, dstRoot string, bar *barTracker) func() <-chan *result {
	return func() <-chan *result {
		done := make(chan *result)
		go func() {
			r := job(src, dst, bar)
			r.dst = dst
			r.rel = rel
			r.dstRoot = dstRoot
			done <- r
			close(done)
		}()
		return done
	}
}

func job(src, dst string, bar *barTracker) *result {
	rd, err := os.Open(src)
	if err != nil {
		return &result{err: err}
	}
	defer rd.Close()

	info, err := os.Stat(src)
	if err != nil {
		return &result{err: err}
	}
	perm := info.Mode().Perm()

	if err := os.MkdirAll(filepath.Dir(dst), 0777); err != nil {
		return &result{err: err}
	}
	wr, err := os.Create(dst)
	if err != nil {
		return &result{err: err}
	}

	h := blake3.New(32, nil)
	var w io.Writer = wr
	if bar != nil {
		w = &progressWriter{w: wr, tracker: bar}
	}
	buf := make([]byte, 4*1024*1024)
	n, err := io.CopyBuffer(w, io.TeeReader(rd, h), buf)
	wr.Sync()
	wr.Close()
	if err != nil {
		os.Remove(dst)
		return &result{err: err}
	}

	if err := os.Chmod(dst, perm); err != nil {
		os.Remove(dst)
		return &result{err: err}
	}

	return &result{n: n, srcHash: hex.EncodeToString(h.Sum(nil))}
}

func hashFile(path string, bar *barTracker) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := blake3.New(32, nil)
	var r io.Reader = f
	if bar != nil {
		r = &progressReader{r: f, tracker: bar}
	}
	buf := make([]byte, 4*1024*1024)
	if _, err := io.CopyBuffer(h, r, buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func mountedCards(cfgs []CardConfig) []mountedCard {
	var out []mountedCard
	for _, c := range cfgs {
		src := filepath.Join("/Volumes", c.Volume, c.Sub)
		if dirExists(src) {
			out = append(out, mountedCard{c, src})
		}
	}
	return out
}

func missionManifestsMatch(a, b []fileEntry) bool {
	if len(a) != len(b) {
		return false
	}
	sizes := make(map[string]int64, len(a))
	for _, f := range a {
		sizes[f.rel] = f.size
	}
	for _, f := range b {
		if sizes[f.rel] != f.size {
			return false
		}
	}
	return true
}

func findMissionSlug(drives []DriveConfig, yearStr string, num int) (string, error) {
	prefix := fmt.Sprintf("%03d_", num)
	for _, d := range drives {
		yearDir := filepath.Join("/Volumes", d.Volume, d.Root, yearStr)
		entries, err := os.ReadDir(yearDir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() && strings.HasPrefix(e.Name(), prefix) {
				return e.Name(), nil
			}
		}
	}
	return "", fmt.Errorf("no mission %s found on any mounted drive", prefix)
}

func mergeChecksums(path string, newLines []string) []string {
	existing := readChecksumFile(path)
	for _, line := range newLines {
		parts := strings.SplitN(line, "  ", 2)
		if len(parts) == 2 {
			existing[parts[1]] = parts[0]
		}
	}
	merged := make([]string, 0, len(existing))
	for rel, hash := range existing {
		merged = append(merged, fmt.Sprintf("%s  %s", hash, rel))
	}
	return merged
}

func readChecksumFile(path string) map[string]string {
	out := make(map[string]string)
	f, err := os.Open(path)
	if err != nil {
		return out
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "  ", 2)
		if len(parts) == 2 {
			out[parts[1]] = parts[0]
		}
	}
	return out
}

func peekMission(year int) (int, error) {
	seq, err := readSeq()
	if err != nil {
		return 0, err
	}
	return seq[year] + 1, nil
}

func revertMission(year int) error {
	seq, err := readSeq()
	if err != nil {
		return err
	}
	if seq[year] > 0 {
		seq[year]--
	}
	return writeSeq(seq)
}

func commitMission(year, num int) error {
	seq, err := readSeq()
	if err != nil {
		return err
	}
	seq[year] = num
	return writeSeq(seq)
}

func readSeq() (map[int]int, error) {
	p, err := expandPath(seqPath)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return map[int]int{}, nil
	}
	if err != nil {
		return nil, err
	}
	var raw map[string]int
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("corrupt %s: %w", p, err)
	}
	seq := make(map[int]int, len(raw))
	for k, v := range raw {
		year, err := strconv.Atoi(k)
		if err != nil {
			return nil, fmt.Errorf("corrupt year key %q in %s", k, p)
		}
		seq[year] = v
	}
	return seq, nil
}

func writeSeq(seq map[int]int) error {
	p, err := expandPath(seqPath)
	if err != nil {
		return err
	}
	raw := make(map[string]int, len(seq))
	for k, v := range seq {
		raw[strconv.Itoa(k)] = v
	}
	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(p, data, 0644)
}

func loadConfig() Config {
	p, err := expandPath("~/.qcp")
	if err != nil {
		exit(1, "err resolving config path: %v", err)
	}
	data, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		exit(1, "config not found — create %s with your drive settings", p)
	}
	if err != nil {
		exit(1, "err reading config: %v", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		exit(1, "err parsing config: %v", err)
	}
	return cfg
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func sanitizeMission(name string) string {
	return strings.ReplaceAll(strings.TrimSpace(name), " ", "_")
}

func expandPath(p string) (string, error) {
	if strings.HasPrefix(p, "~") {
		usr, err := user.Current()
		if err != nil {
			return "", err
		}
		return filepath.Join(usr.HomeDir, p[1:]), nil
	}
	return filepath.Abs(p)
}

func confirm() bool {
	fmt.Print("enter \"y\" to confirm: ")
	var resp string
	fmt.Scan(&resp)
	return resp == "y"
}

func exit(code int, msg string, args ...any) {
	fmt.Printf(msg+"\n", args...)
	os.Exit(code)
}
