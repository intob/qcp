package main

import (
	"bufio"
	"context"
	"flag"
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

var version = "dev"

func usage() {
	w := flag.CommandLine.Output()
	fmt.Fprintf(w, "%s  %s\n\n", bold("qcp"), dim(version))

	const colWidth = 24
	section := func(name string) { fmt.Fprintf(w, "\n%s\n", bold(name)) }
	row := func(f, arg, desc string) {
		plain := f
		display := f
		if arg != "" {
			plain = f + " <" + arg + ">"
			display = f + " " + dim("<"+arg+">")
		}
		pad := strings.Repeat(" ", max(1, colWidth-len(plain)))
		fmt.Fprintf(w, "  %s%s%s\n", display, pad, dim(desc))
	}

	section("INGEST")
	row("-ingest", "name", `new ingest (e.g. "Altissimo with Anton")`)
	row("-to", "n", "append files to existing mission number")

	section("ARCHIVE")
	row("-sync", "", "sync missions from hot drives to cold drives")
	row("-update", "n", "copy files missing from cold drives for mission")
	row("-pull", "n", "pull mission from cold storage to hot drives")
	row("  -sub", "dir", "subdirectory within mission to pull")

	section("VERIFY")
	row("-verify", "n", "re-verify mission across all mounted drives")
	row("-checksum", "n", "generate checksums.b3 for a mission")
	row("-checksum-all", "", "generate checksums.b3 for all missions in year")

	section("ORGANISE")
	row("-organise", "", "group unorganised files into seasonal mission folders")
	row("-reorganise", "", "regroup already-organised missions by season")
	row("-renumber", "", "fix mission numbers to be sequential with no gaps or duplicates")
	row("-init", "", "scan drives and initialise missing sequence numbers")

	section("INFO")
	row("-list", "", "list missions across all mounted drives")
	row("-status", "", "show drive space and mission status")
	row("-check", "", "check all missions in the year for missing files across drives")

	section("MAINTENANCE")
	row("-clean", "", "find and remove junk files from all mounted drives")

	section("FLAGS")
	row("-year", "year", fmt.Sprintf("year override (default: %d)", time.Now().Year()))
	row("-y", "", "skip confirmation prompts")
	row("-version", "", "print version and exit")

	fmt.Fprintln(w)
}

func main() {
	flag.Usage = usage
	showVersion := flag.Bool("version", false, "print version and exit")
	skipConf := flag.Bool("y", false, "skip confirmation")
	missionFlag := flag.String("ingest", "", "ingest name (e.g. \"Altissimo with Anton\")")
	year := flag.Int("year", time.Now().Year(), "year override")
	toMissionStr := flag.String("to", "", "append to existing mission number")
	verifyMissionStr := flag.String("verify", "", "re-verify mission number across all mounted drives")
	checksumMissionStr := flag.String("checksum", "", "generate checksums.b3 for a mission by cross-verifying all mounted drives")
	doChecksumAll := flag.Bool("checksum-all", false, "generate checksums.b3 for every mission in the year across all mounted drives")
	updateMissionStr := flag.String("update", "", "copy files missing from cold drives for an existing mission")
	pullMissionStr := flag.String("pull", "", "pull a mission from cold storage to hot drives")
	pullSub := flag.String("sub", "", "subdirectory within mission to pull (e.g. CFEXP_250_01)")
	doSync := flag.Bool("sync", false, "sync missions from hot drives to cold drives")
	doList := flag.Bool("list", false, "list missions across all mounted drives")
	doStatus := flag.Bool("status", false, "show drive space and mission status")
	doCheck := flag.Bool("check", false, "check all missions in the year for missing files across drives")
	doClean := flag.Bool("clean", false, "find and remove junk files (Synology metadata, Thumbs.db, etc.) from all mounted drives")
	doInit := flag.Bool("init", false, "scan drives and initialise missing sequence numbers")
	doOrganise := flag.Bool("organise", false, "group unorganised files into seasonal mission folders")
	doReorganise := flag.Bool("reorganise", false, "regroup already-organised missions by season (re-runs organise over existing numbered folders)")
	doRenumber := flag.Bool("renumber", false, "fix mission numbers to be sequential with no gaps or duplicates")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	yearExplicit := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "year" {
			yearExplicit = true
		}
	})

	parseMission := func(s string) (string, bool) {
		return s, s != ""
	}

	toMission, hasTo := parseMission(*toMissionStr)
	verifyMission, hasVerify := parseMission(*verifyMissionStr)
	checksumMission, hasChecksum := parseMission(*checksumMissionStr)
	updateMission, hasUpdate := parseMission(*updateMissionStr)
	pullMission, hasPull := parseMission(*pullMissionStr)

	cfg := loadConfig()
	keepAwake()

	if *doCheck {
		if yearExplicit {
			runCheck(cfg, *year)
		} else {
			runCheckAll(cfg)
		}
		return
	}

	if *doClean {
		runClean(cfg, *skipConf, yearExplicit, *year)
		return
	}

	if *doInit {
		runInit(cfg, *year, yearExplicit)
		return
	}

	if *doOrganise {
		runOrganise(cfg, *year, *skipConf, false)
		return
	}

	if *doReorganise {
		runOrganise(cfg, *year, *skipConf, true)
		return
	}

	if *doRenumber {
		runRenumber(cfg, *year, *skipConf)
		return
	}

	if *doList {
		if yearExplicit {
			runList(cfg, *year)
		} else {
			runListAll(cfg)
		}
		return
	}

	if *doStatus {
		runStatus(cfg, *year)
		return
	}

	if hasPull {
		runPull(cfg, pullMission, *year, *pullSub, *skipConf)
		return
	}

	if *doSync {
		if yearExplicit {
			runSync(cfg, *year, *skipConf)
		} else {
			runSyncAll(cfg, *skipConf)
		}
		return
	}

	if hasUpdate {
		runUpdate(cfg, updateMission, *year, *skipConf)
		return
	}

	if hasVerify {
		runVerify(cfg, verifyMission, *year)
		return
	}

	if *doChecksumAll {
		runChecksumYear(cfg, *year)
		return
	}

	if hasChecksum {
		runChecksum(cfg, checksumMission, *year)
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

	if hasTo {
		isAppend = true
		slug, err := resolveMission(cfg.Drives, yearStr, toMission)
		if err != nil {
			exit(2, "mission %03d not found: %v", toMission, err)
		}
		missionSlug = slug
	} else {
		if *missionFlag == "" {
			exit(3, "-ingest is required")
		}
		num, err := peekMission(*year)
		if err != nil {
			exit(4, "err reading mission counter: %v", err)
		}
		missionNum = num
		missionSlug = fmt.Sprintf("%03d_%s", num, sanitizeMission(*missionFlag))
	}

	var dstRoots []string
	dstNames := make(map[string]string) // dstRoot → display name
	dstBase := make(map[string]string)  // dstRoot → base path for probeDrive
	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			fmt.Printf("%s %s %s\n", yellow("warning:"), bold(d.name()), dim("not mounted, skipping"))
			continue
		}
		dstRoot := filepath.Join(base, d.Root, yearStr, missionSlug)
		dstRoots = append(dstRoots, dstRoot)
		dstNames[dstRoot] = d.name()
		dstBase[dstRoot] = base
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
		fmt.Printf("%s → %s\n", sc.src, strings.Join(dstRoots, ", "))
		for _, f := range sc.files {
			fmt.Printf("  %s\n", f.rel)
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
		cancel()                           // stop mpb rendering via context
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

	sizeStr := fmtSize(uint64(totalSize))
	fmt.Printf("\n%s %d files (%s) to %d drive(s)\n\n", dim("copying..."), totalFiles, sizeStr, len(dstRoots))

	// probe destination drives once
	dstInfos := make(map[string]driveInfo)
	for _, dstRoot := range dstRoots {
		dstInfos[dstRoot] = probeDrive(dstBase[dstRoot])
	}

	// Phase 1: copy — per-drive pool, skipping files that already exist
	// First pass: stat-check per destination to get accurate bar totals.
	type fileJob struct {
		src, dst, rel, dstRoot string
		size                   int64
	}
	missingByDst := make(map[string][]fileJob)
	for _, sc := range scanned {
		for _, f := range sc.files {
			dstRel := filepath.Join(sc.Volume, f.rel)
			src := filepath.Join(sc.src, f.rel)
			for _, dstRoot := range dstRoots {
				dst := filepath.Join(dstRoot, dstRel)
				if _, err := os.Stat(dst); err != nil {
					missingByDst[dstRoot] = append(missingByDst[dstRoot],
						fileJob{src, dst, dstRel, dstRoot, f.size})
				}
			}
		}
	}

	p1 := mpb.NewWithContext(ctx, mpb.WithWidth(64))
	copyBars := make(map[string]*barTracker)
	for _, dstRoot := range dstRoots {
		var size int64
		for _, fj := range missingByDst[dstRoot] {
			size += fj.size
		}
		copyBars[dstRoot] = addBar(p1, dstNames[dstRoot], size)
	}

	var results []*result
	var resultsMu sync.Mutex
	var total atomic.Int64
	var copyPools []*pool
	for _, dstRoot := range dstRoots {
		missing := missingByDst[dstRoot]
		if len(missing) == 0 {
			fmt.Printf("%s: %s\n", bold(dstNames[dstRoot]), dim("already up to date"))
			continue
		}
		wp := newPool(dstInfos[dstRoot].concurrency)
		copyPools = append(copyPools, wp)
		for _, fj := range missing {
			fj := fj
			o := prepJob(fj.src, fj.dst, fj.rel, fj.dstRoot, copyBars[fj.dstRoot])
			wp.run(func() {
				if ctx.Err() != nil {
					return
				}
				r := <-o()
				resultsMu.Lock()
				results = append(results, r)
				resultsMu.Unlock()
				if r.err != nil {
					fmt.Printf("\n%s copy: %v\n", red("ERROR"), r.err)
					return
				}
				total.Add(r.n)
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

	var copyFailed int
	for _, r := range results {
		if r != nil && r.err != nil {
			copyFailed++
		}
	}
	if copyFailed > 0 {
		exit(10, "%d file(s) failed to copy", copyFailed)
	}

	// Phase 2: verify — per-drive pool
	fmt.Printf("\n%s\n\n", dim("verifying..."))
	p2 := mpb.NewWithContext(ctx, mpb.WithWidth(64))
	verifyBars := make(map[string]*barTracker)
	for _, dstRoot := range dstRoots {
		verifyBars[dstRoot] = addBar(p2, dstNames[dstRoot], totalSize)
	}

	var mu sync.Mutex
	newChecksums := make(map[string][]string)
	var verifyFailed atomic.Int64
	resultsByDst := make(map[string][]*result)
	for _, r := range results {
		if r != nil && r.err == nil {
			resultsByDst[r.dstRoot] = append(resultsByDst[r.dstRoot], r)
		}
	}
	var verifyPools []*pool
	for dstRoot, rs := range resultsByDst {
		wp := newPool(dstInfos[dstRoot].concurrency)
		verifyPools = append(verifyPools, wp)
		for _, r := range rs {
			r := r
			wp.run(func() {
				if ctx.Err() != nil {
					return
				}
				got, err := hashFile(r.dst, verifyBars[r.dstRoot])
				if err != nil {
					fmt.Printf("\n%s verify: %v\n", red("ERROR"), err)
					verifyFailed.Add(1)
					return
				}
				if got != r.srcHash {
					fmt.Printf("\n%s %s\n", red("MISMATCH:"), r.dst)
					verifyFailed.Add(1)
					return
				}
				mu.Lock()
				newChecksums[r.dstRoot] = append(newChecksums[r.dstRoot],
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
		exit(11, "%d file(s) failed verification", verifyFailed.Load())
	}

	for dstRoot, lines := range newChecksums {
		cPath := filepath.Join(dstRoot, "checksums.b3")
		lines = mergeChecksums(cPath, lines)
		sort.Strings(lines)
		if err := os.WriteFile(cPath, []byte(strings.Join(lines, "\n")+"\n"), 0644); err != nil {
			fmt.Printf("%s writing checksums: %v\n", red("ERROR"), err)
		}
	}

	perDrive := fmtSize(uint64(total.Load()) / uint64(len(dstRoots)))
	fmt.Printf("\n%s %s copied and verified → %s\n", green("✓"), perDrive, bold(missionSlug))
}
