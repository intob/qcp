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
	row("-ingest", "name", `create new mission (e.g. "Altissimo with Anton")`)
	row("-ingest", "n", "append cards to existing mission number")

	section("ARCHIVE")
	row("-sync", "", "sync missions from hot drives to cold drives")
	row("-replicate", "", "replicate missions between cold drives")
	row("-pull", "n", "pull mission from cold storage to hot drives")
	row("  -sub", "dir", "subdirectory within mission to pull")

	section("VERIFY")
	row("-verify", "n|all", "re-verify mission(s) across all mounted drives")
	row("-checksum", "n|all", "generate checksums.b3 for a mission (or all in year)")

	section("ORGANISE")
	row("-organise", "", "group unorganised files into seasonal mission folders")
	row("-reorganise", "", "regroup already-organised missions by season")
	row("-renumber", "", "fix mission numbers to be sequential with no gaps or duplicates")
	row("-init", "", "scan drives and initialise missing sequence numbers")

	section("INFO")
	row("-list", "", "list missions across all mounted drives")
	row("-status", "", "show drive space and mission status")
	row("-check", "n|all", "check mission(s) for missing files across drives")

	section("MAINTENANCE")
	row("-clean", "", "find and remove junk files from all mounted drives")
	row("-eject", "", "eject all mounted cards and drives")

	section("FLAGS")
	row("-year", "year|all", fmt.Sprintf("year to operate on (default: %d)", time.Now().Year()))
	row("-y", "", "skip confirmation prompts")
	row("-version", "", "print version and exit")

	fmt.Fprintln(w)
}

func main() {
	flag.Usage = usage
	showVersion := flag.Bool("version", false, "print version and exit")
	skipConf := flag.Bool("y", false, "skip confirmation")
	missionFlag := flag.String("ingest", "", "mission name or number")
	yearFlag := flag.String("year", "", `year to operate on (default: current year, "all" for all years)`)
	verifyMissionStr := flag.String("verify", "", `re-verify mission(s) across all mounted drives (use "all" for all missions)`)
	checksumMissionStr := flag.String("checksum", "", `generate checksums.b3 for a mission (use "all" for all missions in year)`)
	pullMissionStr := flag.String("pull", "", "pull a mission from cold storage to hot drives")
	pullSub := flag.String("sub", "", "subdirectory within mission to pull (e.g. CFEXP_250_01)")
	doSync := flag.Bool("sync", false, "sync missions from hot drives to cold drives")
	doReplicate := flag.Bool("replicate", false, "replicate missions between cold drives")
	doList := flag.Bool("list", false, "list missions across all mounted drives")
	doStatus := flag.Bool("status", false, "show drive space and mission status")
	checkMissionStr := flag.String("check", "", `check mission(s) for missing files across drives (use "all" for all missions)`)
	doClean := flag.Bool("clean", false, "find and remove junk files (Synology metadata, Thumbs.db, etc.) from all mounted drives")
	doInit := flag.Bool("init", false, "scan drives and initialise missing sequence numbers")
	doOrganise := flag.Bool("organise", false, "group unorganised files into seasonal mission folders")
	doReorganise := flag.Bool("reorganise", false, "regroup already-organised missions by season (re-runs organise over existing numbered folders)")
	doRenumber := flag.Bool("renumber", false, "fix mission numbers to be sequential with no gaps or duplicates")
	doEject := flag.Bool("eject", false, "eject all mounted cards and drives")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}

	year := time.Now().Year()
	yearAll := false
	if yf := *yearFlag; yf != "" {
		if yf == "all" {
			yearAll = true
		} else {
			y, err := strconv.Atoi(yf)
			if err != nil || y < 2000 || y > 2100 {
				exit(1, "invalid year %q — use a year number or \"all\"", yf)
			}
			year = y
		}
	}

	parseMission := func(s string) (int, bool) {
		if s == "" {
			return 0, false
		}
		n, err := strconv.Atoi(s)
		if err != nil || n <= 0 {
			exit(1, "invalid mission number: %s", s)
		}
		return n, true
	}

	pullMission, hasPull := parseMission(*pullMissionStr)

	cfg := loadConfig()
	keepAwake()

	switch {
	case *checkMissionStr == "all":
		if yearAll {
			if !runCheckAll(cfg) {
				os.Exit(1)
			}
		} else {
			if !runCheck(cfg, year) {
				os.Exit(1)
			}
		}
		return
	case *checkMissionStr != "":
		n, _ := parseMission(*checkMissionStr)
		if !runCheckMission(cfg, n, year, !yearAll) {
			os.Exit(1)
		}
		return
	}

	if *doEject {
		runEject(cfg)
		return
	}

	if *doClean {
		runClean(cfg, *skipConf, !yearAll, year)
		return
	}

	if *doInit {
		runInit(cfg, year, !yearAll)
		return
	}

	if *doOrganise {
		runOrganise(cfg, year, *skipConf, false)
		return
	}

	if *doReorganise {
		runOrganise(cfg, year, *skipConf, true)
		return
	}

	if *doRenumber {
		runRenumber(cfg, year, *skipConf)
		return
	}

	if *doList {
		if yearAll {
			runListAll(cfg)
		} else {
			runList(cfg, year)
		}
		return
	}

	if *doStatus {
		runStatus(cfg, year)
		return
	}

	if hasPull {
		runPull(cfg, pullMission, year, *pullSub, *skipConf)
		return
	}

	if *doSync {
		if yearAll {
			if !runSyncAll(cfg, *skipConf) {
				os.Exit(1)
			}
		} else {
			if !runSync(cfg, year, *skipConf) {
				os.Exit(1)
			}
		}
		return
	}

	if *doReplicate {
		if yearAll {
			if !runReplicateAll(cfg, *skipConf) {
				os.Exit(1)
			}
		} else {
			if !runReplicate(cfg, year, *skipConf) {
				os.Exit(1)
			}
		}
		return
	}

	switch {
	case *verifyMissionStr == "all":
		var ok bool
		if yearAll {
			ok = runVerifyAll(cfg)
		} else {
			ok = runVerifyYear(cfg, year)
		}
		if !ok {
			os.Exit(1)
		}
		return
	case *verifyMissionStr != "":
		n, _ := parseMission(*verifyMissionStr)
		runVerify(cfg, n, year)
		return
	}

	switch {
	case *checksumMissionStr == "all":
		var ok bool
		if yearAll {
			ok = runChecksumAll(cfg)
		} else {
			ok = runChecksumYear(cfg, year)
		}
		if !ok {
			os.Exit(1)
		}
		return
	case *checksumMissionStr != "":
		n, _ := parseMission(*checksumMissionStr)
		runChecksum(cfg, n, year)
		return
	}

	cards := mountedCards(cfg.Cards)
	if len(cards) == 0 {
		exit(1, "no configured cards mounted")
	}

	yearStr := strconv.Itoa(year)

	// Scan cards first — needed before mission resolution so we can detect multi-day footage.
	var scanned []scannedCard
	var totalFiles int
	for _, card := range cards {
		files, err := findFiles(card.src)
		if err != nil {
			exit(6, "err scanning %s: %v", card.Volume, err)
		}
		scanned = append(scanned, scannedCard{card, files})
		totalFiles += len(files)
	}
	if totalFiles == 0 {
		exit(7, "no files found on mounted cards")
	}

	// Group files by recording date.
	days := groupAllByDate(scanned)

	// Build the list of mounted destination drives (slug-independent).
	type dstDrv struct {
		cfg  DriveConfig
		base string
	}
	var dstDrvs []dstDrv
	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			fmt.Printf("%s %s %s\n", yellow("warning:"), bold(d.name()), dim("not mounted, skipping"))
			continue
		}
		dstDrvs = append(dstDrvs, dstDrv{d, base})
	}
	if len(dstDrvs) == 0 {
		exit(5, "no destination drives mounted")
	}

	buildDst := func(slug string) (roots []string, names, bases map[string]string) {
		names = make(map[string]string)
		bases = make(map[string]string)
		for _, d := range dstDrvs {
			r := filepath.Join(d.base, d.cfg.Root, yearStr, slug)
			roots = append(roots, r)
			names[r] = d.cfg.name()
			bases[r] = d.base
		}
		return
	}

	// Shared interrupt state — updated before each day's copy begins.
	var (
		intrDstRoots []string
		intrIsNew    bool
	)

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		signal.Stop(sigCh)
		cancel()
		time.Sleep(150 * time.Millisecond)
		reader := bufio.NewReader(os.Stdin)
		var resp string
		for resp != "y" && resp != "n" {
			fmt.Print("\r\033[2K\ninterrupted — delete partial mission and revert counter? (y/n): ")
			line, err := reader.ReadString('\n')
			resp = strings.TrimSpace(line)
			if err != nil {
				break
			}
		}
		if resp == "y" {
			for _, d := range intrDstRoots {
				os.RemoveAll(d)
				fmt.Printf("removed: %s\n", d)
			}
			if intrIsNew {
				if err := revertMission(year); err != nil {
					fmt.Printf("err reverting counter: %v\n", err)
				} else {
					fmt.Println("mission counter reverted")
				}
			}
		}
		os.Exit(130)
	}()

	runDay := func(dayScanned []scannedCard, missionSlug string, dstRoots []string, dstNames, dstBase map[string]string) {
		type fileJob struct {
			src, dst, rel, dstRoot string
			size                   int64
		}

		var dayTotal int64
		var dayFiles int
		for _, sc := range dayScanned {
			for _, f := range sc.files {
				dayTotal += f.size
				dayFiles++
			}
		}

		dstInfos := make(map[string]driveInfo)
		for _, dstRoot := range dstRoots {
			dstInfos[dstRoot] = probeDrive(dstBase[dstRoot])
		}

		missingByDst := make(map[string][]fileJob)
		for _, sc := range dayScanned {
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

		fmt.Printf("\n%s %d files (%s) to %d drive(s)\n\n", dim("copying..."), dayFiles, fmtSize(uint64(dayTotal)), len(dstRoots))

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

		fmt.Printf("\n%s\n\n", dim("verifying..."))

		var mu sync.Mutex
		newChecksums := make(map[string][]string)
		var verifyFailed atomic.Int64
		resultsByDst := make(map[string][]*result)
		for _, r := range results {
			if r != nil && r.err == nil {
				resultsByDst[r.dstRoot] = append(resultsByDst[r.dstRoot], r)
			}
		}

		p2 := mpb.NewWithContext(ctx, mpb.WithWidth(64))
		verifyBars := make(map[string]*barTracker)
		for dstRoot, rs := range resultsByDst {
			var size int64
			for _, r := range rs {
				size += r.n
			}
			verifyBars[dstRoot] = addBar(p2, dstNames[dstRoot], size)
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

		copied := fmtSize(uint64(total.Load()) / uint64(len(dstRoots)))
		fmt.Printf("\n%s %s copied and verified → %s\n", green("✓"), copied, bold(missionSlug))
	}

	if len(days) == 1 {
		// ── Single-day path: existing behaviour ──────────────────────────────
		if *missionFlag == "" {
			exit(3, "-ingest is required")
		}

		var missionSlug string
		var isAppend bool
		var missionNum int

		if n, err := strconv.Atoi(*missionFlag); err == nil && n > 0 {
			isAppend = true
			slug, err := findMissionSlug(cfg.Drives, yearStr, n)
			if err != nil {
				exit(2, "mission %03d not found: %v", n, err)
			}
			missionSlug = slug
		} else {
			num, err := peekMission(year)
			if err != nil {
				exit(4, "err reading mission counter: %v", err)
			}
			missionNum = num
			missionSlug = fmt.Sprintf("%03d_%s", num, sanitizeMission(*missionFlag))
		}

		dstRoots, dstNames, dstBase := buildDst(missionSlug)

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
			if err := commitMission(year, missionNum); err != nil {
				exit(9, "err updating mission counter: %v", err)
			}
		}

		intrDstRoots = dstRoots
		intrIsNew = !isAppend

		runDay(scanned, missionSlug, dstRoots, dstNames, dstBase)

	} else {
		// ── Multi-day path: prompt for each day ──────────────────────────────
		fmt.Printf("Cards have footage from %s days:\n\n", bold(strconv.Itoa(len(days))))
		for _, d := range days {
			fmt.Printf("  %s  %d files  %s\n", bold(d.date), d.fileCount, dim(fmtSize(uint64(d.totalSize))))
		}

		suggestion := *missionFlag // use -ingest value as hint for first day if provided
		fmt.Printf("\nEnter a name (new mission) or number (append to existing) for each day:\n\n")

		// Pre-compute the next available mission number so each new mission
		// in the same run gets a unique sequential number.
		nextNum, err := peekMission(year)
		if err != nil {
			exit(4, "err reading mission counter: %v", err)
		}

		type dayPlan struct {
			day      dayGroup
			slug     string
			isNew    bool
			num      int
			dstRoots []string
			dstNames map[string]string
			dstBase  map[string]string
		}
		var plan []dayPlan

		for i, d := range days {
			hint := ""
			if i == 0 {
				hint = suggestion
			}
			slug, isNew, num, err := promptMissionForDay(cfg, year, nextNum, d.date, hint)
			if err != nil {
				exit(4, "err reading mission counter: %v", err)
			}
			if isNew {
				nextNum++
			}
			dstRoots, dstNames, dstBase := buildDst(slug)
			plan = append(plan, dayPlan{d, slug, isNew, num, dstRoots, dstNames, dstBase})
		}

		fmt.Printf("\n%s\n", bold("Plan:"))
		for _, p := range plan {
			tag := dim(" (new)")
			if !p.isNew {
				tag = dim(" (append)")
			}
			fmt.Printf("  %s → %s%s\n", p.day.date, bold(p.slug), tag)
			for _, r := range p.dstRoots {
				fmt.Printf("             %s\n", r)
			}
		}
		fmt.Println()

		if !*skipConf && !confirm() {
			exit(8, "aborted by user")
		}

		// Commit new mission numbers in ascending order so the counter stays consistent.
		for _, p := range plan {
			if p.isNew {
				if err := commitMission(year, p.num); err != nil {
					exit(9, "err updating mission counter: %v", err)
				}
			}
		}

		for _, p := range plan {
			fmt.Printf("\n%s %s\n", dim("ingesting"), bold(p.slug))
			intrDstRoots = p.dstRoots
			intrIsNew = p.isNew
			runDay(p.day.cards, p.slug, p.dstRoots, p.dstNames, p.dstBase)
		}
	}
}
