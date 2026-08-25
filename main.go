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
	row("-ingest", "", "ingest cards, prompting for mission name")
	row("-ingest", "name", `create new mission (e.g. "Altissimo with Anton")`)
	row("-ingest", "n", "append cards to existing mission number")
	row("  -proxy=false", "", "skip browse-tier proxy generation after ingest")

	section("ARCHIVE")
	row("-sync", "", "sync missions from hot drives to cold drives")
	row("-replicate", "", "replicate missions between cold drives")
	row("-pull", "n|list", "pull mission(s) from cold storage to hot drives")
	row("-copy", "n|list", "copy mission(s) between hot drives")
	row("  -to", "drives", "destination drives (default: all other hot drives)")
	row("  -sub", "dir", "subdirectory within mission to pull or copy")
	row("-evict", "n|list", "delete mission(s) from hot drives, keeping the cold copy")
	row("  -from", "drives", "hot drives to evict from (default: all)")
	row("  -copies", "n", "cold copies required before deleting (default: 1)")
	row("  -quick", "", "trust cold checksums.b3 instead of re-reading the files")

	section("VERIFY")
	row("-verify", "n|list|all", "re-verify mission(s) across all mounted drives")
	row("-checksum", "n|list|all", "generate checksums.b3 for mission(s) (or all in year)")

	section("ORGANISE")
	row("-organise", "", "group unorganised files into seasonal mission folders")
	row("-reorganise", "", "regroup already-organised missions by season")
	row("-renumber", "", "fix mission numbers to be sequential with no gaps or duplicates")
	row("-init", "", "scan drives and initialise missing sequence numbers")

	section("INFO")
	row("-list", "", "list missions across all mounted drives")
	row("-status", "", "show drive space and mission status")
	row("-check", "n|list|all", "check mission(s) for missing files across drives")

	section("PROXY")
	row("-proxy", "n|list|all", "generate proxies for mission(s), or all in the year")
	row("  -tier", "tier", "browse (default), edit, or both")
	row("  -to", "drive", "drive the proxy tree lands on (default: first hot drive)")
	row("-index", "", "build a static browsable index from the proxy manifests")
	row("  -to", "dir", "output directory (default: ~/qcp-index)")
	row("-serve", "", "serve a built index over HTTP, with the proxies playable")
	row("  -addr", "addr", "address to listen on (default: localhost:8080)")
	row("  -to", "dir", "index directory to serve (default: ~/qcp-index)")
	row("-resolve", "", "push clips flagged in the index into the open Resolve project")
	row("  -unflag", "", "also clear qcp's flag from clips no longer flagged")

	section("MAINTENANCE")
	row("-clean", "", "find and remove junk files from all mounted drives")
	row("-eject", "", "eject all mounted cards and drives")

	section("FLAGS")
	row("-year", "year|all", fmt.Sprintf("year to operate on (default: %d)", time.Now().Year()))
	row("-y", "", "skip confirmation prompts")
	row("-version", "", "print version and exit")

	fmt.Fprintln(w)
}

func btoi(b bool) int {
	if b {
		return 1
	}
	return 0
}

// maxMissionRange caps how many missions a single a-b range may expand to, so
// a typo like "1-99999" fails loudly instead of scanning every drive.
const maxMissionRange = 500

// parseMissionList parses a mission selection into a sorted, deduplicated list
// of mission numbers. Accepts a single number ("42"), a comma-separated list
// ("42,44"), an inclusive range ("42-48"), or any combination ("42-44,48").
func parseMissionList(s string) ([]int, bool) {
	if s == "" {
		return nil, false
	}
	parseNum := func(v string) int {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil || n <= 0 {
			exit(1, "invalid mission number: %s", v)
		}
		return n
	}
	seen := make(map[int]bool)
	var nums []int
	add := func(n int) {
		if !seen[n] {
			seen[n] = true
			nums = append(nums, n)
		}
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if i := strings.Index(part, "-"); i > 0 {
			if strings.TrimSpace(part[i+1:]) == "" {
				exit(1, "invalid mission range: %s", part)
			}
			lo, hi := parseNum(part[:i]), parseNum(part[i+1:])
			if hi < lo {
				exit(1, "invalid mission range: %s", part)
			}
			if hi-lo+1 > maxMissionRange {
				exit(1, "mission range %s covers more than %d missions", part, maxMissionRange)
			}
			for n := lo; n <= hi; n++ {
				add(n)
			}
			continue
		}
		add(parseNum(part))
	}
	if len(nums) == 0 {
		exit(1, "invalid mission number: %s", s)
	}
	sort.Ints(nums)
	return nums, true
}

// interruptTarget is the mission the SIGINT handler would offer to delete: the
// destination roots written so far, and whether the mission number was minted
// for it and so has to be given back. The main goroutine sets it before each
// day's copy and clears it once that footage is copied and verified; the
// handler runs on its own goroutine, so both sides go through the mutex.
type interruptTarget struct {
	mu       sync.Mutex
	dstRoots []string
	isNew    bool
}

func (t *interruptTarget) set(dstRoots []string, isNew bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.dstRoots, t.isNew = dstRoots, isNew
}

// clear marks that there is nothing an interrupt should offer to delete.
func (t *interruptTarget) clear() { t.set(nil, false) }

// get returns the pair as one snapshot, so the handler cannot act on the roots
// of one mission with the isNew of another.
func (t *interruptTarget) get() (dstRoots []string, isNew bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.dstRoots, t.isNew
}

func main() {
	flag.Usage = usage
	showVersion := flag.Bool("version", false, "print version and exit")
	skipConf := flag.Bool("y", false, "skip confirmation")
	missionFlag := flag.String("ingest", "", "mission name or number")
	yearFlag := flag.String("year", "", `year to operate on (default: current year, "all" for all years)`)
	verifyMissionStr := flag.String("verify", "", `re-verify mission(s) across all mounted drives (e.g. "42", "42,44", "42-48", "all")`)
	checksumMissionStr := flag.String("checksum", "", `generate checksums.b3 for mission(s) (e.g. "42", "42,44", "42-48", "all")`)
	pullMissionStr := flag.String("pull", "", `pull mission(s) from cold storage to hot drives (e.g. "42", "42,44", "42-48")`)
	copyMissionStr := flag.String("copy", "", `copy mission(s) from one hot drive to the others (e.g. "42", "42,44", "42-48")`)
	copyTo := flag.String("to", "", `destination drives for -copy, comma-separated (default: all other hot drives)`)
	evictMissionStr := flag.String("evict", "", `delete mission(s) from hot drives once the cold copy is proven good (e.g. "42", "42-48")`)
	evictFrom := flag.String("from", "", "hot drives to evict from, comma-separated (default: all)")
	evictCopies := flag.Int("copies", 1, "cold copies that must verify before -evict deletes anything")
	evictQuick := flag.Bool("quick", false, "-evict: trust the cold checksums.b3 instead of re-reading every file")
	pullSub := flag.String("sub", "", "subdirectory within mission to pull or copy (e.g. CFEXP_250_01)")
	doSync := flag.Bool("sync", false, "sync missions from hot drives to cold drives")
	doReplicate := flag.Bool("replicate", false, "replicate missions between cold drives")
	doList := flag.Bool("list", false, "list missions across all mounted drives")
	doStatus := flag.Bool("status", false, "show drive space and mission status")
	checkMissionStr := flag.String("check", "", `check mission(s) for missing files across drives (e.g. "42", "42,44", "42-48", "all")`)
	doClean := flag.Bool("clean", false, "find and remove junk files (Synology metadata, Thumbs.db, etc.) from all mounted drives")
	doInit := flag.Bool("init", false, "scan drives and initialise missing sequence numbers")
	doOrganise := flag.Bool("organise", false, "group unorganised files into seasonal mission folders")
	doReorganise := flag.Bool("reorganise", false, "regroup already-organised missions by season (re-runs organise over existing numbered folders)")
	doRenumber := flag.Bool("renumber", false, "fix mission numbers to be sequential with no gaps or duplicates")
	doEject := flag.Bool("eject", false, "eject all mounted cards and drives")
	proxyFlag := flag.String("proxy", "", `generate proxies for mission(s) (e.g. "42", "42-48", "all"); -proxy=false skips proxy generation during -ingest`)
	tierFlag := flag.String("tier", "", "proxy tier to generate: browse (default), edit, or both")
	doIndex := flag.Bool("index", false, "build a static browsable index from the proxy manifests")
	doServe := flag.Bool("serve", false, "serve a built index over HTTP, with the browse proxies playable in the browser")
	serveAddr := flag.String("addr", "localhost:8080", "address for -serve to listen on (use :8080 to reach it from other devices)")
	doResolve := flag.Bool("resolve", false, "push flagged clips into the open DaVinci Resolve project")
	resolveClear := flag.Bool("unflag", false, "-resolve: also clear qcp's flag from clips no longer flagged")
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

	pullMissions, hasPull := parseMissionList(*pullMissionStr)
	copyMissions, hasCopy := parseMissionList(*copyMissionStr)
	evictMissions, hasEvict := parseMissionList(*evictMissionStr)
	if n := btoi(hasPull) + btoi(hasCopy) + btoi(hasEvict); n > 1 {
		exit(1, "-pull, -copy and -evict cannot be combined")
	}
	if *evictFrom != "" && !hasEvict {
		exit(1, "-from only applies to -evict")
	}
	if (*evictQuick || *evictCopies != 1) && !hasEvict {
		exit(1, "-copies and -quick only apply to -evict")
	}
	if *evictCopies < 1 {
		exit(1, "-copies must be at least 1")
	}
	var evictDrives []string
	for _, name := range strings.Split(*evictFrom, ",") {
		if name = strings.TrimSpace(name); name != "" {
			evictDrives = append(evictDrives, name)
		}
	}
	if n := len(pullMissions) + len(copyMissions); *pullSub != "" && n > 1 {
		exit(1, "-sub applies to a single mission, but %d were given", n)
	}
	proxyAll := *proxyFlag == "all"
	proxyOff := isDisabled(*proxyFlag)
	var proxyMissions []int
	hasProxy := proxyAll
	if !proxyAll && !proxyOff {
		proxyMissions, hasProxy = parseMissionList(*proxyFlag)
	}
	if *copyTo != "" && !hasCopy && !hasProxy && !*doIndex && !*doServe {
		exit(1, "-to only applies to -copy, -proxy, -index and -serve")
	}
	if *tierFlag != "" && !hasProxy {
		exit(1, "-tier only applies to -proxy")
	}
	tiers, err := parseTiers(*tierFlag)
	if err != nil {
		exit(1, "%v", err)
	}
	var copyDrives []string
	for _, name := range strings.Split(*copyTo, ",") {
		if name = strings.TrimSpace(name); name != "" {
			copyDrives = append(copyDrives, name)
		}
	}

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
		nums, _ := parseMissionList(*checkMissionStr)
		ok := true
		for i, n := range nums {
			if i > 0 {
				fmt.Println()
			}
			if !runCheckMission(cfg, n, year, !yearAll) {
				ok = false
			}
		}
		if !ok {
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
		runPull(cfg, pullMissions, year, *pullSub, *skipConf)
		return
	}

	if hasCopy {
		runCopy(cfg, copyMissions, year, *pullSub, copyDrives, *skipConf)
		return
	}

	if hasEvict {
		runEvict(cfg, evictMissions, year, evictDrives, *evictCopies, *evictQuick, *skipConf)
		return
	}

	if hasProxy {
		if yearAll && !proxyAll {
			exit(1, `-year all only applies to -proxy all`)
		}
		years := []int{year}
		if yearAll {
			if years = allYears(cfg); len(years) == 0 {
				exit(1, "no missions found")
			}
		}
		ok := true
		for i, y := range years {
			if len(years) > 1 {
				if i > 0 {
					fmt.Println()
				}
				fmt.Printf("%s\n", bold(strconv.Itoa(y)))
			}
			if !runProxy(cfg, proxyMissions, y, proxyAll, tiers, *copyTo, *skipConf) {
				ok = false
			}
		}
		if !ok {
			os.Exit(1)
		}
		return
	}

	if *doIndex {
		if !runIndex(cfg, *copyTo) {
			os.Exit(1)
		}
		return
	}

	if *doServe {
		if !runServe(cfg, *copyTo, *serveAddr) {
			os.Exit(1)
		}
		return
	}

	if *doResolve {
		if !runResolve(cfg, *resolveClear) {
			os.Exit(1)
		}
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
		nums, _ := parseMissionList(*verifyMissionStr)
		ok := true
		for i, n := range nums {
			if i > 0 {
				fmt.Println()
			}
			if !runVerify(cfg, n, year) {
				ok = false
			}
		}
		if !ok {
			os.Exit(1)
		}
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
		nums, _ := parseMissionList(*checksumMissionStr)
		ok := true
		for i, n := range nums {
			if i > 0 {
				fmt.Println()
			}
			if !runChecksum(cfg, n, year) {
				ok = false
			}
		}
		if !ok {
			os.Exit(1)
		}
		return
	}

	cards := mountedCards(cfg)
	if len(cards) == 0 {
		exit(1, "no cards mounted")
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

	// Warn if card files are already present in an existing mission.
	if dups := checkDuplicateIngest(cfg.Drives, yearStr, scanned); len(dups) > 0 {
		fmt.Printf("\n  %s\n\n", yellow("⚠  These cards may already be ingested:"))
		for slug, n := range dups {
			fmt.Printf("     %s  %s\n", bold(slug), dim(fmt.Sprintf("%d file(s) matched", n)))
		}
		fmt.Println()
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
	var intr interruptTarget

	ctx, cancel := context.WithCancel(context.Background())
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	go func() {
		<-sigCh
		signal.Stop(sigCh)
		cancel()
		dstRoots, isNew := intr.get()
		if len(dstRoots) == 0 {
			fmt.Println()
			os.Exit(130)
		}
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
			for _, d := range dstRoots {
				os.RemoveAll(d)
				fmt.Printf("removed: %s\n", d)
			}
			if isNew {
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
			srcVol                 string // card the file is read from
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

		// Each destination reads the card separately, so with the copies now
		// running concurrently the card would take one reader per destination
		// worker without this.
		srcLimit := newSourceLimiter()
		for _, sc := range dayScanned {
			srcLimit.add(sc.Volume, filepath.Join("/Volumes", sc.Volume))
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
							fileJob{src, dst, dstRel, dstRoot, sc.Volume, f.size})
					}
				}
			}
		}

		// Anything under a partMarker name is from a run that was killed outright:
		// job removes its own temporary when a copy fails.
		if n := sweepCopyParts(dstRoots); n > 0 {
			fmt.Printf("  %s cleared %d unfinished file(s) from an interrupted run\n", dim("·"), n)
		}

		fmt.Printf("\n  %s  %s  %s\n\n", blue("↓"), bold("Copying"), dim(fmt.Sprintf("%d files  %s  →  %d drive(s)", dayFiles, fmtSize(uint64(dayTotal)), len(dstRoots))))

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
		var copySubmit []func()
		for _, dstRoot := range dstRoots {
			missing := missingByDst[dstRoot]
			if len(missing) == 0 {
				fmt.Printf("  %s  %s  %s\n", dim("─"), dim(dstNames[dstRoot]), dim("already up to date"))
				continue
			}
			wp := newPool(dstInfos[dstRoot].concurrency)
			copyPools = append(copyPools, wp)
			copySubmit = append(copySubmit, func() {
				for _, fj := range missing {
					fj := fj
					o := prepJob(fj.src, fj.dst, fj.rel, fj.dstRoot, copyBars[fj.dstRoot])
					wp.run(func() {
						if ctx.Err() != nil {
							return
						}
						release := srcLimit.acquire(fj.srcVol)
						defer release()
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

		var copyFailed int
		for _, r := range results {
			if r != nil && r.err != nil {
				copyFailed++
			}
		}
		if copyFailed > 0 {
			exit(10, "%d file(s) failed to copy", copyFailed)
		}

		fmt.Printf("\n  %s  %s\n\n", magenta("◇"), bold("Verifying"))

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
		var verifySubmit []func()
		for dstRoot, rs := range resultsByDst {
			wp := newPool(dstInfos[dstRoot].concurrency)
			verifyPools = append(verifyPools, wp)
			verifySubmit = append(verifySubmit, func() {
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
		fmt.Printf("\n  %s  %s  %s  %s\n", green("✓"), bold("Done"), dim(copied+" copied and verified  →"), bold(missionSlug))

		// The footage is copied and verified at this point, so an interrupt
		// from here on must not offer to delete the mission.
		intr.clear()

		// Generate the browse tier now, while the cards are still mounted.
		// Proxies are derived and cost nothing to regenerate, so they are not
		// worth guarding either.
		if !proxyOff {
			runIngestProxies(cfg, year, missionSlug)
		}
	}

	if len(days) == 1 {
		// ── Single-day path ───────────────────────────────────────────────────
		nextNum, err := peekMission(year)
		if err != nil {
			exit(4, "err reading mission counter: %v", err)
		}

		var missionSlug string
		var isAppend bool
		var missionNum int

		if *missionFlag != "" {
			// Value provided on CLI — parse directly without prompting.
			if n, err := strconv.Atoi(*missionFlag); err == nil && n > 0 {
				isAppend = true
				slug, err := findMissionSlug(cfg.Drives, yearStr, n)
				if err != nil {
					exit(2, "mission %03d not found: %v", n, err)
				}
				missionSlug = slug
			} else {
				missionNum = nextNum
				missionSlug = fmt.Sprintf("%03d_%s", nextNum, sanitizeMission(*missionFlag))
			}
		} else {
			d := days[0]
			fmt.Printf("  %s  %s  %s  %s  %s  %s  %s\n\n",
				cyan("◆"),
				bold(fmt.Sprintf("%d card(s)", len(scanned))),
				dim("·"), dim(fmt.Sprintf("%d files", d.fileCount)),
				dim("·"), dim(fmtSize(uint64(d.totalSize))),
				dim("·  "+d.date))
			fmt.Printf("  Name or number  %s:\n\n", dim("(- to skip)"))
			slug, isNew, num, skipped, err := promptMissionForDay(cfg, year, nextNum, days[0].date, "")
			if err != nil {
				exit(4, "err prompting for mission: %v", err)
			}
			if skipped {
				fmt.Printf("\n  %s\n", dim("skipped"))
				return
			}
			missionSlug = slug
			isAppend = !isNew
			missionNum = num
		}

		dstRoots, dstNames, dstBase := buildDst(missionSlug)

		fmt.Println()
		for _, sc := range scanned {
			fmt.Printf("  %s  %s  %s\n", dim("source "), cyan(sc.src), dim(fmt.Sprintf("(%d files, %s)", len(sc.files), fmtSize(uint64(sc.totalSize())))))
		}
		fmt.Printf("  %s  %s\n", dim("mission"), bold(missionSlug))
		for _, r := range dstRoots {
			fmt.Printf("  %s  %s\n", dim("dest   "), dim(r))
		}
		fmt.Println()
		if !*skipConf && !confirm() {
			exit(8, "aborted by user")
		}

		if !isAppend {
			if err := commitMission(year, missionNum); err != nil {
				exit(9, "err updating mission counter: %v", err)
			}
		}

		intr.set(dstRoots, !isAppend)

		runDay(scanned, missionSlug, dstRoots, dstNames, dstBase)

	} else {
		// ── Multi-day path: prompt for each day ──────────────────────────────
		fmt.Printf("  %s  %s across %s days:\n\n",
			cyan("◆"),
			dim(fmt.Sprintf("%d cards", len(scanned))),
			bold(strconv.Itoa(len(days))))
		for _, d := range days {
			fmt.Printf("     %s  %s  %s\n", bold(d.date), dim(fmt.Sprintf("%d files", d.fileCount)), dim(fmtSize(uint64(d.totalSize))))
		}

		suggestion := *missionFlag // use -ingest value as hint for first day if provided
		fmt.Printf("\n  Name or number for each day  %s:\n\n", dim("(- to skip)"))

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
			slug, isNew, num, skipped, err := promptMissionForDay(cfg, year, nextNum, d.date, hint)
			if err != nil {
				exit(4, "err reading mission counter: %v", err)
			}
			if skipped {
				continue
			}
			if isNew {
				nextNum++
			}
			dstRoots, dstNames, dstBase := buildDst(slug)
			plan = append(plan, dayPlan{d, slug, isNew, num, dstRoots, dstNames, dstBase})
		}

		if len(plan) == 0 {
			fmt.Printf("\n  %s\n", dim("all days skipped"))
			return
		}

		rule := strings.Repeat("─", 56)
		fmt.Printf("\n  %s\n  %s\n", bold("Plan"), dim(rule))
		for _, p := range plan {
			tag := green("new")
			if !p.isNew {
				tag = cyan("append")
			}
			fmt.Printf("  %s  →  %s  %s\n", dim(p.day.date), bold(p.slug), dim("["+tag+dim("]")))
			for _, r := range p.dstRoots {
				fmt.Printf("  %s      %s\n", strings.Repeat(" ", len(p.day.date)), dim(r))
			}
		}
		fmt.Printf("  %s\n\n", dim(rule))

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
			fmt.Printf("\n  %s  %s\n", blue("▶"), bold(p.slug))
			intr.set(p.dstRoots, p.isNew)
			runDay(p.day.cards, p.slug, p.dstRoots, p.dstNames, p.dstBase)
		}
	}
}
