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

type progressWriter struct {
	w   io.Writer
	bar *mpb.Bar
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	start := time.Now()
	n, err := pw.w.Write(p)
	pw.bar.EwmaIncrBy(n, time.Since(start))
	return n, err
}

type progressReader struct {
	r   io.Reader
	bar *mpb.Bar
}

func (pr *progressReader) Read(p []byte) (int, error) {
	start := time.Now()
	n, err := pr.r.Read(p)
	pr.bar.EwmaIncrBy(n, time.Since(start))
	return n, err
}

func main() {
	skipConf := flag.Bool("y", false, "skip confirmation")
	missionFlag := flag.String("mission", "", "mission name (e.g. \"Altissimo with Anton\")")
	year := flag.Int("year", time.Now().Year(), "year override")
	toMission := flag.Int("to", 0, "append to existing mission number")
	verifyMission := flag.Int("verify", 0, "re-verify mission number across all mounted drives")
	flag.Parse()

	cfg := loadConfig()

	if *verifyMission > 0 {
		runVerify(cfg, *verifyMission)
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
		cancel()
		signal.Stop(sigCh)
	}()

	cleanup := func() {
		fmt.Print("\n\ninterrupted — delete partial mission and revert counter? (y/n): ")
		var resp string
		fmt.Scan(&resp)
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
	}

	sizeStr := jfmt.FmtSize64(uint64(totalSize))
	fmt.Printf("\ncopying %d files (%s) to %d drive(s)\n\n", totalFiles, sizeStr, len(dstRoots))

	// Phase 1: copy
	p1 := mpb.NewWithContext(ctx, mpb.WithWidth(64))
	copyBars := make(map[string]*mpb.Bar)
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
	p1.Wait()

	if ctx.Err() != nil {
		cleanup()
	}

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
	verifyBars := make(map[string]*mpb.Bar)
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
	p2.Wait()

	if ctx.Err() != nil {
		cleanup()
	}

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

func addBar(p *mpb.Progress, name string, total int64) *mpb.Bar {
	return p.AddBar(total,
		mpb.PrependDecorators(
			decor.Name(fmt.Sprintf("%-12s", name)),
			decor.CountersKibiByte("% .1f / % .1f  "),
		),
		mpb.AppendDecorators(
			decor.EwmaSpeed(decor.SizeB1024(0), "% .1f  ", 30),
			decor.OnComplete(decor.EwmaETA(decor.ET_STYLE_GO, 30), "✓"),
		),
	)
}

func volName(dstRoot string) string {
	parts := strings.SplitN(dstRoot, string(os.PathSeparator), 4)
	if len(parts) >= 3 {
		return parts[2]
	}
	return dstRoot
}

type scannedCard struct {
	mountedCard
	files []fileEntry
}

func buildOps(scanned []scannedCard, dstRoots []string, bars map[string]*mpb.Bar) []*op {
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

	fmt.Printf("verifying mission %03d on %d drive(s)\n\n", missionNum, len(jobs))

	p := mpb.New(mpb.WithWidth(64))
	var failed atomic.Int64
	var wg sync.WaitGroup
	pool := fnpool.NewPool(runtime.NumCPU())

	for _, job := range jobs {
		bar := addBar(p, job.vol, job.totalSize)
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

func prepJob(src, dst, rel, dstRoot string, bar *mpb.Bar) func() <-chan *result {
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

func job(src, dst string, bar *mpb.Bar) *result {
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
		w = &progressWriter{w: wr, bar: bar}
	}
	n, err := io.Copy(w, io.TeeReader(rd, h))
	wr.Sync()
	wr.Close()
	if err != nil {
		return &result{err: err}
	}

	if err := os.Chmod(dst, perm); err != nil {
		return &result{err: err}
	}

	return &result{n: n, srcHash: hex.EncodeToString(h.Sum(nil))}
}

func hashFile(path string, bar *mpb.Bar) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := blake3.New(32, nil)
	var r io.Reader = f
	if bar != nil {
		r = &progressReader{r: f, bar: bar}
	}
	if _, err := io.Copy(h, r); err != nil {
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
