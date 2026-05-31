package main

import (
	"bufio"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
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
	"lukechampine.com/blake3"
)

const seqPath = "~/.qcp_seq"

type Config struct {
	Cards  []CardConfig  `json:"cards"`
	Drives []DriveConfig `json:"drives"`
}

type CardConfig struct {
	Name   string `json:"name"`
	Volume string `json:"volume"`
	Sub    string `json:"sub"`
}

type DriveConfig struct {
	Volume string `json:"volume"`
	Root   string `json:"root"`
}

type mountedCard struct {
	CardConfig
	src string // full path to card sub dir
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

func main() {
	skipConf := flag.Bool("y", false, "skip confirmation")
	missionFlag := flag.String("mission", "", "mission name (e.g. \"Altissimo with Anton\")")
	year := flag.Int("year", time.Now().Year(), "year override")
	toMission := flag.Int("to", 0, "append to existing mission number")
	verifyDir := flag.String("verify", "", "re-verify checksums in a mission dir")
	flag.Parse()

	if *verifyDir != "" {
		runVerify(*verifyDir)
		return
	}

	cfg := loadConfig()

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

	dstRoots := []string{}
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

	// Use per-card subdirs when multiple cards are mounted or appending to an existing mission.
	useSubdir := len(cards) > 1 || isAppend

	ops, err := buildOps(cards, dstRoots, useSubdir)
	if err != nil {
		exit(6, "err scanning cards: %v", err)
	}
	if len(ops) == 0 {
		exit(7, "no files found on mounted cards")
	}

	for _, op := range ops {
		fmt.Printf("plan: %s\n      -> %s\n", op.src, op.dst)
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

	// Phase 1: copy
	fmt.Println("\ncopying...")
	results := make([]*result, len(ops))
	pool := fnpool.NewPool(runtime.NumCPU())
	var total atomic.Int64
	var wg sync.WaitGroup
	for i, op := range ops {
		wg.Add(1)
		i, op := i, op
		pool.Dispatch(func() {
			defer wg.Done()
			r := <-op.do()
			results[i] = r
			if r.err != nil {
				fmt.Printf("ERROR copy: %v\n", r.err)
				return
			}
			fmt.Printf("copied: %s\n", op.dst)
			total.Add(r.n)
		})
	}
	wg.Wait()

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
	fmt.Println("\nverifying...")
	pool2 := fnpool.NewPool(runtime.NumCPU())
	var mu sync.Mutex
	newChecksums := make(map[string][]string) // dstRoot → lines
	var verifyFailed atomic.Int64
	var wg2 sync.WaitGroup
	for _, r := range results {
		wg2.Add(1)
		r := r
		pool2.Dispatch(func() {
			defer wg2.Done()
			got, err := hashFile(r.dst)
			if err != nil {
				fmt.Printf("ERROR verify: %v\n", err)
				verifyFailed.Add(1)
				return
			}
			if got != r.srcHash {
				fmt.Printf("MISMATCH: %s\n", r.dst)
				verifyFailed.Add(1)
				return
			}
			fmt.Printf("ok: %s\n", r.dst)
			mu.Lock()
			newChecksums[r.dstRoot] = append(newChecksums[r.dstRoot],
				fmt.Sprintf("%s  %s", got, r.rel))
			mu.Unlock()
		})
	}
	wg2.Wait()

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
		} else {
			fmt.Printf("checksums: %s\n", cPath)
		}
	}

	perDrive := jfmt.FmtSize64(uint64(total.Load()) / uint64(len(dstRoots)))
	fmt.Printf("\ncopied %s to %d drive(s) → %s\n", perDrive, len(dstRoots), missionSlug)
}

func buildOps(cards []mountedCard, dstRoots []string, useSubdir bool) ([]*op, error) {
	var ops []*op
	for _, card := range cards {
		files, err := findFiles(card.src)
		if err != nil {
			return nil, fmt.Errorf("card %s: %w", card.Name, err)
		}
		for _, rel := range files {
			src := filepath.Join(card.src, rel)
			dstRel := rel
			if useSubdir {
				dstRel = filepath.Join(card.Name, rel)
			}
			for _, dstRoot := range dstRoots {
				dst := filepath.Join(dstRoot, dstRel)
				ops = append(ops, &op{src: src, dst: dst, do: prepJob(src, dst, dstRel, dstRoot)})
			}
		}
	}
	return ops, nil
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

// findMissionSlug looks for an existing NNN_ directory across mounted drives.
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

// mergeChecksums reads existing checksums.b3 and merges with new lines (new wins on collision).
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

func readChecksumFile(path string) map[string]string { // rel → hash
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

func runVerify(dir string) {
	cPath := filepath.Join(dir, "checksums.b3")
	f, err := os.Open(cPath)
	if err != nil {
		exit(1, "cannot open %s: %v", cPath, err)
	}
	defer f.Close()

	type entry struct{ hash, rel string }
	var entries []entry
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "  ", 2)
		if len(parts) == 2 {
			entries = append(entries, entry{parts[0], parts[1]})
		}
	}
	if err := scanner.Err(); err != nil {
		exit(1, "err reading checksums: %v", err)
	}

	fmt.Printf("verifying %d files in %s\n", len(entries), dir)

	pool := fnpool.NewPool(runtime.NumCPU())
	var failed atomic.Int64
	var wg sync.WaitGroup
	for _, e := range entries {
		wg.Add(1)
		e := e
		pool.Dispatch(func() {
			defer wg.Done()
			got, err := hashFile(filepath.Join(dir, e.rel))
			if err != nil {
				fmt.Printf("ERROR: %v\n", err)
				failed.Add(1)
				return
			}
			if got != e.hash {
				fmt.Printf("FAIL: %s\n", e.rel)
				failed.Add(1)
				return
			}
			fmt.Printf("ok: %s\n", e.rel)
		})
	}
	wg.Wait()

	if n := failed.Load(); n > 0 {
		exit(1, "%d file(s) failed", n)
	}
	fmt.Printf("all %d files ok\n", len(entries))
}

func findFiles(root string) ([]string, error) {
	var files []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d == nil || d.IsDir() {
			return nil
		}
		rel := strings.TrimPrefix(path, root)
		for _, part := range strings.Split(rel, string(os.PathSeparator)) {
			if strings.HasPrefix(part, ".") {
				return nil
			}
		}
		files = append(files, strings.TrimPrefix(rel, string(os.PathSeparator)))
		return nil
	})
	return files, err
}

func prepJob(src, dst, rel, dstRoot string) func() <-chan *result {
	return func() <-chan *result {
		done := make(chan *result)
		go func() {
			r := job(src, dst)
			r.dst = dst
			r.rel = rel
			r.dstRoot = dstRoot
			done <- r
			close(done)
		}()
		return done
	}
}

func job(src, dst string) *result {
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
	n, err := io.Copy(wr, io.TeeReader(rd, h))
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

func hashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := blake3.New(32, nil)
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

func peekMission(year int) (int, error) {
	seq, err := readSeq()
	if err != nil {
		return 0, err
	}
	return seq[year] + 1, nil
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
