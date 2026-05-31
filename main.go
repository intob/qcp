package main

import (
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
	Card   CardConfig    `json:"card"`
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

type op struct {
	src, dst string
	do       func() <-chan *result
}

type result struct {
	err     error
	n       int64
	hash    string
	rel     string
	dstRoot string
}

func main() {
	skipConf := flag.Bool("y", false, "skip confirmation")
	missionFlag := flag.String("mission", "", "mission name (e.g. \"Altissimo with Anton\")")
	year := flag.Int("year", time.Now().Year(), "year override")
	flag.Parse()

	if *missionFlag == "" {
		exit(1, "-mission is required")
	}

	cfg := loadConfig()

	cardSrc := filepath.Join("/Volumes", cfg.Card.Volume, cfg.Card.Sub)
	if !dirExists(cardSrc) {
		exit(2, "card not mounted at %s", cardSrc)
	}

	missionName := sanitizeMission(*missionFlag)
	yearStr := strconv.Itoa(*year)

	num, err := peekMission(*year)
	if err != nil {
		exit(3, "err reading mission counter: %v", err)
	}
	missionSlug := fmt.Sprintf("%03d_%s", num, missionName)

	files, err := findFiles(cardSrc)
	if err != nil {
		exit(6, "err scanning card: %v", err)
	}
	if len(files) == 0 {
		exit(7, "no files found on card at %s", cardSrc)
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
		exit(8, "no destination drives mounted")
	}

	ops := make([]*op, 0, len(files)*len(dstRoots))
	for _, rel := range files {
		src := filepath.Join(cardSrc, rel)
		for _, dstRoot := range dstRoots {
			dst := filepath.Join(dstRoot, rel)
			fmt.Printf("plan: %s\n      -> %s\n", src, dst)
			ops = append(ops, &op{src: src, dst: dst, do: prepJob(src, dst, rel, dstRoot)})
		}
	}

	fmt.Printf("\nmission:      %s\n", missionSlug)
	fmt.Printf("destinations: %s\n", strings.Join(dstRoots, "\n              "))
	if !*skipConf && !confirm() {
		exit(9, "aborted by user")
	}

	if err := commitMission(*year, num); err != nil {
		exit(10, "err updating mission counter: %v", err)
	}

	pool := fnpool.NewPool(runtime.NumCPU())
	var total atomic.Int64
	var wg sync.WaitGroup
	var mu sync.Mutex
	checksums := make(map[string][]string) // dstRoot → "hash  rel" lines

	for _, op := range ops {
		wg.Add(1)
		pool.Dispatch(func() {
			defer wg.Done()
			res := <-op.do()
			if res.err != nil {
				fmt.Printf("ERROR: %v\n", res.err)
				return
			}
			fmt.Printf("done: %s\n", op.dst)
			total.Add(res.n)
			mu.Lock()
			checksums[res.dstRoot] = append(checksums[res.dstRoot],
				fmt.Sprintf("%s  %s", res.hash, res.rel))
			mu.Unlock()
		})
	}
	wg.Wait()

	for dstRoot, lines := range checksums {
		sort.Strings(lines)
		content := strings.Join(lines, "\n") + "\n"
		cPath := filepath.Join(dstRoot, "checksums.b3")
		if err := os.WriteFile(cPath, []byte(content), 0644); err != nil {
			fmt.Printf("ERROR writing checksums: %v\n", err)
		} else {
			fmt.Printf("checksums: %s\n", cPath)
		}
	}

	perDrive := jfmt.FmtSize64(uint64(total.Load()) / uint64(len(dstRoots)))
	fmt.Printf("copied %s to %d drive(s) → %s\n", perDrive, len(dstRoots), missionSlug)
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
		files = append(files, rel)
		return nil
	})
	return files, err
}

func prepJob(src, dst, rel, dstRoot string) func() <-chan *result {
	rel = strings.TrimPrefix(rel, string(os.PathSeparator))
	return func() <-chan *result {
		done := make(chan *result)
		go func() {
			r := job(src, dst)
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

	srcH := blake3.New(32, nil)
	n, err := io.Copy(wr, io.TeeReader(rd, srcH))
	wr.Close()
	if err != nil {
		return &result{err: err}
	}

	if err := os.Chmod(dst, perm); err != nil {
		return &result{err: err}
	}

	// verify destination
	dstRd, err := os.Open(dst)
	if err != nil {
		return &result{err: fmt.Errorf("verify open: %w", err)}
	}
	defer dstRd.Close()
	dstH := blake3.New(32, nil)
	if _, err := io.Copy(dstH, dstRd); err != nil {
		return &result{err: fmt.Errorf("verify read: %w", err)}
	}

	srcHash := hex.EncodeToString(srcH.Sum(nil))
	dstHash := hex.EncodeToString(dstH.Sum(nil))
	if srcHash != dstHash {
		return &result{err: fmt.Errorf("checksum mismatch: %s", dst)}
	}

	return &result{n: n, hash: srcHash}
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
