package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/user"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/inneslabs/fnpool"
	"github.com/inneslabs/jfmt"
)

type op struct {
	src, dst string
	do       func() <-chan *result
}

type result struct {
	err error
	n   int64
}

func main() {
	const includeFile = "/etc/qcpinclude"
	skipConf := flag.Bool("y", false, "skip confirmation")
	flag.Parse()
	if flag.NArg() < 2 {
		exit(1, "specify src and dst path")
	}
	srcRoot, err := expandPath(flag.Arg(0))
	if err != nil {
		exit(2, "err expanding src path: %v", err)
	}
	dstRoot, err := expandPath(flag.Arg(1))
	if err != nil {
		exit(3, "err expanding dst path: %v", err)
	}
	ops := make([]*op, 0)
	for op := range walk(includeFile, srcRoot, dstRoot) {
		ops = append(ops, op)
		fmt.Printf("plan: %s ->%s\n", op.src, op.dst)
	}
	if !*skipConf && !confirm() {
		exit(4, "aborted by user")
	}
	pool := fnpool.NewPool(runtime.NumCPU())
	var total atomic.Int64
	var wg sync.WaitGroup
	for _, op := range ops {
		wg.Add(1)
		pool.Dispatch(func() {
			defer wg.Done()
			res := <-op.do()
			if res.err != nil {
				fmt.Printf("ERROR: %v\n", res.err)
				return
			}
			fmt.Printf("done: ->%s\n", op.dst)
			total.Add(res.n)
		})
	}
	wg.Wait()
	size := jfmt.FmtSize64(uint64(total.Load()))
	fmt.Printf("copied %s from %s to %s\n", size, srcRoot, dstRoot)
}

func walk(includeFile, srcRoot, dstRoot string) <-chan *op {
	patterns, err := readPatterns(includeFile)
	if err != nil {
		exit(5, "err reading %q: %v", includeFile, err)
	}
	ops := make(chan *op, 1)
	go func(ops chan<- *op) {
		defer close(ops)
		filepath.WalkDir(srcRoot, func(src string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d != nil && d.IsDir() {
				return nil
			}
			if !match(srcRoot, src, patterns) {
				return nil
			}
			srcRel := strings.TrimPrefix(src, srcRoot)
			dst := path.Join(dstRoot, srcRel)
			ops <- &op{
				src: src,
				dst: dst,
				do:  prepJob(src, dst),
			}
			return nil
		})
	}(ops)
	return ops
}

func match(srcRoot, src string, patterns []string) bool {
	if path.Ext(src) == ".DS_Store" {
		return false
	}
	base := strings.TrimPrefix(src, srcRoot)
	for _, pattern := range patterns {
		if strings.HasPrefix(base, pattern) {
			return true
		}
	}
	return false
}

func prepJob(src, dst string) func() <-chan *result {
	return func() <-chan *result {
		done := make(chan *result)
		go func() {
			done <- job(src, dst)
			close(done)
		}()
		return done
	}
}

func job(src, dst string) *result {
	rd, err := os.Open(src)
	if err != nil {
		return &result{err, 0}
	}
	defer rd.Close()
	info, err := os.Stat(src)
	if err != nil {
		return &result{err, 0}
	}
	perm := info.Mode().Perm()
	err = os.MkdirAll(path.Dir(dst), 0777)
	if err != nil {
		return &result{err, 0}
	}
	wr, err := os.Create(dst)
	if err != nil {
		return &result{err, 0}
	}
	defer wr.Close()
	n, err := io.Copy(wr, rd)
	if err != nil {
		return &result{err, 0}
	}
	return &result{os.Chmod(dst, perm), n}
}

func readPatterns(filename string) ([]string, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()
	var patterns []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		patterns = append(patterns, line)
	}
	return patterns, scanner.Err()
}

func expandPath(path string) (string, error) {
	if strings.HasPrefix(path, "~") {
		usr, err := user.Current()
		if err != nil {
			return "", err
		}
		homeDir := usr.HomeDir
		return filepath.Join(homeDir, path[1:]), nil
	}
	return filepath.Abs(path)
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
