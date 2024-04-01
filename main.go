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
	skipConf := flag.Bool("y", false, "skip confirmation")
	flag.Parse()

	if flag.NArg() < 2 {
		exit(1, "specify src and dst path")
	}

	rootSrc, err := expandPath(flag.Arg(0))
	if err != nil {
		exit(2, "err expanding src path: %v", err)
	}
	rootDst, err := expandPath(flag.Arg(1))
	if err != nil {
		exit(3, "err expanding dst path: %v", err)
	}

	patterns, err := readPatterns("/etc/qcpinclude")
	if err != nil {
		exit(5, "err reading globfile: %v", err)
	}

	match := func(src string) bool {
		base := strings.Replace(src, rootSrc, "", 1)
		for _, pattern := range patterns {
			match, err := filepath.Match(pattern, base)
			if err != nil {
				exit(6, "malformed pattern: %v", err)
			}
			if match {
				return true
			}
		}
		return false
	}

	ops := make([]*op, 0)
	for op := range walk(rootSrc, rootDst, match) {
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
	fmt.Printf("copied %s from %s to %s\n", size, rootSrc, rootDst)
}

func confirm() bool {
	fmt.Print("enter \"y\" to confirm: ")
	var resp string
	fmt.Scan(&resp)
	return resp == "y"
}

func walk(rootSrc, rootDst string, match func(src string) bool) <-chan *op {
	ops := make(chan *op, 1)
	go func() {
		defer close(ops)
		filepath.WalkDir(rootSrc, func(src string, d fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if d != nil && d.IsDir() {
				return nil
			}
			if !match(src) {
				return nil
			}
			srcNoRoot := strings.Replace(src, rootSrc, "", 1)
			dst := path.Join(rootDst, srcNoRoot)
			ops <- &op{
				src: src,
				dst: dst,
				do:  prepJob(src, dst),
			}
			return nil
		})
	}()
	return ops
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

func expandPath(path string) (string, error) {
	if strings.HasPrefix(path, "~/") {
		usr, err := user.Current()
		if err != nil {
			return "", err
		}
		homeDir := usr.HomeDir
		return filepath.Join(homeDir, path[2:]), nil
	}
	return filepath.Abs(path)
}

func exit(code int, msg string, args ...any) {
	fmt.Printf(msg+"\n", args...)
	os.Exit(code)
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
