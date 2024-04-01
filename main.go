package main

import (
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
	"sync/atomic"

	"github.com/inneslabs/fnpool"
	"github.com/inneslabs/jfmt"
)

var ignoreExts = []string{
	".DS_Store",
}

var inc = []string{
	".gitconfig",
	".keys/",
	".ssh/",
	".zshrc",
	"Documents/",
	"Misc/",
	"Music/",
	"Pictures/",
	"Video/",
}

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
		exit(1, "specify src and dst paths")
	}
	rootSrc, err := expandPath(flag.Arg(0))
	if err != nil {
		exit(2, "err expanding src path: %v", err)
	}
	rootDst, err := expandPath(flag.Arg(1))
	if err != nil {
		exit(3, "err expanding dst path: %v", err)
	}

	match := func(src string) bool {
		ext := path.Ext(src)
		for _, ignoreExt := range ignoreExts {
			if ext == ignoreExt {
				return false
			}
		}
		rel, err := filepath.Rel(rootSrc, src)
		if err != nil {
			exit(4, "err resolving path: %v", err)
		}
		for _, i := range inc {
			if strings.HasPrefix(rel, i) {
				return true
			}
		}
		return false
	}

	if !*skipConf && !confirm(rootSrc, rootDst, match) {
		exit(4, "aborted by user")
	}

	pool := fnpool.NewPool(runtime.NumCPU())
	var total atomic.Int64
	for job := range walk(rootSrc, rootDst, match) {
		pool.Dispatch(func() {
			res := <-job.do()
			if res.err != nil {
				fmt.Printf("ERROR: %v\n", res.err)
				return
			}
			relDst, err := filepath.Rel(rootDst, job.dst)
			if err != nil {
				panic(err)
			}
			fmt.Printf("done: %s -> %s\n", flag.Arg(0), relDst)
			total.Add(res.n)
		})
	}
	pool.StopAndWait()
	size := jfmt.FmtSize64(uint64(total.Load()))
	fmt.Printf("copied %s to %s\n", size, rootDst)
}

func confirm(rootSrc, rootDst string, match func(src string) bool) bool {
	for job := range walk(rootSrc, rootDst, match) {
		fmt.Printf("%s -> %s\n", job.src, job.dst)
	}
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
			if match(src) {
				srcNoRoot := strings.Replace(src, rootSrc, "", 1)
				dst := path.Join(rootDst, srcNoRoot)
				ops <- &op{
					src: src,
					dst: dst,
					do:  prepJob(src, dst),
				}
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
			defer close(done)
			rd, err := os.Open(src)
			if err != nil {
				done <- &result{err, 0}
				return
			}
			defer rd.Close()
			info, err := os.Stat(src)
			if err != nil {
				done <- &result{err, 0}
				return
			}
			perm := info.Mode().Perm()
			err = os.MkdirAll(path.Dir(dst), 0777)
			if err != nil {
				done <- &result{err, 0}
				return
			}
			wr, err := os.Create(dst)
			if err != nil {
				done <- &result{err, 0}
				return
			}
			defer wr.Close()
			n, err := io.Copy(wr, rd)
			if err != nil {
				done <- &result{err, 0}
				return
			}
			done <- &result{os.Chmod(dst, perm), n}
		}()
		return done
	}
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
