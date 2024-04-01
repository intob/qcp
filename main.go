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
	"strings"
)

type job struct {
	src, dst string
	do       func() <-chan error
}

func main() {
	if len(os.Args) < 3 {
		panic("specify src and dst paths")
	}
	rootSrc, err := expandPath(os.Args[1])
	if err != nil {
		panic(err)
	}
	rootDst, err := expandPath(os.Args[2])
	if err != nil {
		panic(err)
	}
	match := func(src string) bool {
		return filepath.Ext(src) == ".txt"
	}

	skipConf := flag.Bool("y", false, "skip confirmation")
	flag.Parse()
	if !*skipConf && !confirm(rootSrc, rootDst, match) {
		panic("aborted")
	}

	for job := range walk(rootSrc, rootDst, match) {
		err := <-job.do()
		if err != nil {
			fmt.Printf("ERROR: copy %s: %s", job.src, err)
		} else {
			fmt.Println("done:", job.src)
		}
	}
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

func walk(rootSrc, rootDst string, match func(src string) bool) <-chan *job {
	ops := make(chan *job, 1)
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
				ops <- &job{
					src: src,
					dst: dst,
					do:  prepJob(src, dst),
				}
			}
			return err
		})
	}()
	return ops
}

func prepJob(src, dst string) func() <-chan error {
	return func() <-chan error {
		done := make(chan error)
		go func() {
			defer close(done)
			rd, err := os.Open(src)
			if err != nil {
				done <- err
				return
			}
			defer rd.Close()
			info, err := os.Stat(src)
			if err != nil {
				done <- err
				return
			}
			perm := info.Mode().Perm()
			err = os.MkdirAll(path.Dir(dst), 0777)
			if err != nil {
				done <- err
				return
			}
			wr, err := os.Create(dst)
			if err != nil {
				done <- err
				return
			}
			defer wr.Close()
			_, err = io.Copy(wr, rd)
			if err != nil {
				done <- err
				return
			}
			err = os.Chmod(dst, perm)
			if err != nil {
				done <- err
			}
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
