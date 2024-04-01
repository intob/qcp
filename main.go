package main

import (
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
	srcFname string
	do       func(rootDst string) <-chan error
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
	match := func(srcFname string, err error) bool {
		return filepath.Ext(srcFname) == ".txt"
	}
	for job := range walk(rootSrc, match) {
		err := <-job.do(rootDst)
		if err != nil {
			fmt.Printf("ERROR: copy %s: %s", job.srcFname, err)
		} else {
			fmt.Println("done:", job.srcFname)
		}
	}
}

func walk(rootSrc string, match func(srcFname string, err error) bool) <-chan *job {
	ops := make(chan *job, 1)
	go func() {
		defer close(ops)
		filepath.WalkDir(rootSrc, func(srcFname string, d fs.DirEntry, err error) error {
			if d != nil && d.IsDir() {
				return nil
			}
			if match(srcFname, err) {
				ops <- &job{
					srcFname: srcFname,
					do:       prepJob(rootSrc, srcFname),
				}
			}
			return err
		})
	}()
	return ops
}

func prepJob(rootSrc, srcFname string) func(rootDst string) <-chan error {
	return func(rootDst string) <-chan error {
		done := make(chan error)
		go func() {
			defer close(done)
			src, err := os.Open(srcFname)
			if err != nil {
				done <- err
				return
			}
			defer src.Close()
			srcInfo, err := os.Stat(srcFname)
			if err != nil {
				done <- err
				return
			}
			srcPerm := srcInfo.Mode().Perm()
			srcNoRoot := strings.Replace(srcFname, rootSrc, "", 1)
			dstFname := path.Join(rootDst, srcNoRoot)
			err = os.MkdirAll(path.Dir(dstFname), 0777)
			if err != nil {
				done <- err
				return
			}
			dst, err := os.Create(dstFname)
			if err != nil {
				done <- err
				return
			}
			defer dst.Close()
			_, err = io.Copy(dst, src)
			if err != nil {
				done <- err
				return
			}
			err = os.Chmod(dstFname, srcPerm)
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
