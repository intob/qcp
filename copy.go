package main

import (
	"encoding/hex"
	"io"
	"os"
	"path/filepath"

	"lukechampine.com/blake3"
)

func prepJob(src, dst, rel, dstRoot string, bar *barTracker) func() <-chan *result {
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

func job(src, dst string, bar *barTracker) *result {
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
		w = &progressWriter{w: wr, tracker: bar}
	}
	buf := make([]byte, 4*1024*1024)
	n, err := io.CopyBuffer(w, io.TeeReader(rd, h), buf)
	syncErr := wr.Sync()
	closeErr := wr.Close()
	if err != nil {
		os.Remove(dst)
		return &result{err: err}
	}
	if syncErr != nil {
		os.Remove(dst)
		return &result{err: syncErr}
	}
	if closeErr != nil {
		os.Remove(dst)
		return &result{err: closeErr}
	}

	if err := os.Chmod(dst, perm); err != nil {
		os.Remove(dst)
		return &result{err: err}
	}

	return &result{n: n, srcHash: hex.EncodeToString(h.Sum(nil))}
}

func hashFile(path string, bar *barTracker) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := blake3.New(32, nil)
	var r io.Reader = f
	if bar != nil {
		r = &progressReader{r: f, tracker: bar}
	}
	buf := make([]byte, 4*1024*1024)
	if _, err := io.CopyBuffer(h, r, buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
