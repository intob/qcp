package main

import (
	"bytes"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"unsafe"
)

// residentFraction reports how much of a file is in the page cache.
func residentFraction(t *testing.T, path string) float64 {
	t.Helper()
	f, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	n := int(st.Size())
	b, err := syscall.Mmap(int(f.Fd()), 0, n, syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		t.Fatal(err)
	}
	defer syscall.Munmap(b)
	page := os.Getpagesize()
	vec := make([]byte, (n+page-1)/page)
	if _, _, errno := syscall.Syscall(syscall.SYS_MINCORE,
		uintptr(unsafe.Pointer(&b[0])), uintptr(n), uintptr(unsafe.Pointer(&vec[0]))); errno != 0 {
		t.Fatal(errno)
	}
	in := 0
	for _, v := range vec {
		if v&1 != 0 {
			in++
		}
	}
	return float64(in) / float64(len(vec))
}

// A copy is verified by reading it back. Written through the page cache, the
// whole file was still in memory when the verify phase read it, so the
// read-back never reached the drive: an ingest verified a clip that the drive
// it was written to did not hold. Nothing job writes may be left in the cache.
func TestJobLeavesNothingOfTheCopyInMemory(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src.mp4")
	if err := os.WriteFile(src, bytes.Repeat([]byte("footage "), 8<<20), 0o644); err != nil { // 64MiB
		t.Fatal(err)
	}
	dst := filepath.Join(dir, "out", "dst.mp4")
	if r := job(src, dst, nil); r.err != nil {
		t.Fatal(r.err)
	}
	if got := residentFraction(t, dst); got > 0.01 {
		t.Errorf("%.0f%% of the copy is still in the page cache, so verifying it would read memory, not the drive", 100*got)
	}
	if got := residentFraction(t, src); got < 0.5 {
		t.Skipf("source only %.0f%% resident: this machine is not keeping written files in the cache, so the test proves nothing", 100*got)
	}
}
