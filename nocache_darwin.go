package main

import (
	"os"
	"syscall"
)

// keepOutOfCache stops what is written through f from being kept in the
// page cache, so the next read of the file has to come from the drive.
//
// A copy is verified by reading it back, and a file just written through the
// cache is read back from memory: with 32GB of RAM a whole mission's worth of
// fresh copies is still resident when the verify phase runs. The read-back then
// proves only that memory holds the right bytes, and damage on the way to the
// drive — or on it — goes unseen until the copy is next read from disk. That is
// how an ingest verified a clip that T7 never held, and -sync then archived the
// damaged bytes under a fresh hash.
//
// Only writes are kept out. The read-back itself goes through the cache as
// usual: a read with F_NOCACHE gets no read-ahead and ran at about 50MB/s
// against 770MB/s on a USB SSD, and one such read stalled the drive outright.
// A cached page that came from a read is a copy of what is on the disk, so with
// nothing written left behind, a normal read sees the drive's bytes at full
// speed. Writing this way costs nothing measurable.
func keepOutOfCache(f *os.File) error {
	_, _, errno := syscall.Syscall(syscall.SYS_FCNTL, f.Fd(), syscall.F_NOCACHE, 1)
	if errno != 0 {
		return &os.PathError{Op: "fcntl F_NOCACHE", Path: f.Name(), Err: errno}
	}
	return nil
}
