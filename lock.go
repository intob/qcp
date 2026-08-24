package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
)

// proxyLockName is the lockfile at the root of a drive's proxy tree. A dotfile,
// like the flags file, so nothing that walks the drive has to know about it.
const proxyLockName = ".qcp-proxy.lock"

// lockProxyTree takes an exclusive lock on one drive's proxy tree for the
// lifetime of a run, and returns the release.
//
// Two concurrent -proxy runs over the same mission corrupt each other's
// bookkeeping rather than merely duplicating work: proxies.json is written once
// at the end of a run, assembled from that run's own view of what it generated
// and what it judged cached. The second run's plan is computed against a
// manifest the first has not written yet, and whichever finishes last
// overwrites with its own partial picture — so clips that were built read as
// missing and get built again. It is a lock rather than a merge because two
// simultaneous runs have nothing useful to say to each other: the loser should
// come back when the winner has finished and plan against the truth.
func lockProxyTree(root string) (func(), error) {
	if err := os.MkdirAll(root, 0777); err != nil {
		return nil, err
	}
	path := filepath.Join(root, proxyLockName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		return nil, fmt.Errorf("another qcp is generating proxies on this drive")
	}
	// The pid is for a human reading the file, not for the locking, which is
	// the flock itself and is released even if the process dies outright.
	f.Truncate(0)
	f.WriteAt([]byte(fmt.Sprintf("%d\n", os.Getpid())), 0)
	return func() {
		syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		f.Close()
		os.Remove(path)
	}, nil
}

// sweepPartFiles removes .qcp-part temporaries under root. encodeOutputs
// already deletes its own on failure, so anything left is from a run that was
// killed outright and never got to. It is only safe to do this while holding
// the tree lock — otherwise it would delete a live run's work in progress.
func sweepPartFiles(root string) []string {
	var found []string
	filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil
		}
		if strings.Contains(d.Name(), ".qcp-part") {
			if os.Remove(path) == nil {
				found = append(found, path)
			}
		}
		return nil
	})
	return found
}
