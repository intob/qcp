package main

import (
	"os"
	"path/filepath"
	"testing"
)

// Two concurrent runs over one proxy tree corrupt each other's manifest rather
// than merely duplicating work, so the second must be turned away instead of
// waiting or proceeding.
func TestProxyTreeLockExcludesASecondRun(t *testing.T) {
	root := filepath.Join(t.TempDir(), "proxies")
	unlock, err := lockProxyTree(root)
	if err != nil {
		t.Fatalf("first lock failed: %v", err)
	}
	if _, err := lockProxyTree(root); err == nil {
		t.Fatal("a second run took the lock while the first held it")
	}
	unlock()
	// Once released the tree is available again, and the lockfile is gone.
	again, err := lockProxyTree(root)
	if err != nil {
		t.Fatalf("lock not reusable after release: %v", err)
	}
	again()
	if _, err := os.Stat(filepath.Join(root, proxyLockName)); !os.IsNotExist(err) {
		t.Error("the lockfile outlived the run that held it")
	}
}

// Anything left with a .qcp-part name is from a run killed outright, since
// encodeOutputs removes its own on failure. Finished renditions must survive.
func TestSweepRemovesOnlyUnfinishedFiles(t *testing.T) {
	root := t.TempDir()
	browse := filepath.Join(root, "2026", "001_X", "browse")
	if err := os.MkdirAll(browse, 0777); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(browse, "GX012856.mp4")
	drop := filepath.Join(browse, "GX012856.qcp-part.mp4")
	for _, f := range []string{keep, drop} {
		if err := os.WriteFile(f, []byte("x"), 0644); err != nil {
			t.Fatal(err)
		}
	}
	got := sweepPartFiles(root)
	if len(got) != 1 {
		t.Fatalf("swept %d file(s), want 1: %v", len(got), got)
	}
	if _, err := os.Stat(keep); err != nil {
		t.Error("a finished rendition was swept away")
	}
	if _, err := os.Stat(drop); !os.IsNotExist(err) {
		t.Error("the unfinished file survived the sweep")
	}
}
