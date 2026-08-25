package main

import (
	"os"
	"path/filepath"
	"testing"
)

// The mission counter is a promise never to mint a number twice, and -init
// resets it from what a scan can see — which depends on what happens to be
// plugged in. main.go passed !yearAll as the "explicit year" flag, so a bare
// `qcp -init` took the branch that lets the counter move *down*: with the
// archive in a drawer and old missions already evicted off the hot drives, a
// year whose counter had reached 042 was reset to whatever remained, and the
// next -ingest handed out a number that already named a mission.
//
// Raising is always safe. Lowering now needs a -year the user actually typed.
func TestInitOnlyRewindsForAnExplicitYear(t *testing.T) {
	drive := t.TempDir()
	if err := os.MkdirAll(filepath.Join(drive, "2026", "030_Recent"), 0o755); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Drives: []DriveConfig{
		{Volume: "T9", Path: drive, Role: "hot"},
		// the archive holding 031..042, not mounted
		{Volume: "ARCHIVE", Path: filepath.Join(t.TempDir(), "absent"), Role: "cold"},
	}}

	seqIs := func(t *testing.T, want int) {
		t.Helper()
		seq, err := readSeq()
		if err != nil {
			t.Fatal(err)
		}
		if seq[2026] != want {
			t.Errorf("seq[2026] = %d, want %d", seq[2026], want)
		}
	}

	t.Run("bare -init cannot rewind", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		if err := writeSeq(map[int]int{2026: 42}); err != nil {
			t.Fatal(err)
		}
		runInit(cfg, 2026, true, false)
		seqIs(t, 42)
	})

	t.Run("-year 2026 may rewind", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		if err := writeSeq(map[int]int{2026: 42}); err != nil {
			t.Fatal(err)
		}
		runInit(cfg, 2026, true, true)
		seqIs(t, 30)
	})

	t.Run("raising needs no permission", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		if err := writeSeq(map[int]int{2026: 7}); err != nil {
			t.Fatal(err)
		}
		runInit(cfg, 2026, true, false)
		seqIs(t, 30)
	})
}
