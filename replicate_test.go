package main

import (
	"os"
	"path/filepath"
	"testing"
)

// -replicate took the first cold drive that had a mission as the source and
// diffed every other copy against it, so a gap *on that drive* could never be
// filled: the drive listed first in the config silently defined the mission and
// the run reported "cold drives are in sync" over an archive that was short a
// file. README.md sells this exact case — "to catch up a drive that wasn't
// present during -sync" — and it worked only if the stale drive happened to
// sort second.
//
// Same rule as resolveSource on the pull side now: the fullest copy is the
// source, whichever drive it is on.
func TestReplicateFillsTheThinnerColdDrive(t *testing.T) {
	first, second := t.TempDir(), t.TempDir()
	for _, base := range []string{first, second} {
		writeMissionFile(t, base, "2026", "042_Test", "a.mxf", "aaa")
	}
	// only the second drive has it, and the first is the one listed first
	writeMissionFile(t, second, "2026", "042_Test", "b.mxf", "bbb")

	cfg := Config{Drives: []DriveConfig{
		{Volume: "ARCHIVE_01", Path: first, Role: "cold"},
		{Volume: "ARCHIVE_02", Path: second, Role: "cold"},
	}}
	if ok := runReplicate(cfg, 2026, true); !ok {
		t.Fatal("runReplicate reported failure")
	}

	got, err := os.ReadFile(filepath.Join(first, "2026", "042_Test", "b.mxf"))
	if err != nil {
		t.Fatalf("ARCHIVE_01 still lacks b.mxf after -replicate: %v", err)
	}
	if string(got) != "bbb" {
		t.Errorf("b.mxf on ARCHIVE_01 = %q, want %q", got, "bbb")
	}
	// and the copy it made is recorded, like every other transfer's
	if h := readChecksumFile(filepath.Join(first, "2026", "042_Test", "checksums.b3")); h["b.mxf"] == "" {
		t.Errorf("checksums.b3 on ARCHIVE_01 has no entry for the file it just copied")
	}
}
