package main

import (
	"os"
	"path/filepath"
	"testing"
)

// -clean walked the drive's footage root. T9 is configured with "root": "", so
// that was the whole volume, and it deleted ._* files and empty directories
// from Spotlight's store, the Trash and anything else kept on the drive.
func TestCleanOnlyTouchesYearDirectories(t *testing.T) {
	drive := t.TempDir()
	cfg := Config{Drives: []DriveConfig{{Volume: "T9", Path: drive, Root: "", Role: "hot"}}}
	writeFile(t, filepath.Join(drive, "2026", "001_A", "._clip.mp4"), "appledouble")
	os.MkdirAll(filepath.Join(drive, "2026", "001_A", "empty"), 0o755)
	writeFile(t, filepath.Join(drive, "Personal", "._notes.txt"), "keep")
	os.MkdirAll(filepath.Join(drive, "Personal", "a", "b"), 0o755)
	os.MkdirAll(filepath.Join(drive, ".Spotlight-V100", "Store-V2", "x"), 0o755)

	runClean(cfg, true, false, 2026)

	for _, gone := range []string{"2026/001_A/._clip.mp4", "2026/001_A/empty"} {
		if exists(filepath.Join(drive, gone)) {
			t.Errorf("%s was not cleaned", gone)
		}
	}
	for _, kept := range []string{"Personal/._notes.txt", "Personal/a/b", ".Spotlight-V100/Store-V2/x"} {
		if !exists(filepath.Join(drive, kept)) {
			t.Errorf("%s is outside the footage and was removed", kept)
		}
	}
}
