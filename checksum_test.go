package main

import (
	"os"
	"path/filepath"
	"testing"
)

// -checksum replaced checksums.b3 with whatever was on disk. A mission only has
// to gain one unrecorded file to be rehashed, so a file that had rotted since
// ingest had its good hash overwritten by the bad one, and the run said ✓.
func TestChecksumNeverOverwritesARecordedHash(t *testing.T) {
	for _, mode := range []string{"mission", "year"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			drive := t.TempDir()
			dir := filepath.Join(drive, "2026", "001_A")
			cfg := Config{Drives: []DriveConfig{{Volume: "HOT", Path: drive, Role: "hot"}}}
			writeFile(t, filepath.Join(dir, "a.mp4"), "rotted")
			writeFile(t, filepath.Join(dir, "b.mp4"), "appended")
			writeFile(t, filepath.Join(dir, "checksums.b3"), b3("original")+"  a.mp4\n")

			var ok bool
			if mode == "mission" {
				ok = runChecksum(cfg, 1, 2026)
			} else {
				ok = runChecksumYear(cfg, 2026)
			}

			if ok {
				t.Error("reported success over a file that no longer matches its recorded hash")
			}
			m := readChecksumFile(filepath.Join(dir, "checksums.b3"))
			if m["a.mp4"] != b3("original") {
				t.Errorf("recorded hash was overwritten: %v", m)
			}
		})
	}
}

// A copy that was already fully checksummed was skipped and never consulted, so
// a cold copy hashed on its own recorded whatever it held.
func TestChecksumHoldsACopyToTheHashesOfTheOthers(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	hot, cold := t.TempDir(), t.TempDir()
	cfg := Config{Drives: []DriveConfig{
		{Volume: "HOT", Path: hot, Role: "hot"},
		{Volume: "COLD", Path: cold, Role: "cold"},
	}}
	writeFile(t, filepath.Join(hot, "2026", "001_A", "a.mp4"), "original")
	writeFile(t, filepath.Join(hot, "2026", "001_A", "checksums.b3"), b3("original")+"  a.mp4\n")
	writeFile(t, filepath.Join(cold, "2026", "001_A", "a.mp4"), "rotted!!")

	if runChecksum(cfg, 1, 2026) {
		t.Error("reported success for a cold copy that disagrees with the hot copy's record")
	}
	if _, err := os.Stat(filepath.Join(cold, "2026", "001_A", "checksums.b3")); err == nil {
		t.Error("the cold copy's damage was recorded")
	}
}

// Rewriting the manifest from the disk dropped the entries for files that had
// gone missing, and with them the only sign that they were gone.
func TestChecksumKeepsTheRecordOfAMissingFile(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	drive := t.TempDir()
	dir := filepath.Join(drive, "2026", "001_A")
	cfg := Config{Drives: []DriveConfig{{Volume: "HOT", Path: drive, Role: "hot"}}}
	writeFile(t, filepath.Join(dir, "a.mp4"), "a")
	writeFile(t, filepath.Join(dir, "b.mp4"), "new")
	writeFile(t, filepath.Join(dir, "checksums.b3"), b3("a")+"  a.mp4\n"+b3("gone")+"  gone.mp4\n")

	if runChecksum(cfg, 1, 2026) {
		t.Error("reported success with a recorded file missing")
	}
	if m := readChecksumFile(filepath.Join(dir, "checksums.b3")); m["gone.mp4"] == "" {
		t.Errorf("the missing file's entry was dropped: %v", m)
	}
}

// The ordinary case still works: an append is recorded, the old entries kept.
func TestChecksumRecordsAnAppend(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	drive := t.TempDir()
	dir := filepath.Join(drive, "2026", "001_A")
	cfg := Config{Drives: []DriveConfig{{Volume: "HOT", Path: drive, Role: "hot"}}}
	writeFile(t, filepath.Join(dir, "a.mp4"), "a")
	writeFile(t, filepath.Join(dir, "b.mp4"), "appended")
	writeFile(t, filepath.Join(dir, "checksums.b3"), b3("a")+"  a.mp4\n")

	if !runChecksum(cfg, 1, 2026) {
		t.Fatal("failed on a plain append")
	}
	m := readChecksumFile(filepath.Join(dir, "checksums.b3"))
	if m["a.mp4"] != b3("a") || m["b.mp4"] != b3("appended") {
		t.Errorf("manifest = %v", m)
	}
}
