package main

import (
	"os"
	"path/filepath"
	"testing"
)

// -renumber renamed only the drives that were mounted. The archive kept the
// old numbers, its missions were invisible to the new numbering, and the
// counter was set to the number of missions it could see — moving it back
// below numbers already spent on the drive in the drawer.
func TestRenumberRefusesWithADriveAway(t *testing.T) {
	root := fixtureDir(t)
	hot := filepath.Join(root, "hot")
	cfg := Config{Drives: []DriveConfig{
		{Volume: "HOT", Path: hot, Role: "hot"},
		{Volume: "ARCHIVE", Path: filepath.Join(root, "absent"), Role: "cold"},
	}}
	if !isChild(t) {
		os.MkdirAll(filepath.Join(hot, "2026", "003_C"), 0o755)
		os.MkdirAll(filepath.Join(hot, "2026", "007_G"), 0o755)
		writeFile(t, filepath.Join(root, ".qcp_seq"), `{"2026": 7}`)
	}

	code, out := inSubprocess(t, root, func() { runRenumber(cfg, 2026, true) })

	if code == 0 {
		t.Errorf("renumber ran with the archive unmounted\n%s", out)
	}
	if !dirExists(filepath.Join(hot, "2026", "003_C")) || !dirExists(filepath.Join(hot, "2026", "007_G")) {
		t.Errorf("missions were renamed\n%s", out)
	}
	t.Setenv("HOME", root)
	if got := seqFor(t, 2026); got != 7 {
		t.Errorf("seq = %d, want 7", got)
	}
}

// Renumbering deleted every renamed mission's checksums.b3 as "stale", though
// its paths are relative to the mission and the rename changes none of them.
func TestRenumberKeepsTheManifest(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	hot := t.TempDir()
	cfg := Config{Drives: []DriveConfig{{Volume: "HOT", Path: hot, Role: "hot"}}}
	writeFile(t, filepath.Join(hot, "2026", "003_C", "a.mp4"), "a")
	writeFile(t, filepath.Join(hot, "2026", "003_C", "checksums.b3"), b3("a")+"  a.mp4\n")

	runRenumber(cfg, 2026, true)

	m := readChecksumFile(filepath.Join(hot, "2026", "001_C", "checksums.b3"))
	if m["a.mp4"] != b3("a") {
		t.Errorf("manifest not carried through the rename: %v", m)
	}
	if got := seqFor(t, 2026); got != 1 {
		t.Errorf("seq = %d, want 1", got)
	}
}
