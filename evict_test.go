package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// writeMissionFile puts one file in a mission directory on a drive.
func writeMissionFile(t *testing.T, base, year, slug, name, body string) {
	t.Helper()
	dir := filepath.Join(base, year, slug)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
		t.Fatal(err)
	}
}

// -evict is the only command that deletes footage, and the manifest cross-check
// is what ties the cold bytes to the hot ones. readChecksumFile returns an empty
// map for a manifest that is missing or unparseable and manifestConflicts walks
// the hot side, so a hot copy with no checksums.b3 used to report zero conflicts
// — "I could not compare" passing as "they agree" — and the cold copy qualified.
func TestQualifyBackupsNeedsHotManifest(t *testing.T) {
	hot, cold := t.TempDir(), t.TempDir()
	const (
		year = "2026"
		slug = "042_Test"
	)

	write := func(base, name, body string) {
		dir := filepath.Join(base, year, slug)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	// The cold copy is beyond reproach: the file is there and the manifest
	// covers it. Only the hot manifest is in question.
	write(hot, "A.MP4", "footage")
	write(cold, "A.MP4", "footage")
	write(cold, "checksums.b3", "abc  A.MP4\n")

	cfg := Config{Drives: []DriveConfig{
		{Volume: "HOT", Path: hot, Role: "hot"},
		{Volume: "COLD", Path: cold, Role: "cold"},
	}}
	hotDir := filepath.Join(hot, year, slug)
	files, _, _, err := missionFiles(hotDir)
	if err != nil {
		t.Fatal(err)
	}
	targets := []evictTarget{{vol: "HOT", dir: hotDir, files: files}}

	backups, notes := qualifyBackups(cfg, year, slug, 42, targets, 1)
	if len(backups) > 0 {
		t.Fatalf("qualified %d cold copy/copies with no hot manifest to compare against", len(backups))
	}
	if len(notes) == 0 || !strings.Contains(notes[0], "HOT") {
		t.Fatalf("want a note naming the hot drive, got %v", notes)
	}

	// With a hot manifest that agrees, the same cold copy must still qualify.
	write(hot, "checksums.b3", "abc  A.MP4\n")
	if backups, notes = qualifyBackups(cfg, year, slug, 42, targets, 1); len(backups) != 1 {
		t.Fatalf("want the cold copy to qualify once both manifests agree, got %v", notes)
	}

	// ...and must not once they disagree.
	write(hot, "checksums.b3", "def  A.MP4\n")
	if backups, notes = qualifyBackups(cfg, year, slug, 42, targets, 1); len(backups) != 0 {
		t.Fatalf("qualified a cold copy whose manifest disagrees with the hot one")
	}
	if len(notes) == 0 || !strings.Contains(notes[0], "different hashes") {
		t.Fatalf("want a conflict note, got %v", notes)
	}
}

// -evict proves every *file* survives on cold and then removes the whole hot
// directory — which took .qcp-flags.json with it. Flags are deliberately never
// synced, so there was nothing to bring them back from: the one piece of state
// in qcp a person creates rather than derives, destroyed by the one command
// that deletes, with no warning and no way back.
//
// They are carried to the cold copies that justify the deletion instead. A
// dotfile is invisible to findFiles, checksums.b3, -verify and -check, so this
// cannot make an archive look out of date, and flagStore reads every mounted
// drive holding the mission, so -serve and -resolve pick them up from there.
func TestEvictCarriesFlagsToTheColdCopy(t *testing.T) {
	hot, cold := t.TempDir(), t.TempDir()
	const (
		year = "2026"
		slug = "042_Test"
	)
	for _, base := range []string{hot, cold} {
		writeMissionFile(t, base, year, slug, "A.MP4", "footage")
		sum, err := hashFile(filepath.Join(base, year, slug, "A.MP4"), nil)
		if err != nil {
			t.Fatal(err)
		}
		writeMissionFile(t, base, year, slug, "checksums.b3", sum+"  A.MP4\n")
	}
	hotDir := filepath.Join(hot, year, slug)
	coldDir := filepath.Join(cold, year, slug)

	// one flag set on the hot copy, and an older one already on the cold copy
	if err := writeMissionFlags(hotDir, missionFlags{Version: 1, Flags: map[string]clipFlag{
		"A.MP4": {Colour: flagColour, At: "2026-08-25T12:00:00Z"},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := writeMissionFlags(coldDir, missionFlags{Version: 1, Flags: map[string]clipFlag{
		"B.MP4": {Colour: flagColour, At: "2026-01-01T00:00:00Z"},
	}}); err != nil {
		t.Fatal(err)
	}

	cfg := Config{Drives: []DriveConfig{
		{Volume: "HOT", Path: hot, Role: "hot"},
		{Volume: "COLD", Path: cold, Role: "cold"},
	}}
	runEvict(cfg, []int{42}, 2026, nil, 1, false, true)

	if dirExists(hotDir) {
		t.Fatalf("the hot copy was not evicted")
	}
	got, err := readMissionFlags(coldDir)
	if err != nil {
		t.Fatal(err)
	}
	if got.Flags["A.MP4"].Colour != flagColour {
		t.Errorf("the flag on the evicted hot copy did not reach the cold one: %v", got.Flags)
	}
	if got.Flags["B.MP4"].Colour != flagColour {
		t.Errorf("carrying the flags over dropped one the cold copy already had: %v", got.Flags)
	}

	// and the merged set is what the store reads back, from cold alone
	store := &flagStore{drives: cfg.Drives}
	merged, err := store.read(2026, slug)
	if err != nil {
		t.Fatal(err)
	}
	if len(merged.Flags) != 2 {
		t.Errorf("flagStore.read after eviction = %v, want both flags", merged.Flags)
	}
}

// Two configured drives can resolve to one folder — here the cold entry is a
// symlink to the hot drive. The "cold copy" was then the hot copy itself: it
// had every file, its manifest agreed with itself, it verified, and -evict
// deleted the only copy it had just declared safe.
func TestQualifyBackupsRefusesTheHotCopyItself(t *testing.T) {
	root := t.TempDir()
	hot := filepath.Join(root, "hot")
	cold := filepath.Join(root, "cold")
	writeMissionFile(t, hot, "2026", "001_A", "a.mp4", "only copy")
	writeMissionFile(t, hot, "2026", "001_A", "checksums.b3", b3("only copy")+"  a.mp4\n")
	if err := os.Symlink(hot, cold); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Drives: []DriveConfig{
		{Volume: "HOT", Path: hot, Role: "hot"},
		{Volume: "COLD", Path: cold, Role: "cold"},
	}}
	dir := filepath.Join(hot, "2026", "001_A")
	files, _, _, err := missionFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	targets := []evictTarget{{"HOT", dir, files, 9}}

	backups, problems := qualifyBackups(cfg, "2026", "001_A", 1, targets, 1)

	if len(backups) != 0 {
		t.Fatalf("the hot copy qualified as its own backup")
	}
	if !strings.Contains(strings.Join(problems, "\n"), "same directory") {
		t.Errorf("problems = %v", problems)
	}
}

// Two entries for one cold folder must not count as two copies.
func TestQualifyBackupsCountsOneFolderOnce(t *testing.T) {
	root := t.TempDir()
	hot := filepath.Join(root, "hot")
	cold := filepath.Join(root, "cold")
	writeMissionFile(t, hot, "2026", "001_A", "a.mp4", "x")
	writeMissionFile(t, hot, "2026", "001_A", "checksums.b3", b3("x")+"  a.mp4\n")
	writeMissionFile(t, cold, "2026", "001_A", "a.mp4", "x")
	writeMissionFile(t, cold, "2026", "001_A", "checksums.b3", b3("x")+"  a.mp4\n")
	if err := os.Symlink(cold, filepath.Join(root, "cold2")); err != nil {
		t.Fatal(err)
	}
	cfg := Config{Drives: []DriveConfig{
		{Volume: "HOT", Path: hot, Role: "hot"},
		{Volume: "COLD", Path: cold, Role: "cold"},
		{Volume: "COLD2", Path: filepath.Join(root, "cold2"), Role: "cold"},
	}}
	dir := filepath.Join(hot, "2026", "001_A")
	files, _, _, _ := missionFiles(dir)

	backups, _ := qualifyBackups(cfg, "2026", "001_A", 1, []evictTarget{{"HOT", dir, files, 1}}, 2)

	if len(backups) != 0 {
		t.Errorf("one cold folder counted as %d copies", len(backups))
	}
}
