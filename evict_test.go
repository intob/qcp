package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

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
