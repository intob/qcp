package main

import (
	"os"
	"path/filepath"
	"testing"
)

// A flag has to be invisible to the integrity machinery: findFiles is what
// builds checksums.b3 and what -sync, -replicate and -pull walk, so if it ever
// stops skipping dotfiles a flag would start reading as an extra file and make
// a good mission look wrong.
func TestFlagsFileIsInvisibleToTheManifestWalk(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "923_0322.MXF"), []byte("footage"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := writeMissionFlags(dir, missionFlags{Flags: map[string]clipFlag{
		"923_0322.MXF": {Colour: "Blue", At: "2026-08-24T12:00:00Z"},
	}}); err != nil {
		t.Fatal(err)
	}
	files, err := findFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if f.rel == flagsFileName {
			t.Fatalf("%s is visible to findFiles — it would be checksummed and synced", flagsFileName)
		}
	}
	if len(files) != 1 {
		t.Errorf("expected only the footage file, got %d: %v", len(files), files)
	}
}

// Unflagging the last clip should leave nothing behind rather than an empty
// file that later reads as state.
func TestUnflaggingEverythingRemovesTheFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, flagsFileName)
	if err := writeMissionFlags(dir, missionFlags{Flags: map[string]clipFlag{
		"a.MXF": {Colour: "Blue", At: "2026-08-24T12:00:00Z"},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("flags file was not written: %v", err)
	}
	if err := writeMissionFlags(dir, missionFlags{Flags: map[string]clipFlag{}}); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("flags file survived unflagging everything")
	}
	// And a missing file reads as no flags rather than an error.
	f, err := readMissionFlags(dir)
	if err != nil {
		t.Fatalf("reading a missing flags file: %v", err)
	}
	if len(f.Flags) != 0 {
		t.Errorf("expected no flags, got %v", f.Flags)
	}
}

// Drives drift apart whenever one was unmounted during an edit, so a merge
// takes the union and lets the newest timestamp settle a disagreement.
func TestMergeTakesTheUnionAndNewestWins(t *testing.T) {
	a := missionFlags{Flags: map[string]clipFlag{
		"a.MXF": {Colour: "Blue", At: "2026-08-24T10:00:00Z"},
		"b.MXF": {Colour: "Blue", At: "2026-08-24T10:00:00Z"},
	}}
	b := missionFlags{Flags: map[string]clipFlag{
		"b.MXF": {Colour: "Green", At: "2026-08-24T12:00:00Z"}, // newer
		"c.MXF": {Colour: "Blue", At: "2026-08-24T09:00:00Z"},
	}}
	got := mergeMissionFlags([]missionFlags{a, b})
	if len(got.Flags) != 3 {
		t.Errorf("expected the union of both drives, got %d: %v", len(got.Flags), got.Flags)
	}
	if got.Flags["b.MXF"].Colour != "Green" {
		t.Errorf("older entry won the conflict: %v", got.Flags["b.MXF"])
	}
	if got.Flags["a.MXF"].Colour != "Blue" || got.Flags["c.MXF"].Colour != "Blue" {
		t.Errorf("an entry present on only one drive was dropped: %v", got.Flags)
	}
}

// The key is what the page, the API and the drives all agree on.
func TestFlagKeyMatchesTheIndexPathShape(t *testing.T) {
	if got := flagKey(2026, "024_Jamie", "CFEXP/923_0322.MXF"); got != "2026/024_Jamie/CFEXP/923_0322.MXF" {
		t.Errorf("flagKey = %q", got)
	}
}

// The dangerous path: a flags file that will not parse must not be treated as
// "nothing flagged", because every write starts from the read. If it were,
// one corrupt file plus one click would silently discard every other flag in
// the mission.
func TestUnreadableFlagsAreNeverOverwritten(t *testing.T) {
	root := t.TempDir()
	const year, slug = 2026, "024_Jamie"
	dir := filepath.Join(root, "2026", slug)
	if err := os.MkdirAll(dir, 0755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, flagsFileName)
	if err := os.WriteFile(path, []byte("{ this is not json"), 0644); err != nil {
		t.Fatal(err)
	}
	s := &flagStore{drives: []DriveConfig{{Path: root, Role: "hot"}}}

	if _, err := s.read(year, slug); err == nil {
		t.Fatal("read reported success on a corrupt flags file")
	}
	if _, err := s.set(year, slug, "a.MXF", true); err == nil {
		t.Fatal("set overwrote a flags file it could not read")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(raw) != "{ this is not json" {
		t.Errorf("the unreadable file was modified: %q", raw)
	}
}
