package main

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"testing"
)

// A manifest describes the directory it sits in, so it is not part of the
// mission it describes — which is why missionFiles leaves it out of everything
// -sync and -replicate plan from. -check compared with findFiles instead, so a
// cold copy that had not been checksummed yet read as a copy missing a file:
// it printed "− checksums.b3", counted it, exited 1 and told the user to run
// -sync, which never copies one. -sync said "all drives are in sync" about the
// same tree. A cold drive unmounted during -checksum is the ordinary way in.
func TestCheckIgnoresTheManifestItself(t *testing.T) {
	hot, cold := t.TempDir(), t.TempDir()
	for _, base := range []string{hot, cold} {
		writeMissionFile(t, base, "2026", "042_Test", "a.mxf", "footage")
	}
	cfg := Config{Drives: []DriveConfig{
		{Volume: "T9", Path: hot, Role: "hot"},
		{Volume: "ARCHIVE", Path: cold, Role: "cold"},
	}}

	// Neither side checksummed, then each side in turn: the mission is complete
	// in all three cases, and -check must agree with -sync about all three.
	for _, c := range []struct{ name, on string }{
		{"neither drive checksummed", ""},
		{"only the hot copy checksummed", hot},
		{"only the cold copy checksummed", cold},
	} {
		for _, base := range []string{hot, cold} {
			os.Remove(filepath.Join(base, "2026", "042_Test", "checksums.b3"))
		}
		if c.on != "" {
			writeMissionFile(t, c.on, "2026", "042_Test", "checksums.b3", "abc  a.mxf\n")
		}
		if ok := runCheck(cfg, 2026); !ok {
			t.Errorf("%s: -check calls the mission incomplete", c.name)
		}
		if ok := runCheckMission(cfg, 42, 2026, true); !ok {
			t.Errorf("%s: -check 42 calls the mission incomplete", c.name)
		}
		if ok := runSync(cfg, 2026, true); !ok {
			t.Errorf("%s: -sync reported failure", c.name)
		}
	}
}

// contentFiles is the one answer to "what is this mission" that the planner and
// the checker share. Only top-level bookkeeping is excluded, matching what the
// transfers skip; anything nested is footage until proven otherwise.
func TestContentFilesExcludesTopLevelBookkeepingOnly(t *testing.T) {
	dir := t.TempDir()
	for _, rel := range []string{
		"a.mxf", "checksums.b3", proxyManifestName, proxyMetaName, flagsFileName,
		"CARD_01/b.mxf", "CARD_01/checksums.b3",
	} {
		p := filepath.Join(dir, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	files, err := contentFiles(dir)
	if err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, f := range files {
		got = append(got, filepath.ToSlash(f.rel))
	}
	want := []string{"CARD_01/b.mxf", "CARD_01/checksums.b3", "a.mxf"}
	sort.Strings(got)
	if !reflect.DeepEqual(got, want) {
		t.Errorf("contentFiles = %v, want %v", got, want)
	}
}
