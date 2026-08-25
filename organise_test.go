package main

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// checksums.b3 describes the directory it sits in, so -reorganise must leave it
// where it is. With regroup=true the walk descends into existing numbered
// missions, and before the metadataFiles guard it picked each manifest up as
// footage: the plan's collision rule renamed the second one, which put it out of
// reach of the stale-manifest removal, and it became a permanent file in the new
// mission that -checksum hashed, -list counted and -sync carried to cold storage.
func TestScanUnorganisedSkipsMetadata(t *testing.T) {
	yearDir := t.TempDir()
	for _, mission := range []string{"001_Old", "002_Older"} {
		dir := filepath.Join(yearDir, mission)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		for name, body := range map[string]string{
			"A.MP4":           "footage",
			"checksums.b3":    "abc  A.MP4\n",
			proxyManifestName: "def  A.MP4\n",
			proxyMetaName:     "{}\n",
			flagsFileName:     "{}\n",
			"Thumbs.db":       "junk",
		} {
			if err := os.WriteFile(filepath.Join(dir, name), []byte(body), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}

	files, err := scanUnorganised(yearDir, true)
	if err != nil {
		t.Fatalf("scanUnorganised: %v", err)
	}
	var got []string
	for _, f := range files {
		got = append(got, f.rel)
	}
	if len(got) != 2 {
		t.Fatalf("want only the two .MP4 files, got %v", got)
	}
	for _, rel := range got {
		if filepath.Ext(rel) != ".MP4" {
			t.Errorf("scanUnorganised picked up %s; metadata must stay put", rel)
		}
	}
}

// -list and -status walk the year directory and previously took every directory
// under it for a mission, so -organise's _unsorted showed up as a mission row.
// The guard they gained has to keep 000_* visible: those are synced like any
// mission and only mission-number commands cannot address them.
func TestMissionDirPredicates(t *testing.T) {
	cases := []struct {
		name     string
		mission  bool
		numbered bool
	}{
		{"042_Altissimo_with_Anton", true, true},
		{"000_Edits", true, false},
		{"_unsorted", false, false},
		{"proxies", false, false},
		{"-1_Backwards", false, false},
		{"042", false, false},
	}
	for _, c := range cases {
		if got := isMissionDir(c.name); got != c.mission {
			t.Errorf("isMissionDir(%q) = %v, want %v", c.name, got, c.mission)
		}
		if got := isNumberedMission(c.name); got != c.numbered {
			t.Errorf("isNumberedMission(%q) = %v, want %v", c.name, got, c.numbered)
		}
	}
}

// missionDirs is the one enumeration every command that hashes, checks,
// verifies, indexes or collects flags now shares. 000_* used to be filtered out
// at each of those sites with isNumberedMission, so -sync wrote the copies and
// nothing ever checked them.
func TestMissionDirsIncludes000AndSkipsStrays(t *testing.T) {
	year := t.TempDir()
	for _, name := range []string{
		"000_Edits", "042_Altissimo_with_Anton", "007_Bond",
		"_unsorted", "proxies", "notamission",
	} {
		if err := os.MkdirAll(filepath.Join(year, name), 0777); err != nil {
			t.Fatal(err)
		}
	}
	// a file whose name would parse as a mission is still not a directory
	if err := os.WriteFile(filepath.Join(year, "099_File"), nil, 0644); err != nil {
		t.Fatal(err)
	}

	got := missionDirs(year)
	want := []string{"000_Edits", "007_Bond", "042_Altissimo_with_Anton"}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("missionDirs = %v, want %v", got, want)
	}

	if got := missionDirs(filepath.Join(year, "nosuchdir")); got != nil {
		t.Errorf("missionDirs of an unreadable directory = %v, want nil", got)
	}
}

// -organise groups what is not yet in a mission, so every mission is off limits
// to it. The predicate deciding "already a mission" was isNumberedMission, which
// requires n > 0, so a plain -organise walked into 000_Edits, dated its contents
// by mtime and planned to move them into NNN_Season — and removeEmptyDirs then
// took the emptied 000_Edits away. README.md is explicit that 000_* directories
// are missions and only unaddressable by number.
//
// -reorganise does re-bucket missions, which is what it is for, but 000_* sits
// outside the numbering by construction — a named mission rather than a season's
// worth of footage — so it is left alone by that too.
func TestScanUnorganisedLeaves000MissionsAlone(t *testing.T) {
	year := t.TempDir()
	for _, mission := range []string{"000_Edits", "042_Real"} {
		dir := filepath.Join(year, mission)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "clip.mp4"), []byte("footage"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// the loose file -organise actually exists to pick up
	if err := os.WriteFile(filepath.Join(year, "loose.mp4"), []byte("footage"), 0o644); err != nil {
		t.Fatal(err)
	}

	for _, c := range []struct {
		regroup bool
		want    []string
	}{
		{false, []string{"loose.mp4"}},
		{true, []string{"042_Real/clip.mp4", "loose.mp4"}},
	} {
		files, err := scanUnorganised(year, c.regroup)
		if err != nil {
			t.Fatalf("scanUnorganised(regroup=%v): %v", c.regroup, err)
		}
		var got []string
		for _, f := range files {
			got = append(got, filepath.ToSlash(f.rel))
		}
		sort.Strings(got)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("scanUnorganised(regroup=%v) = %v, want %v", c.regroup, got, c.want)
		}
		for _, rel := range got {
			if strings.HasPrefix(rel, "000_") {
				t.Errorf("regroup=%v took a 000_* mission apart: %s", c.regroup, rel)
			}
		}
	}
}

func TestSkipOrganise(t *testing.T) {
	cases := []struct {
		name            string
		organise, reorg bool
	}{
		{"042_Altissimo_with_Anton", true, false}, // -reorganise re-buckets it
		{"000_Edits", true, true},                 // neither touches it
		{"loose_footage", false, false},           // not a mission at all
		{"DCIM", false, false},
	}
	for _, c := range cases {
		if got := skipOrganise(c.name, false); got != c.organise {
			t.Errorf("skipOrganise(%q, regroup=false) = %v, want %v", c.name, got, c.organise)
		}
		if got := skipOrganise(c.name, true); got != c.reorg {
			t.Errorf("skipOrganise(%q, regroup=true) = %v, want %v", c.name, got, c.reorg)
		}
	}
}
