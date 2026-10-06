package main

import (
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"
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

// -organise deleted the checksums.b3 of every directory a file moved into or
// out of. A rename does not change a file's content, so the hashes recorded
// when the footage was known good were thrown away for nothing — and the next
// -checksum recorded whatever was on disk by then.
func TestOrganiseCarriesRecordedHashesWithTheFiles(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	drive := t.TempDir()
	year := filepath.Join(drive, "2026")
	cfg := Config{Drives: []DriveConfig{{Volume: "HOT", Path: drive, Role: "hot"}}}
	writeFile(t, filepath.Join(year, "003_X", "card", "clip_20260715.mp4"), "summer")
	writeFile(t, filepath.Join(year, "003_X", "card", "clip_20260110.mp4"), "winter")
	writeFile(t, filepath.Join(year, "003_X", "checksums.b3"),
		b3("summer")+"  card/clip_20260715.mp4\n"+b3("winter")+"  card/clip_20260110.mp4\n")

	runOrganise(cfg, 2026, true, true)

	summer := readChecksumFile(filepath.Join(year, "004_Summer", "checksums.b3"))
	winter := readChecksumFile(filepath.Join(year, "005_Winter", "checksums.b3"))
	if summer["clip_20260715.mp4"] != b3("summer") {
		t.Errorf("summer manifest = %v", summer)
	}
	if winter["clip_20260110.mp4"] != b3("winter") {
		t.Errorf("winter manifest = %v", winter)
	}
	if dirExists(filepath.Join(year, "003_X")) {
		t.Error("the emptied mission was left behind, held open by its manifest")
	}
}

// os.Rename replaces an existing file without a word; -organise must not.
func TestOrganiseMoveNeverReplacesAFile(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a"), "moving")
	writeFile(t, filepath.Join(dir, "b"), "already here")
	if err := moveNoReplace(filepath.Join(dir, "a"), filepath.Join(dir, "b")); err == nil {
		t.Error("moved over an existing file")
	}
	if data, _ := os.ReadFile(filepath.Join(dir, "b")); string(data) != "already here" {
		t.Errorf("existing file was replaced: %q", data)
	}
}

// Each drive was dated on its own, falling back to the copy's mtime — and a
// cold copy's mtime was when it was synced. The hot and cold copies of one clip
// then landed in different seasons, and the drives disagreed from then on.
func TestOrganiseFilesEveryCopyIntoTheSameMission(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	hot, cold := t.TempDir(), t.TempDir()
	cfg := Config{Drives: []DriveConfig{
		{Volume: "HOT", Path: hot, Role: "hot"},
		{Volume: "COLD", Path: cold, Role: "cold"},
	}}
	july := time.Date(2026, 7, 10, 12, 0, 0, 0, time.Local)
	november := time.Date(2026, 11, 2, 12, 0, 0, 0, time.Local)
	for _, c := range []struct {
		drive string
		mtime time.Time
	}{{hot, july}, {cold, november}} {
		p := filepath.Join(c.drive, "2026", "card", "clip.mp4")
		writeFile(t, p, "no date in here")
		if err := os.Chtimes(p, c.mtime, c.mtime); err != nil {
			t.Fatal(err)
		}
	}

	runOrganise(cfg, 2026, true, false)

	for _, d := range []string{hot, cold} {
		if !exists(filepath.Join(d, "2026", "001_Summer", "clip.mp4")) {
			entries, _ := os.ReadDir(filepath.Join(d, "2026"))
			var names []string
			for _, e := range entries {
				names = append(names, e.Name())
			}
			t.Errorf("%s: clip not filed under 001_Summer; year holds %v", d, names)
		}
	}
}
