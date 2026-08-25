package main

import (
	"os"
	"path/filepath"
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
