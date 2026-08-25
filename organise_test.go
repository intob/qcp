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
