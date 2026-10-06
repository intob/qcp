package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// catalogFixture is a mounted hot drive and an archive that is configured but
// not mounted, with HOME pointed into the test so the catalog is the test's own.
func catalogFixture(t *testing.T) (cfg Config, hot, archive string) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	hot = t.TempDir()
	archive = filepath.Join(t.TempDir(), "absent")
	cfg = Config{Drives: []DriveConfig{
		{Volume: "HOT", Path: hot, Role: "hot"},
		{Volume: "ARCHIVE", Path: archive, Role: "cold"},
	}}
	return cfg, hot, archive
}

// seeArchive catalogues the archive as if it had been mounted holding slugs.
func seeArchive(t *testing.T, cfg Config, archive string, slugs ...string) {
	t.Helper()
	for _, s := range slugs {
		writeFile(t, filepath.Join(archive, "2026", s, "clip.mp4"), s)
	}
	refreshCatalog(cfg, []int{2026})
	if err := os.RemoveAll(archive); err != nil { // back in the drawer
		t.Fatal(err)
	}
}

func TestCatalogRemembersAnUnmountedDrive(t *testing.T) {
	cfg, hot, archive := catalogFixture(t)
	writeFile(t, filepath.Join(hot, "2026", "001_A", "a.mp4"), "aa")
	writeFile(t, filepath.Join(hot, "2026", "001_A", "checksums.b3"), b3("aa")+"  a.mp4\n")
	seeArchive(t, cfg, archive, "001_A", "002_B")

	refreshCatalog(cfg, []int{2026}) // archive away: its entry must survive

	a := readCatalog("ARCHIVE").Years["2026"]
	if len(a.Missions) != 2 || a.maxMission() != 2 {
		t.Errorf("archive as last seen = %+v", a)
	}
	h := readCatalog("HOT").Years["2026"].Missions["001_A"]
	if h.Size != 2 || !h.Checksummed || h.Files["a.mp4"] != 2 {
		t.Errorf("hot mission = %+v", h)
	}
	if _, ok := h.Files["checksums.b3"]; ok {
		t.Error("the manifest was catalogued as content")
	}
}

// A mounted drive is the truth: what is gone from it is gone from the catalog.
// With the whole drive scanned, a year it no longer holds is recorded empty
// rather than remembered.
func TestCatalogForgetsWhatAMountedDriveNoLongerHolds(t *testing.T) {
	cfg, hot, _ := catalogFixture(t)
	writeFile(t, filepath.Join(hot, "2025", "009_Old", "a.mp4"), "a")
	writeFile(t, filepath.Join(hot, "2026", "001_A", "a.mp4"), "a")
	refreshCatalog(cfg, nil)

	os.RemoveAll(filepath.Join(hot, "2025")) // evicted
	os.RemoveAll(filepath.Join(hot, "2026", "001_A"))
	refreshCatalog(cfg, nil)

	c := readCatalog("HOT")
	if n := len(c.Years["2025"].Missions); n != 0 {
		t.Errorf("2025 still lists %d mission(s)", n)
	}
	if n := len(c.Years["2026"].Missions); n != 0 {
		t.Errorf("2026 still lists %d mission(s)", n)
	}
}

// The catalog is a cache: a damaged entry is dropped and rebuilt, never fatal.
func TestCatalogRebuildsADamagedEntry(t *testing.T) {
	cfg, hot, _ := catalogFixture(t)
	writeFile(t, filepath.Join(hot, "2026", "001_A", "a.mp4"), "a")
	p, _ := catalogPath("HOT")
	writeFile(t, p, "{ not json")

	if n := len(readCatalog("HOT").Years); n != 0 {
		t.Fatalf("damaged entry read as %d year(s)", n)
	}
	refreshCatalog(cfg, []int{2026})
	if _, ok := readCatalog("HOT").Years["2026"].Missions["001_A"]; !ok {
		t.Error("entry not rebuilt")
	}
}

// The counter check used to see only what was mounted. A number the archive
// held when it was last seen is taken even with the archive in a drawer, and
// raising the counter is safe on any evidence.
func TestCounterRisesToWhatTheArchiveHeld(t *testing.T) {
	cfg, hot, archive := catalogFixture(t)
	os.MkdirAll(filepath.Join(hot, "2026", "040_Hot"), 0o755)
	seeArchive(t, cfg, archive, "045_X", "046_Y")
	writeSeq(map[int]int{2026: 44})

	checkMissionCounter(cfg, 2026, true)

	if got := seqFor(t, 2026); got != 46 {
		t.Errorf("seq = %d, want 46", got)
	}
}

// A counter ahead of the drives, with the archive away but catalogued, is
// reported — but never moved back on the catalog's word, which could reuse a
// number added to the archive since it was last seen here.
func TestCounterIsNotRewoundOnTheCatalogsWord(t *testing.T) {
	cfg, hot, archive := catalogFixture(t)
	os.MkdirAll(filepath.Join(hot, "2026", "044_Hot"), 0o755)
	seeArchive(t, cfg, archive, "044_Hot")
	writeSeq(map[int]int{2026: 46})

	st, err := readCounterState(cfg, 2026)
	if err != nil {
		t.Fatal(err)
	}
	if !st.allKnown || st.allMounted || st.onDrives != 44 {
		t.Errorf("state = %+v", st)
	}
	checkMissionCounter(cfg, 2026, false)
	if got := seqFor(t, 2026); got != 46 {
		t.Errorf("seq = %d, want 46", got)
	}
}

// Footage evicted to the archive is the likeliest to be ingested twice, and
// the archive is the drive least likely to be plugged in.
func TestDuplicateIngestSeesTheArchive(t *testing.T) {
	cfg, _, archive := catalogFixture(t)
	seeArchive(t, cfg, archive, "012_Evicted")
	card := scannedCard{files: []fileEntry{{rel: "clip.mp4", size: 1}}}

	hits := checkDuplicateIngest(cfg.Drives, "2026", []scannedCard{card})

	if hits["012_Evicted"] != 1 {
		t.Errorf("hits = %v", hits)
	}
}

func TestListMarksTheCatalogAsAMemory(t *testing.T) {
	sc := missionScan{checksummed: true}
	if got := listMarker(sc, true, false); got != dim("✓") {
		t.Errorf("catalogued and checksummed = %q", got)
	}
	if got := listMarker(sc, true, true); got != green("✓") {
		t.Errorf("mounted and checksummed = %q", got)
	}
	away := map[string]catalogYear{"ARCHIVE": {Scanned: time.Now()}}
	if note := catalogNote(away, []string{"HOT", "ARCHIVE"}); !strings.Contains(note, "ARCHIVE as last seen") {
		t.Errorf("note = %q", note)
	}
}

// The catalog is refreshed by an exit hook, and commands end through exit and
// quit — on failure as much as success, which is when the drives have changed
// in a way nothing planned. The hooks must run, and run once.
func TestExitRunsTheHooksOnce(t *testing.T) {
	dir := fixtureDir(t)
	code, out := inSubprocess(t, dir, func() {
		onExit(func() {
			f, _ := os.OpenFile(filepath.Join(dir, "ran"), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
			f.WriteString("x")
			f.Close()
		})
		runExitHooks()
		exit(3, "failing")
	})
	if code != 3 {
		t.Errorf("exit code %d\n%s", code, out)
	}
	if data, _ := os.ReadFile(filepath.Join(dir, "ran")); string(data) != "x" {
		t.Errorf("hooks ran %d time(s), want 1", len(data))
	}
}

// -init rewound to what the mounted drives showed when given -year, even with
// a drive away that the catalog knew held higher numbers.
func TestInitNeverRewindsBelowWhatTheCatalogSaw(t *testing.T) {
	cfg, hot, archive := catalogFixture(t)
	os.MkdirAll(filepath.Join(hot, "2026", "030_Recent"), 0o755)
	seeArchive(t, cfg, archive, "042_Archived")

	writeSeq(map[int]int{2026: 50})
	captureStdout(t, func() { runInit(cfg, 2026, true, true) })
	if got := seqFor(t, 2026); got != 42 {
		t.Errorf("rewound to %d, want 42 — the archive held 042", got)
	}

	writeSeq(map[int]int{2026: 7})
	captureStdout(t, func() { runInit(cfg, 2026, true, false) })
	if got := seqFor(t, 2026); got != 42 {
		t.Errorf("raised to %d, want 42", got)
	}
}

// "Not found" now says where the mission went.
func TestMissionNotFoundNamesTheDriveItIsOn(t *testing.T) {
	cfg, _, archive := catalogFixture(t)
	seeArchive(t, cfg, archive, "042_Archived")
	_, err := findMissionSlug(cfg.Drives, "2026", 42)
	if err == nil || !strings.Contains(err.Error(), "042_Archived is on ARCHIVE, which is not mounted") {
		t.Errorf("err = %v", err)
	}
	if _, err := findMissionSlug(cfg.Drives, "2026", 43); err == nil || strings.Contains(err.Error(), "ARCHIVE") {
		t.Errorf("err for a mission nowhere = %v", err)
	}
}
