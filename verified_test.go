package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// stampAt writes a verified stamp for dir's current manifest, as if -verify had
// passed it at t.
func stampAt(t *testing.T, dir string, at time.Time) {
	t.Helper()
	vc := planVerifyCopy(dir, 1)
	if vc.problem != "" {
		t.Fatal(vc.problem)
	}
	recordVerified("TEST", dir, vc, at)
	if got := lastVerified(dir); !got.Equal(at.UTC()) {
		t.Fatalf("stamp at %v reads back as %v", at, got)
	}
}

// A copy that passes is stamped; a copy that fails beside it is not, and an
// unstamped copy reads as never verified.
func TestVerifyStampsOnlyTheCopiesThatPassed(t *testing.T) {
	for _, mode := range []string{"mission", "year"} {
		t.Run(mode, func(t *testing.T) {
			cfg, one, two := verifyFixture(t)
			writeFile(t, filepath.Join(two, "a.mp4"), "rot!")
			if !lastVerified(one).IsZero() {
				t.Fatal("a copy never verified has a date")
			}

			before := time.Now().Add(-time.Second)
			captureStdout(t, func() {
				if mode == "mission" {
					runVerify(cfg, 1, 2026)
				} else {
					runVerifyYear(cfg, 2026)
				}
			})

			if got := lastVerified(one); got.Before(before) {
				t.Errorf("the good copy was not stamped: %v", got)
			}
			if got := lastVerified(two); !got.IsZero() {
				t.Errorf("the corrupt copy was stamped %v", got)
			}
		})
	}
}

// A copy with a file its manifest does not record has not been verified in
// full, so it is not stamped even though every recorded file matched.
func TestVerifyDoesNotStampACopyWithUnrecordedFiles(t *testing.T) {
	cfg, one, _ := verifyFixture(t)
	writeFile(t, filepath.Join(one, "b.mp4"), "never recorded")

	captureStdout(t, func() { runVerify(cfg, 1, 2026) })

	if got := lastVerified(one); !got.IsZero() {
		t.Errorf("stamped a copy with an unrecorded file: %v", got)
	}
}

// The stamp vouches for the manifest it verified. Once checksums.b3 changes —
// an append, a sync filling a gap — it no longer counts.
func TestAChangedManifestVoidsTheStamp(t *testing.T) {
	_, one, _ := verifyFixture(t)
	stampAt(t, one, time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC))

	writeFile(t, filepath.Join(one, "b.mp4"), "appended")
	if err := addChecksums(filepath.Join(one, "checksums.b3"), []string{b3("appended") + "  b.mp4"}); err != nil {
		t.Fatal(err)
	}

	if got := lastVerified(one); !got.IsZero() {
		t.Errorf("stamp survived a manifest change: %v", got)
	}
}

// The stamp is bookkeeping, not footage: no transfer may carry it and no
// manifest may record it.
func TestTheStampIsInvisibleToTheManifestWalk(t *testing.T) {
	_, one, _ := verifyFixture(t)
	stampAt(t, one, time.Now())
	files, err := findFiles(one)
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range files {
		if f.rel == verifiedFileName {
			t.Fatalf("%s is visible to findFiles — it would be checksummed and synced", verifiedFileName)
		}
	}
}

// -verify oldest works through copies never verified first, then the longest
// unchecked, and with a budget already spent still verifies one copy, so a
// short run always makes progress.
func TestVerifyOldestTakesTheLongestUncheckedFirst(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	drive := t.TempDir()
	cfg := Config{Drives: []DriveConfig{{Volume: "ARCHIVE", Path: drive, Role: "cold"}}}
	mission := func(year, slug string) string {
		dir := filepath.Join(drive, year, slug)
		writeFile(t, filepath.Join(dir, "a.mp4"), slug)
		writeFile(t, filepath.Join(dir, "checksums.b3"), b3(slug)+"  a.mp4\n")
		return dir
	}
	recent := mission("2025", "001_Recent")
	old := mission("2026", "002_Old")
	never := mission("2026", "003_Never")
	stampAt(t, recent, time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC))
	stampAt(t, old, time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC))

	queue, _ := scrubQueue(cfg, nil)
	var order []string
	for _, c := range queue {
		order = append(order, c.slug)
	}
	if strings.Join(order, " ") != "003_Never 002_Old 001_Recent" {
		t.Fatalf("queue = %v, want never verified, then oldest first", order)
	}

	var ok bool
	out := captureStdout(t, func() { ok = runVerifyOldest(cfg, nil, time.Nanosecond) })
	if !ok {
		t.Errorf("run failed:\n%s", out)
	}
	if lastVerified(never).IsZero() {
		t.Error("the copy never verified was not taken first")
	}
	if got := lastVerified(old); !got.Equal(time.Date(2025, 2, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("went past a spent budget: the next copy was re-stamped %v", got)
	}
	if !strings.Contains(out, "next: 2026/002_Old on ARCHIVE") {
		t.Errorf("did not say what is next:\n%s", out)
	}

	// -year narrows the queue.
	queue, _ = scrubQueue(cfg, []int{2025})
	if len(queue) != 1 || queue[0].slug != "001_Recent" {
		t.Errorf("queue for 2025 = %v", queue)
	}
}

// The catalog keeps the date so a drive in a drawer can still say how long it
// has gone unchecked.
func TestTheCatalogRemembersWhenACopyWasVerified(t *testing.T) {
	cfg, one, _ := verifyFixture(t)
	at := time.Date(2026, 3, 1, 12, 0, 0, 0, time.UTC)
	stampAt(t, one, at)

	refreshCatalog(cfg, []int{2026})

	cy := readCatalog("ONE").Years["2026"]
	if got := cy.Missions["001_A"].Verified; !got.Equal(at) {
		t.Errorf("catalog verified = %v, want %v", got, at)
	}
	if s := cy.verified(); s.verified != 1 || s.missions != 1 {
		t.Errorf("summary = %+v", s)
	}
	if got := readCatalog("TWO").Years["2026"].verified().String(); got != "never verified" {
		t.Errorf("unverified drive summary = %q", got)
	}
}

// -organise rewrites the manifest of every directory it moves files out of. The
// stamp there no longer counts, and left behind it would keep the emptied
// mission's directory from being removed.
func TestOrganiseClearsTheStampOfAMissionItEmpties(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	drive := t.TempDir()
	year := filepath.Join(drive, "2026")
	cfg := Config{Drives: []DriveConfig{{Volume: "HOT", Path: drive, Role: "hot"}}}
	writeFile(t, filepath.Join(year, "003_X", "card", "clip_20260715.mp4"), "summer")
	writeFile(t, filepath.Join(year, "003_X", "checksums.b3"), b3("summer")+"  card/clip_20260715.mp4\n")
	stampAt(t, filepath.Join(year, "003_X"), time.Now())

	runOrganise(cfg, 2026, true, true)

	if dirExists(filepath.Join(year, "003_X")) {
		entries, _ := os.ReadDir(filepath.Join(year, "003_X"))
		t.Errorf("the emptied mission was left behind, holding %v", entries)
	}
}

// A stamp from a future format, or one that does not parse, is never trusted.
func TestAnUnreadableStampCountsAsNeverVerified(t *testing.T) {
	_, one, _ := verifyFixture(t)
	digest, _ := manifestDigest(one)
	for _, raw := range []string{"{ not json", mustJSON(t, verifiedStamp{Version: 2, Verified: time.Now(), Manifest: digest})} {
		writeFile(t, filepath.Join(one, verifiedFileName), raw)
		if got := lastVerified(one); !got.IsZero() {
			t.Errorf("%q read as verified %v", raw, got)
		}
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// -checksum reads every file of each copy it hashes, and writes the manifest
// only when they agree with every other copy and every recorded hash, which is
// a full verification. Those copies are stamped; one it skipped as already
// checksummed was not read and is not; and nothing is stamped on a conflict.
func TestChecksumStampsTheCopiesItHashed(t *testing.T) {
	for _, mode := range []string{"mission", "year"} {
		t.Run(mode, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			a, b, done := t.TempDir(), t.TempDir(), t.TempDir()
			cfg := Config{Drives: []DriveConfig{
				{Volume: "A", Path: a, Role: "hot"},
				{Volume: "B", Path: b, Role: "cold"},
				{Volume: "DONE", Path: done, Role: "cold"},
			}}
			mission := func(drive string) string { return filepath.Join(drive, "2026", "001_A") }
			for _, d := range []string{a, b, done} {
				writeFile(t, filepath.Join(mission(d), "CARD", "clip.mp4"), "clip")
			}
			writeFile(t, filepath.Join(mission(done), "checksums.b3"), b3("clip")+"  CARD/clip.mp4\n")

			run := func() bool {
				if mode == "mission" {
					return runChecksum(cfg, 1, 2026)
				}
				return runChecksumYear(cfg, 2026)
			}
			var ok bool
			captureStdout(t, func() { ok = run() })
			if !ok {
				t.Fatal("checksum failed")
			}
			for _, d := range []string{a, b} {
				if lastVerified(mission(d)).IsZero() {
					t.Errorf("%s was hashed in full but not stamped", d)
				}
			}
			if got := lastVerified(mission(done)); !got.IsZero() {
				t.Errorf("a copy skipped as already checksummed was stamped %v", got)
			}
		})
	}

	t.Run("conflict", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		a, b := t.TempDir(), t.TempDir()
		cfg := Config{Drives: []DriveConfig{{Volume: "A", Path: a, Role: "hot"}, {Volume: "B", Path: b, Role: "cold"}}}
		writeFile(t, filepath.Join(a, "2026", "001_A", "clip.mp4"), "clip")
		writeFile(t, filepath.Join(b, "2026", "001_A", "clip.mp4"), "clip, differently")
		captureStdout(t, func() { runChecksum(cfg, 1, 2026) })
		for _, d := range []string{a, b} {
			if raw, err := os.ReadFile(filepath.Join(d, "2026", "001_A", verifiedFileName)); err == nil {
				t.Errorf("stamped despite a conflict: %s", raw)
			}
		}
	})
}

// -evict relies on a cold copy verified within the last week instead of
// re-reading it, but only for a stamp of the very manifest the copy qualified
// with. Here the cold file has rotted since it was stamped, so a re-read would
// fail: a trusted stamp passes without reading, and an old stamp or one for a
// different manifest is read and fails.
func TestEvictTrustsARecentStampOnlyForTheSameManifest(t *testing.T) {
	now := time.Now()
	for _, c := range []struct {
		name    string
		at      time.Time
		digest  func(real string) string
		trusted bool
	}{
		{"recent", now.Add(-48 * time.Hour), func(d string) string { return d }, true},
		{"too old", now.Add(-evictTrustsVerifiedFor - time.Hour), func(d string) string { return d }, false},
		{"other manifest", now.Add(-time.Hour), func(string) string { return b3("some other manifest") }, false},
		{"from the future", now.Add(time.Hour), func(d string) string { return d }, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			root := t.TempDir()
			hot, cold := filepath.Join(root, "hot"), filepath.Join(root, "cold")
			cfg := Config{Drives: []DriveConfig{{Volume: "HOT", Path: hot, Role: "hot"}, {Volume: "COLD", Path: cold, Role: "cold"}}}
			for _, d := range []string{hot, cold} {
				writeMissionFile(t, d, "2026", "001_A", "a.mp4", "clip")
				writeMissionFile(t, d, "2026", "001_A", "checksums.b3", b3("clip")+"  a.mp4\n")
			}
			coldDir := filepath.Join(cold, "2026", "001_A")
			digest, _ := manifestDigest(coldDir)
			stampVerified("COLD", coldDir, c.digest(digest), 1, c.at)
			writeMissionFile(t, cold, "2026", "001_A", "a.mp4", "rot!") // same size, rotted since

			hotDir := filepath.Join(hot, "2026", "001_A")
			files, _, _, err := missionFiles(hotDir)
			if err != nil {
				t.Fatal(err)
			}
			backups, problems := qualifyBackups(cfg, "2026", "001_A", 1, []evictTarget{{"HOT", hotDir, files, 4}}, 1)
			if len(problems) > 0 {
				t.Fatal(problems)
			}

			var ok bool
			out := captureStdout(t, func() { ok = verifyBackups([]evictPlan{{num: 1, slug: "001_A", backups: backups}}) })

			if ok != c.trusted {
				t.Errorf("verifyBackups = %v, want %v (trusted the stamp: %v)\n%s", ok, c.trusted, c.trusted, out)
			}
			if c.trusted && !strings.Contains(out, "not re-read") {
				t.Errorf("did not say the copy was not re-read:\n%s", out)
			}
		})
	}
}
