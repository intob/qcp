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
