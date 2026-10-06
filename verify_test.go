package main

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// captureStdout runs f and returns what it printed.
func captureStdout(t *testing.T, f func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	old := os.Stdout
	os.Stdout = w
	done := make(chan string)
	go func() {
		b, _ := io.ReadAll(r)
		done <- string(b)
	}()
	defer func() { os.Stdout = old }()
	f()
	w.Close()
	os.Stdout = old
	return <-done
}

// verifyFixture is two drives each holding mission 001_A with a.mp4 recorded.
func verifyFixture(t *testing.T) (cfg Config, one, two string) {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	d1, d2 := t.TempDir(), t.TempDir()
	cfg = Config{Drives: []DriveConfig{
		{Volume: "ONE", Path: d1, Role: "hot"},
		{Volume: "TWO", Path: d2, Role: "cold"},
	}}
	one, two = filepath.Join(d1, "2026", "001_A"), filepath.Join(d2, "2026", "001_A")
	for _, dir := range []string{one, two} {
		writeFile(t, filepath.Join(dir, "a.mp4"), "good")
		writeFile(t, filepath.Join(dir, "checksums.b3"), b3("good")+"  a.mp4\n")
	}
	return cfg, one, two
}

// The FAIL line printed the mission's directory name where the drive belonged,
// so with two copies there was no telling which one was bad.
func TestVerifyNamesTheDriveThatFailed(t *testing.T) {
	cfg, _, two := verifyFixture(t)
	writeFile(t, filepath.Join(two, "a.mp4"), "rot!")

	var ok bool
	out := captureStdout(t, func() { ok = runVerify(cfg, 1, 2026) })

	if ok {
		t.Error("passed with a corrupt copy")
	}
	if !strings.Contains(out, "[TWO] a.mp4") {
		t.Errorf("failure does not name the drive:\n%s", out)
	}
}

// A file on disk that checksums.b3 does not record was never looked at, and the
// run still said "all N files ok".
func TestVerifyFailsOnAnUnrecordedFile(t *testing.T) {
	cfg, one, _ := verifyFixture(t)
	writeFile(t, filepath.Join(one, "appended.mp4"), "never recorded")

	var ok bool
	out := captureStdout(t, func() { ok = runVerify(cfg, 1, 2026) })

	if ok {
		t.Errorf("passed with an unrecorded file:\n%s", out)
	}
	if !strings.Contains(out, "appended.mp4") {
		t.Errorf("the unrecorded file is not named:\n%s", out)
	}
}

// A copy with no checksums.b3 was skipped with a warning, and the mission
// passed on the other copy alone.
func TestVerifyFailsOnACopyWithNoManifest(t *testing.T) {
	cfg, _, two := verifyFixture(t)
	os.Remove(filepath.Join(two, "checksums.b3"))

	var ok bool
	captureStdout(t, func() { ok = runVerify(cfg, 1, 2026) })

	if ok {
		t.Error("passed with a copy that could not be verified")
	}
}

// -verify all passed a mission it could not verify at all, where -verify 1 on
// the same mission failed.
func TestVerifyAllFailsAMissionItCannotVerify(t *testing.T) {
	cfg, one, two := verifyFixture(t)
	os.Remove(filepath.Join(one, "checksums.b3"))
	os.Remove(filepath.Join(two, "checksums.b3"))

	var ok bool
	captureStdout(t, func() { ok = runVerifyYear(cfg, 2026) })

	if ok {
		t.Error("-verify all passed a mission with no manifest anywhere")
	}
}

// The plain case still passes.
func TestVerifyPassesAGoodMission(t *testing.T) {
	cfg, _, _ := verifyFixture(t)
	var one, all bool
	captureStdout(t, func() { one, all = runVerify(cfg, 1, 2026), runVerifyYear(cfg, 2026) })
	if !one || !all {
		t.Errorf("good mission: -verify 1 = %v, -verify all = %v", one, all)
	}
}

// Every manifest reader stopped at the first failed read and carried on with
// what it had, so an I/O error part-way looked like a shorter manifest. A
// directory standing in for checksums.b3 opens but fails its first read.
func TestAManifestThatCannotBeReadIsAnError(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "checksums.b3")
	if err := os.Mkdir(path, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := readChecksums(path); err == nil {
		t.Error("readChecksums: no error")
	}
	if _, err := mergeChecksums(path, []string{b3("x") + "  x.mp4"}); err == nil {
		t.Error("mergeChecksums: merged into a manifest it could not read")
	}
	if vc := planVerifyCopy(dir, 1); vc.problem == "" {
		t.Error("-verify would have treated the copy as verifiable")
	}
	if !sourceMismatch(sourceManifest{err: os.ErrInvalid}, &result{rel: "x.mp4"}) {
		t.Error("a copy from a source whose manifest cannot be read was accepted")
	}
	if _, err := readChecksums(filepath.Join(dir, "absent.b3")); err != nil {
		t.Errorf("a missing manifest is empty, not an error: %v", err)
	}
}
