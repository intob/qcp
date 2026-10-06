package main

import (
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"lukechampine.com/blake3"
)

func b3(s string) string {
	h := blake3.Sum256([]byte(s))
	return hex.EncodeToString(h[:])
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// syncFixture is a hot drive holding mission 001_A and an empty cold drive.
func syncFixture(root string) (cfg Config, hotDir, coldDir string) {
	hot := filepath.Join(root, "hot")
	cold := filepath.Join(root, "cold")
	cfg = Config{Drives: []DriveConfig{
		{Volume: "HOT", Path: hot, Role: "hot"},
		{Volume: "COLD", Path: cold, Role: "cold"},
	}}
	return cfg, filepath.Join(hot, "2026", "001_A"), filepath.Join(cold, "2026", "001_A")
}

// -sync verified each cold copy against the bytes it had just read from the hot
// drive, never against the hot drive's own checksums.b3. A hot file that had
// rotted since ingest was copied faithfully, and the cold manifest recorded the
// damage as the good hash. A copy that failed verification was also left on the
// cold drive under its final name, where every re-run skipped it as synced.
func TestSyncRefusesASourceThatNoLongerMatchesItsManifest(t *testing.T) {
	root := fixtureDir(t)
	cfg, hotDir, coldDir := syncFixture(root)
	if !isChild(t) {
		writeFile(t, filepath.Join(hotDir, "a.mp4"), "rotted")
		writeFile(t, filepath.Join(hotDir, "b.mp4"), "fine")
		writeFile(t, filepath.Join(hotDir, "checksums.b3"),
			b3("original")+"  a.mp4\n"+b3("fine")+"  b.mp4\n")
		os.MkdirAll(filepath.Join(root, "cold", "2026"), 0o755)
	}

	code, out := inSubprocess(t, root, func() { runSync(cfg, 2026, true) })

	if code == 0 {
		t.Errorf("sync succeeded over a source that does not match its manifest\n%s", out)
	}
	if exists(filepath.Join(coldDir, "a.mp4")) {
		t.Error("the copy of the rotted file was left on the cold drive")
	}
	m := readChecksumFile(filepath.Join(coldDir, "checksums.b3"))
	if _, ok := m["a.mp4"]; ok {
		t.Error("the cold manifest recorded the rotted file")
	}
	if m["b.mp4"] != b3("fine") {
		t.Errorf("the file that did verify was not recorded: %v", m)
	}
}

// A copy failure exited before the verify phase. The files that had copied were
// on the cold drive under their final names, never read back and in no
// manifest, and a re-run skipped them because they existed.
func TestSyncVerifiesWhatCopiedWhenSomethingElseFailed(t *testing.T) {
	root := fixtureDir(t)
	cfg, hotDir, coldDir := syncFixture(root)
	if !isChild(t) {
		writeFile(t, filepath.Join(hotDir, "a.mp4"), "unreadable")
		writeFile(t, filepath.Join(hotDir, "b.mp4"), "fine")
		os.Chmod(filepath.Join(hotDir, "a.mp4"), 0)
		t.Cleanup(func() { os.Chmod(filepath.Join(hotDir, "a.mp4"), 0o644) })
		os.MkdirAll(filepath.Join(root, "cold", "2026"), 0o755)
	}

	code, out := inSubprocess(t, root, func() { runSync(cfg, 2026, true) })

	if code == 0 {
		t.Errorf("sync succeeded with a file it could not read\n%s", out)
	}
	if m := readChecksumFile(filepath.Join(coldDir, "checksums.b3")); m["b.mp4"] != b3("fine") {
		t.Errorf("the file that copied was not verified and recorded: %v\n%s", m, out)
	}
}

// -copy, -pull and -replicate carry their own copy of the verify phase; each
// must refuse a rotted source the same way -sync does.
func TestTransfersRefuseASourceThatNoLongerMatchesItsManifest(t *testing.T) {
	cases := []struct {
		name             string
		srcRole, dstRole string
		run              func(Config)
	}{
		{"copy", "hot", "hot", func(cfg Config) { runCopy(cfg, []int{1}, 2026, "", []string{"DST"}, true) }},
		{"replicate", "cold", "cold", func(cfg Config) { runReplicate(cfg, 2026, true) }},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			root := fixtureDir(t)
			src := filepath.Join(root, "src")
			dst := filepath.Join(root, "dst")
			cfg := Config{Drives: []DriveConfig{
				{Volume: "SRC", Path: src, Role: c.srcRole},
				{Volume: "DST", Path: dst, Role: c.dstRole},
			}}
			srcDir := filepath.Join(src, "2026", "001_A")
			dstDir := filepath.Join(dst, "2026", "001_A")
			if !isChild(t) {
				writeFile(t, filepath.Join(srcDir, "a.mp4"), "rotted")
				writeFile(t, filepath.Join(srcDir, "b.mp4"), "fine")
				writeFile(t, filepath.Join(srcDir, "checksums.b3"),
					b3("original")+"  a.mp4\n"+b3("fine")+"  b.mp4\n")
				os.MkdirAll(filepath.Join(dst, "2026"), 0o755)
			}

			code, out := inSubprocess(t, root, func() { c.run(cfg) })

			if code == 0 {
				t.Errorf("succeeded over a source that does not match its manifest\n%s", out)
			}
			if exists(filepath.Join(dstDir, "a.mp4")) {
				t.Errorf("the copy of the rotted file was left behind\n%s", out)
			}
			if m := readChecksumFile(filepath.Join(dstDir, "checksums.b3")); m["b.mp4"] != b3("fine") {
				t.Errorf("the file that did verify was not recorded: %v\n%s", m, out)
			}
		})
	}
}

// Every transfer decided "already there" by name alone, so a truncated file or
// a different file under the same name was taken for done and never reported.
// A size mismatch is reported and left alone — the destination may be the
// right copy — and the run fails.
func TestSyncReportsAFileOfTheWrongSize(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	root := t.TempDir()
	cfg, hotDir, coldDir := syncFixture(root)
	writeFile(t, filepath.Join(hotDir, "a.mp4"), "the whole clip")
	writeFile(t, filepath.Join(hotDir, "b.mp4"), "new")
	writeFile(t, filepath.Join(coldDir, "a.mp4"), "trunc")

	var ok bool
	out := captureStdout(t, func() { ok = runSync(cfg, 2026, true) })

	if ok {
		t.Errorf("sync succeeded over a file of the wrong size\n%s", out)
	}
	if data, _ := os.ReadFile(filepath.Join(coldDir, "a.mp4")); string(data) != "trunc" {
		t.Error("the mismatched file was overwritten")
	}
	if !exists(filepath.Join(coldDir, "b.mp4")) {
		t.Error("the missing file beside it was not copied")
	}
	if !strings.Contains(out, "a.mp4") {
		t.Errorf("the mismatch is not named:\n%s", out)
	}
}

func TestCopyReportsAFileOfTheWrongSize(t *testing.T) {
	root := fixtureDir(t)
	src, dst := filepath.Join(root, "src"), filepath.Join(root, "dst")
	cfg := Config{Drives: []DriveConfig{
		{Volume: "SRC", Path: src, Role: "hot"},
		{Volume: "DST", Path: dst, Role: "hot"},
	}}
	if !isChild(t) {
		writeFile(t, filepath.Join(src, "2026", "001_A", "a.mp4"), "the whole clip")
		writeFile(t, filepath.Join(src, "2026", "001_A", "more.mp4"), "more of it")
		writeFile(t, filepath.Join(dst, "2026", "001_A", "a.mp4"), "trunc")
	}
	code, out := inSubprocess(t, root, func() { runCopy(cfg, []int{1}, 2026, "", []string{"DST"}, true) })
	if code == 0 {
		t.Errorf("copy succeeded over a file of the wrong size\n%s", out)
	}
	if data, _ := os.ReadFile(filepath.Join(dst, "2026", "001_A", "a.mp4")); string(data) != "trunc" {
		t.Error("the mismatched file was overwritten")
	}
}

func TestCheckReportsAFileOfTheWrongSize(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	root := t.TempDir()
	cfg, hotDir, coldDir := syncFixture(root)
	writeFile(t, filepath.Join(hotDir, "a.mp4"), "the whole clip")
	writeFile(t, filepath.Join(coldDir, "a.mp4"), "trunc")

	var mission, year bool
	out := captureStdout(t, func() {
		mission = runCheckMission(cfg, 1, 2026, true)
		year = runCheck(cfg, 2026)
	})
	if mission || year {
		t.Errorf("-check passed a cold copy of the wrong size (mission %v, year %v)\n%s", mission, year, out)
	}
}

func TestPlanCopy(t *testing.T) {
	src := []fileEntry{{"same", 5}, {"gone", 3}, {"short", 10}}
	dst := []fileEntry{{"same", 5}, {"short", 4}, {"extra", 1}}
	missing, conflicts := planCopy(src, dst)
	if len(missing) != 1 || missing[0].rel != "gone" {
		t.Errorf("missing = %v", missing)
	}
	if len(conflicts) != 1 || conflicts[0] != (sizeConflict{"short", 10, 4}) {
		t.Errorf("conflicts = %v", conflicts)
	}
}

// -check compared the reference copy with the cold drives only, so a second
// hot copy was never compared with anything.
func TestCheckComparesASecondHotCopy(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	root := t.TempDir()
	hot1, hot2, cold := filepath.Join(root, "h1"), filepath.Join(root, "h2"), filepath.Join(root, "c")
	cfg := Config{Drives: []DriveConfig{
		{Volume: "T9", Path: hot1, Role: "hot"},
		{Volume: "T7", Path: hot2, Role: "hot"},
		{Volume: "ARCHIVE", Path: cold, Role: "cold"},
	}}
	for _, d := range []string{hot1, cold} {
		writeFile(t, filepath.Join(d, "2026", "001_A", "a.mp4"), "a")
		writeFile(t, filepath.Join(d, "2026", "001_A", "b.mp4"), "b")
		// on T9 and the archive, not on T7: not every hot drive holds every mission
		writeFile(t, filepath.Join(d, "2026", "002_B", "c.mp4"), "c")
	}
	writeFile(t, filepath.Join(hot2, "2026", "001_A", "a.mp4"), "a") // b.mp4 never arrived

	var mission, year bool
	out := captureStdout(t, func() {
		mission = runCheckMission(cfg, 1, 2026, true)
		year = runCheck(cfg, 2026)
	})
	if mission || year {
		t.Errorf("-check passed a hot copy missing a file (mission %v, year %v)\n%s", mission, year, out)
	}
	if !strings.Contains(out, "T7") || !strings.Contains(out, "b.mp4") {
		t.Errorf("the incomplete hot copy is not named:\n%s", out)
	}
	if strings.Contains(out, "002_B") {
		t.Errorf("a mission T7 does not hold was reported as a gap:\n%s", out)
	}
	if !runCheckMission(cfg, 2, 2026, true) {
		t.Error("-check 2 failed although every drive holding it is complete")
	}
}

// -pull and -copy listed the source with findFiles, so its checksums.b3 was
// copied like footage and the destination's manifest was a byte copy of the
// source's, stale entries included, instead of the hashes this run verified.
func TestCopyBuildsTheManifestFromWhatItVerified(t *testing.T) {
	t.Setenv("HOME", t.TempDir())
	root := t.TempDir()
	src, dst := filepath.Join(root, "src"), filepath.Join(root, "dst")
	cfg := Config{Drives: []DriveConfig{
		{Volume: "SRC", Path: src, Role: "hot"},
		{Volume: "DST", Path: dst, Role: "hot"},
	}}
	writeFile(t, filepath.Join(src, "2026", "001_A", "a.mp4"), "a")
	writeFile(t, filepath.Join(src, "2026", "001_A", "checksums.b3"),
		b3("a")+"  a.mp4\n"+b3("gone")+"  gone.mp4\n")
	os.MkdirAll(filepath.Join(dst, "2026"), 0o755)

	s, err := resolveSource(cfg, "hot", "2026", 1, "")
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range s.files {
		if f.rel == "checksums.b3" {
			t.Fatal("the source manifest is listed as a file to copy")
		}
	}
	captureStdout(t, func() { runCopy(cfg, []int{1}, 2026, "", []string{"DST"}, true) })

	m := readChecksumFile(filepath.Join(dst, "2026", "001_A", "checksums.b3"))
	if m["a.mp4"] != b3("a") {
		t.Errorf("the copied file is not recorded: %v", m)
	}
	if _, ok := m["gone.mp4"]; ok {
		t.Errorf("a stale entry was carried across from the source: %v", m)
	}
}
