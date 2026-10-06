package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

// proxyFixture builds one drive holding a mission of clips, and returns the
// drive base and the mission's proxy directory.
func proxyFixture(t *testing.T, clips ...string) (base, outDir string) {
	t.Helper()
	base = t.TempDir()
	for _, c := range clips {
		writeMissionFile(t, base, "2026", "042_Test", c, "footage-"+c)
	}
	outDir = proxyMissionDir(base, 2026, "042_Test")
	if err := os.MkdirAll(filepath.Join(outDir, "browse"), 0o755); err != nil {
		t.Fatal(err)
	}
	return base, outDir
}

// writeRendition puts a complete browse tier on disk for one clip — the video
// and the stills that are generated with it, since a clip missing either is one
// -proxy still has work for. fileExists rejects empty files, so each needs a
// body.
func writeRendition(t *testing.T, outDir, clip string) {
	t.Helper()
	for _, rel := range []string{
		proxyRel("browse", clip, ".mp4"),
		proxyRel("stills", clip, ".poster.jpg"),
		proxyRel("stills", clip, ".sprite.jpg"),
	} {
		p := filepath.Join(outDir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte("rendition"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

// writeProxyMeta writes proxies.json describing the given clips as current.
func writeProxyMeta(t *testing.T, base, slug, outDir string, clips ...string) {
	t.Helper()
	m := proxyManifest{Version: proxyManifestVersion, Year: 2026, Mission: slug}
	for _, c := range clips {
		fi, err := os.Stat(filepath.Join(base, "2026", slug, c))
		if err != nil {
			t.Fatal(err)
		}
		m.Clips = append(m.Clips, clipMeta{
			Rel: c, Size: fi.Size(), SrcMtime: fi.ModTime().Unix(),
			Duration: 10, Width: 1920, Height: 1080, FPS: 25, Codec: "h264",
			Transform:  transformNone.ID,
			BrowseSpec: browseSpec(), Browse: proxyRel("browse", c, ".mp4"),
			Poster: proxyRel("stills", c, ".poster.jpg"),
			Sprite: proxyRel("stills", c, ".sprite.jpg"),
		})
	}
	raw, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(outDir, proxyMetaName), raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

// rewriteProxyMeta edits proxies.json in place, for the cases where what makes
// a rendition stale is something the writing helper would never produce.
func rewriteProxyMeta(t *testing.T, outDir string, edit func(*proxyManifest)) {
	t.Helper()
	p := filepath.Join(outDir, proxyMetaName)
	raw, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	var m proxyManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	edit(&m)
	if raw, err = json.MarshalIndent(m, "", "  "); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, raw, 0o644); err != nil {
		t.Fatal(err)
	}
}

func scanOne(base string) proxyScan {
	cfg := Config{Drives: []DriveConfig{{Volume: "T9", Path: base, Role: "hot"}}}
	return scanProxyMission(cfg, 2026, "2026", "042_Test", colourTransform{})
}

// A mission with a rendition for every clip and a manifest that still matches
// is the only case that should read as complete.
func TestScanProxyMissionComplete(t *testing.T) {
	base, outDir := proxyFixture(t, "a.mp4", "b.mp4")
	writeRendition(t, outDir, "a.mp4")
	writeRendition(t, outDir, "b.mp4")
	writeProxyMeta(t, base, "042_Test", outDir, "a.mp4", "b.mp4")

	sc := scanOne(base)
	if sc.clips != 2 || sc.browse != 2 {
		t.Errorf("clips = %d, browse = %d, want 2 and 2", sc.clips, sc.browse)
	}
	if sc.staleBrowse != 0 {
		t.Errorf("staleBrowse = %d, want 0", sc.staleBrowse)
	}
	if len(sc.drives) != 1 || sc.drives[0] != "T9" {
		t.Errorf("drives = %v, want [T9]", sc.drives)
	}
	if sc.unchecked {
		t.Error("unchecked = true with the footage mounted")
	}
}

// Footage shot after the proxy run is the ordinary way a tree falls behind, and
// the count against the mission is the only thing that shows it: the manifest
// agrees with itself, and every rendition it lists is on disk.
func TestScanProxyMissionPartial(t *testing.T) {
	base, outDir := proxyFixture(t, "a.mp4", "b.mp4", "c.mp4")
	writeRendition(t, outDir, "a.mp4")
	writeProxyMeta(t, base, "042_Test", outDir, "a.mp4")

	sc := scanOne(base)
	if sc.clips != 3 || sc.browse != 1 {
		t.Errorf("clips = %d, browse = %d, want 3 and 1", sc.clips, sc.browse)
	}
	if sc.staleBrowse != 0 {
		t.Errorf("staleBrowse = %d, want 0 — the built clip is still current", sc.staleBrowse)
	}
}

// A source that has changed under an existing rendition is worse than a missing
// one: the clip looks browsable and shows the wrong picture. It has to count as
// stale rather than as covered.
func TestScanProxyMissionStaleSource(t *testing.T) {
	base, outDir := proxyFixture(t, "a.mp4")
	writeRendition(t, outDir, "a.mp4")
	writeProxyMeta(t, base, "042_Test", outDir, "a.mp4")

	clip := filepath.Join(base, "2026", "042_Test", "a.mp4")
	if err := os.WriteFile(clip, []byte("re-shot, a different length"), 0o644); err != nil {
		t.Fatal(err)
	}

	sc := scanOne(base)
	if sc.browse != 1 {
		t.Errorf("browse = %d, want 1 — the rendition is still on disk", sc.browse)
	}
	if sc.staleBrowse != 1 {
		t.Errorf("staleBrowse = %d, want 1", sc.staleBrowse)
	}
}

// A rendition built to a superseded tier spec is on disk and matches its
// source, but is no longer what the browse tier means, and -proxy would rebuild
// it. Reporting it as complete would hide work the tool intends to do.
func TestScanProxyMissionStaleSpec(t *testing.T) {
	base, outDir := proxyFixture(t, "a.mp4")
	writeRendition(t, outDir, "a.mp4")
	writeProxyMeta(t, base, "042_Test", outDir, "a.mp4")

	rewriteProxyMeta(t, outDir, func(m *proxyManifest) { m.Clips[0].BrowseSpec = "640w/1000k" })

	if sc := scanOne(base); sc.staleBrowse != 1 {
		t.Errorf("staleBrowse = %d, want 1 for a rendition built to an older spec", sc.staleBrowse)
	}
}

// A mission with footage and no tree is the normal "not done yet" case, and
// must not be confused with one whose footage is unmounted.
func TestScanProxyMissionNone(t *testing.T) {
	base := t.TempDir()
	writeMissionFile(t, base, "2026", "042_Test", "a.mp4", "footage")

	sc := scanOne(base)
	if sc.clips != 1 || sc.browse != 0 {
		t.Errorf("clips = %d, browse = %d, want 1 and 0", sc.clips, sc.browse)
	}
	if len(sc.drives) != 0 {
		t.Errorf("drives = %v, want none", sc.drives)
	}
	if sc.unchecked {
		t.Error("unchecked = true for a mounted mission with no proxies")
	}
}

// Proxies outlive their footage: the browse tier stays on a hot drive so an
// evicted mission is still browsable. With nothing to count against, the row
// has to report the manifest's own count and admit it could not check it —
// counting the clips as zero would read as "no proxies" for a tree that is
// there and working.
func TestScanProxyMissionFootageUnmounted(t *testing.T) {
	base, outDir := proxyFixture(t, "a.mp4", "b.mp4")
	writeRendition(t, outDir, "a.mp4")
	writeRendition(t, outDir, "b.mp4")
	writeProxyMeta(t, base, "042_Test", outDir, "a.mp4", "b.mp4")

	if err := os.RemoveAll(filepath.Join(base, "2026")); err != nil {
		t.Fatal(err)
	}

	sc := scanOne(base)
	if !sc.unchecked {
		t.Error("unchecked = false with the footage gone")
	}
	if sc.clips != 2 || sc.browse != 2 {
		t.Errorf("clips = %d, browse = %d, want 2 and 2 from the manifest", sc.clips, sc.browse)
	}
	if len(sc.drives) != 1 {
		t.Errorf("drives = %v, want the drive holding the tree", sc.drives)
	}
}

// The mission list has to come from both trees, or a mission that has been
// evicted to an unmounted cold drive disappears from a report whose whole
// subject is the tier that outlives it.
func TestProxyMissionSlugsIncludesProxyOnlyMissions(t *testing.T) {
	base := t.TempDir()
	writeMissionFile(t, base, "2026", "042_Footage", "a.mp4", "footage")
	if err := os.MkdirAll(proxyMissionDir(base, 2026, "043_Evicted"), 0o755); err != nil {
		t.Fatal(err)
	}

	cfg := Config{Drives: []DriveConfig{{Volume: "T9", Path: base, Role: "hot"}}}
	got := proxyMissionSlugs(cfg, 2026)
	want := []string{"042_Footage", "043_Evicted"}
	if len(got) != len(want) {
		t.Fatalf("slugs = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("slugs = %v, want %v", got, want)
		}
	}
}

// A year that exists only under proxies/ must survive -year all for the same
// reason.
func TestProxyYearsIncludesProxyOnlyYears(t *testing.T) {
	base := t.TempDir()
	writeMissionFile(t, base, "2026", "042_Footage", "a.mp4", "footage")
	if err := os.MkdirAll(proxyMissionDir(base, 2024, "007_Old"), 0o755); err != nil {
		t.Fatal(err)
	}

	cfg := Config{Drives: []DriveConfig{{Volume: "T9", Path: base, Role: "hot"}}}
	years := proxyYears(cfg)
	if len(years) != 2 || years[0] != 2026 || years[1] != 2024 {
		t.Errorf("years = %v, want [2026 2024]", years)
	}
}

// A rendition whose recorded transform is no longer the one that would be
// chosen — the look was repointed, edited, or removed — is on disk, matches its
// source, and was built to the current tier spec. Everything cheap about it
// looks current, and only the transform gives it away. Reporting it as done
// would tell you the mission is finished while -proxy is waiting to rebuild
// every clip in it.
func TestScanProxyMissionStaleTransform(t *testing.T) {
	base, outDir := proxyFixture(t, "a.mp4", "b.mp4")
	writeRendition(t, outDir, "a.mp4")
	writeRendition(t, outDir, "b.mp4")
	writeProxyMeta(t, base, "042_Test", outDir, "a.mp4", "b.mp4")
	rewriteProxyMeta(t, outDir, func(m *proxyManifest) {
		for i := range m.Clips {
			m.Clips[i].Transform = "look/superseded@deadbeef"
		}
	})

	sc := scanOne(base)
	if sc.browse != 2 {
		t.Errorf("browse = %d, want 2 — the renditions are still on disk", sc.browse)
	}
	if sc.staleBrowse != 2 {
		t.Errorf("staleBrowse = %d, want 2 for renditions baked with a superseded transform", sc.staleBrowse)
	}
}

// The report has one job it cannot be allowed to get wrong: agreeing with the
// run it describes. planMission is what -proxy plans with, so for the browse
// tier the clips it has work for are exactly the ones with no rendition plus
// the ones reported stale. Deriving staleness independently of planMission drifts
// from it silently — it did, on a real tree — so pin the two together here.
func TestScanProxyMissionAgreesWithPlan(t *testing.T) {
	base, outDir := proxyFixture(t, "a.mp4", "b.mp4", "c.mp4", "d.mp4")
	writeRendition(t, outDir, "a.mp4") // current
	writeRendition(t, outDir, "b.mp4") // built with a transform since superseded
	writeRendition(t, outDir, "c.mp4") // source changed underneath it
	// d.mp4 has no rendition at all.
	writeProxyMeta(t, base, "042_Test", outDir, "a.mp4", "b.mp4", "c.mp4")
	rewriteProxyMeta(t, outDir, func(m *proxyManifest) {
		for i := range m.Clips {
			if m.Clips[i].Rel == "b.mp4" {
				m.Clips[i].Transform = "look/superseded@deadbeef"
			}
		}
	})
	clip := filepath.Join(base, "2026", "042_Test", "c.mp4")
	if err := os.WriteFile(clip, []byte("re-shot, a different length"), 0o644); err != nil {
		t.Fatal(err)
	}

	sc := scanOne(base)
	cfg := Config{Drives: []DriveConfig{{Volume: "T9", Path: base, Role: "hot"}}}
	src, err := proxySourceForSlug(cfg, "2026", "042_Test")
	if err != nil {
		t.Fatal(err)
	}
	plan := planMission(src, outDir, proxyTiers{browse: true}, colourTransform{})

	if want := (sc.clips - sc.browse) + sc.staleBrowse; plan.todo != want {
		t.Errorf("plan.todo = %d, but the report implies %d work "+
			"(%d clips, %d with a rendition, %d of those stale)",
			plan.todo, want, sc.clips, sc.browse, sc.staleBrowse)
	}
	if sc.staleBrowse != 2 {
		t.Errorf("staleBrowse = %d, want 2 — the superseded transform and the changed source", sc.staleBrowse)
	}
}

// The index shows a clip through its poster and scrubs it through its sprite,
// so a browse video sitting on its own is not a browse tier anyone can use, and
// -proxy has work for it. Counting it as covered and current would report a
// mission as browsable that the index cannot render.
func TestScanProxyMissionMissingStills(t *testing.T) {
	base, outDir := proxyFixture(t, "a.mp4")
	writeRendition(t, outDir, "a.mp4")
	writeProxyMeta(t, base, "042_Test", outDir, "a.mp4")
	if err := os.Remove(filepath.Join(outDir, proxyRel("stills", "a.mp4", ".poster.jpg"))); err != nil {
		t.Fatal(err)
	}

	sc := scanOne(base)
	if sc.browse != 1 {
		t.Errorf("browse = %d, want 1 — the video is on disk", sc.browse)
	}
	if sc.staleBrowse != 1 {
		t.Errorf("staleBrowse = %d, want 1 for a browse video with no poster", sc.staleBrowse)
	}
}
