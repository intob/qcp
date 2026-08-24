package main

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// proxies.b3 has to be byte-identical in shape to checksums.b3, so the existing
// verify and check machinery reads it without knowing it is a proxy manifest.
func TestProxyManifestRoundTrip(t *testing.T) {
	dir := t.TempDir()
	files := map[string]string{
		"browse/CFEXP/923_0322.mp4":        "hello browse",
		"edit/CFEXP/923_0322.mov":          "hello edit",
		"stills/CFEXP/923_0322.poster.jpg": "hello poster",
	}
	var written []string
	for rel, body := range files {
		p := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0777); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0644); err != nil {
			t.Fatal(err)
		}
		written = append(written, rel)
	}

	m := proxyManifest{
		Version: proxyManifestVersion,
		Year:    2026,
		Mission: "024_Jamie_Balint_landing_La_Jonction",
		Clips: []clipMeta{{
			Rel: "CFEXP/923_0322.MXF", Card: "CFEXP", Size: 2299976240,
			SrcHash: strings.Repeat("ab", 32), Duration: 74.9,
			Width: 3840, Height: 2160, FPS: 23.976, Codec: "h264",
			Gamma: "s-log3-cine", Primaries: "s-gamut3-cine", Sidecar: true,
			Transform: transformSGamut3Cine.ID,
			Browse:    "browse/CFEXP/923_0322.mp4",
			Edit:      "edit/CFEXP/923_0322.mov",
			Poster:    "stills/CFEXP/923_0322.poster.jpg",
		}},
	}
	if err := writeProxyManifests(dir, m, written); err != nil {
		t.Fatal(err)
	}

	// The existing manifest reader must understand it unchanged.
	got := readChecksumFile(filepath.Join(dir, proxyManifestName))
	for rel := range files {
		hash, ok := got[rel]
		if !ok {
			t.Errorf("%s missing from %s", rel, proxyManifestName)
			continue
		}
		want, err := hashFile(filepath.Join(dir, rel), nil)
		if err != nil {
			t.Fatal(err)
		}
		if hash != want {
			t.Errorf("%s: manifest has %s, file hashes to %s", rel, hash, want)
		}
	}
	if _, ok := got[proxyManifestName]; ok {
		t.Errorf("%s describes itself", proxyManifestName)
	}
	if _, ok := got[proxyMetaName]; !ok {
		t.Errorf("%s is not covered by the manifest", proxyMetaName)
	}

	// Sorted, two-space separated, one entry per line — same as checksums.b3.
	raw, err := os.ReadFile(filepath.Join(dir, proxyManifestName))
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	for i, line := range lines {
		hash, rel, ok := strings.Cut(line, "  ")
		if !ok || len(hash) != 64 || rel == "" {
			t.Errorf("line %d is not a manifest entry: %q", i, line)
		}
		if i > 0 && line < lines[i-1] {
			t.Errorf("line %d is out of order: %q after %q", i, line, lines[i-1])
		}
	}

	// And the JSON sidecar round-trips.
	back := readProxyManifest(dir)
	if len(back.Clips) != 1 || back.Clips[0].Transform != transformSGamut3Cine.ID {
		t.Fatalf("proxies.json did not round-trip: %+v", back)
	}
	if back.Clips[0].SrcHash != m.Clips[0].SrcHash {
		t.Error("source hash did not survive the round-trip")
	}
}

// The edit tier must never carry a colour transform: a graded proxy against an
// ungraded original makes the image jump when proxies are toggled in Resolve.
func TestEditTierIsNeverGraded(t *testing.T) {
	args := strings.Join(encodeArgs("/src/923.MXF", true,
		"/p/edit/923.mov", "/p/browse/923.mp4", "/p/luts/x.cube", false, 6_000_000), " ")

	edit, browse, ok := strings.Cut(args, "/p/edit/923.mov")
	if !ok {
		t.Fatal("edit output missing from the command")
	}
	if strings.Contains(edit, "lut3d") {
		t.Errorf("the edit leg is colour-transformed: %s", edit)
	}
	if strings.Contains(edit, "prores_videotoolbox") == false {
		t.Errorf("the edit leg is not ProRes: %s", edit)
	}
	if !strings.Contains(browse, "lut3d=/p/luts/x.cube:interp=tetrahedral") {
		t.Errorf("the browse leg is missing its LUT: %s", browse)
	}
	// The generated cubes are computed against raw code values.
	if !strings.Contains(browse, "in_range=full") {
		t.Errorf("the browse leg is not full range: %s", browse)
	}
	if strings.Contains(browse, "in_range=limited") {
		t.Errorf("the browse leg uses the wrong range convention: %s", browse)
	}
	// Scale before the LUT — it is materially cheaper.
	if strings.Index(browse, "scale=w=") > strings.Index(browse, "lut3d") {
		t.Errorf("the browse leg scales after the LUT: %s", browse)
	}
	// One input, so one decode feeds both renditions.
	if n := strings.Count(args, " -i "); n != 1 {
		t.Errorf("expected a single decode, got %d inputs", n)
	}
}

// A clip with no colour transform must not get the LUT segment at all.
func TestPassThroughOmitsTheLUT(t *testing.T) {
	args := strings.Join(encodeArgs("/src/GH010042.MP4", true, "", "/p/browse/GH010042.mp4", "", false, 6_000_000), " ")
	if strings.Contains(args, "lut3d") || strings.Contains(args, "gbrp10le") {
		t.Errorf("pass-through clip was colour-transformed: %s", args)
	}
	if !strings.Contains(args, "scale=w='min(1920,iw)':h=-2") {
		t.Errorf("browse scale missing: %s", args)
	}
}

// A silent clip still has to encode.
func TestSilentClipDropsTheAudioMap(t *testing.T) {
	args := strings.Join(encodeArgs("/src/x.mp4", false, "", "/p/browse/x.mp4", "", false, 6_000_000), " ")
	if strings.Contains(args, "0:a") {
		t.Errorf("audio was mapped for a clip with no audio stream: %s", args)
	}
}

// Resolve links externally generated proxies by filename excluding extension,
// so the stem has to survive into the proxy tree.
func TestProxyRelKeepsTheSourceStem(t *testing.T) {
	for _, tc := range []struct{ tier, rel, ext, want string }{
		{"browse", "CFEXP/923_0322.MXF", ".mp4", "browse/CFEXP/923_0322.mp4"},
		{"edit", "CFEXP/923_0322.MXF", ".mov", "edit/CFEXP/923_0322.mov"},
		{"browse", "GH010042.MP4", ".mp4", "browse/GH010042.mp4"},
		{"stills", "SD/DJI_0019_D.MP4", ".poster.jpg", "stills/SD/DJI_0019_D.poster.jpg"},
	} {
		if got := proxyRel(tc.tier, tc.rel, tc.ext); got != tc.want {
			t.Errorf("proxyRel(%q, %q, %q) = %q, want %q", tc.tier, tc.rel, tc.ext, got, tc.want)
		}
	}
}

// Proxies are derived and must never reach cold storage. They live outside the
// footage root, so nothing that walks a year directory can pick them up.
func TestProxyTreeSitsOutsideTheFootageRoot(t *testing.T) {
	d := DriveConfig{Volume: "ARCHIVE_01", Root: "Footage", Role: "cold"}
	yearDir := filepath.Join(d.basePath(), d.Root, "2026")
	dir := proxyMissionDir(d.basePath(), 2026, "042_Foo")
	if strings.HasPrefix(dir, yearDir) {
		t.Errorf("proxy dir %s is inside the year directory %s", dir, yearDir)
	}
	if !isProxyDir(filepath.Base(proxyRoot(d.basePath()))) {
		t.Error("the proxy root is not recognised as one")
	}
}

func TestParseTiers(t *testing.T) {
	for _, tc := range []struct {
		in           string
		edit, browse bool
		wantErr      bool
	}{
		{"", false, true, false}, // browse is the default; edit is opt-in only
		{"browse", false, true, false},
		{"edit", true, false, false},
		{"both", true, true, false},
		{"BOTH", true, true, false},
		{"prores", false, false, true},
	} {
		got, err := parseTiers(tc.in)
		if (err != nil) != tc.wantErr {
			t.Errorf("parseTiers(%q) error = %v, wantErr %v", tc.in, err, tc.wantErr)
			continue
		}
		if err == nil && (got.edit != tc.edit || got.browse != tc.browse) {
			t.Errorf("parseTiers(%q) = %+v, want edit=%v browse=%v", tc.in, got, tc.edit, tc.browse)
		}
	}
}

func TestIsVideoFile(t *testing.T) {
	for rel, want := range map[string]bool{
		"CFEXP/923_0322.MXF":    true,
		"GoPro/GH010042.MP4":    true,
		"SD/DJI_0019_D.MP4":     true,
		"SD/DJI_0019_D.LRF":     false, // DJI's own low-res companion
		"GoPro/GL010042.LRV":    false,
		"CFEXP/923_0322M01.XML": false,
		"checksums.b3":          false,
		"Untitled/GOPR0001.THM": false,
	} {
		if got := isVideoFile(rel); got != want {
			t.Errorf("isVideoFile(%q) = %v, want %v", rel, got, want)
		}
	}
}

// A path with a colon or a comma in it would otherwise be read as further
// filter options.
func TestEscapeFilterArg(t *testing.T) {
	got := escapeFilterArg("/Volumes/My Drive: v2/luts/a,b.cube")
	if strings.Contains(strings.ReplaceAll(got, `\:`, ""), ":") {
		t.Errorf("colon left unescaped: %s", got)
	}
	if strings.Contains(strings.ReplaceAll(got, `\,`, ""), ",") {
		t.Errorf("comma left unescaped: %s", got)
	}
}

func TestSpriteCoversTheWholeClip(t *testing.T) {
	args := strings.Join(spriteArgs("/p/browse/x.mp4", "/p/stills/x.sprite.jpg", 420), " ")
	if !strings.Contains(args, "tile=10x10") {
		t.Errorf("sprite is not a 10x10 tile: %s", args)
	}
	want := strconv.FormatFloat(float64(spriteTiles)/420, 'f', 6, 64)
	if !strings.Contains(args, "fps="+want) {
		t.Errorf("sprite rate should sample %d frames across the clip: %s", spriteTiles, args)
	}
}

func TestSidecarColour(t *testing.T) {
	dir := t.TempDir()
	clip := filepath.Join(dir, "923_0013.MXF")
	if err := os.WriteFile(clip, []byte("not really an mxf"), 0644); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := readSidecarColour(clip); ok {
		t.Error("reported a sidecar where there is none")
	}
	sidecar := `<?xml version="1.0"?>
<NonRealTimeMeta>
  <AcquisitionRecord>
    <Group name="CameraUnitMetadataSet">
      <Item name="CaptureGammaEquation" value="s-log3-cine"/>
      <Item name="CaptureColorPrimaries" value="s-gamut3-cine"/>
      <Item name="CodingEquations" value="rec709"/>
    </Group>
  </AcquisitionRecord>
</NonRealTimeMeta>`
	if err := os.WriteFile(filepath.Join(dir, "923_0013M01.XML"), []byte(sidecar), 0644); err != nil {
		t.Fatal(err)
	}
	gamma, prim, ok := readSidecarColour(clip)
	if !ok || gamma != "s-log3-cine" || prim != "s-gamut3-cine" {
		t.Fatalf("read %q / %q (ok=%v)", gamma, prim, ok)
	}
	// CodingEquations is not the capture gamut and must not be mistaken for it.
	if got := pickTransform(gamma, prim); got.ID != transformSGamut3Cine.ID {
		t.Errorf("selected %s, want %s", got.ID, transformSGamut3Cine.ID)
	}
}

func TestClipMetaStaleness(t *testing.T) {
	base := clipMeta{Size: 100, SrcMtime: 7, SrcHash: "aa"}
	if base.stale(100, 7, "aa") {
		t.Error("an unchanged source was reported stale")
	}
	if !base.stale(100, 7, "bb") {
		t.Error("a changed hash was not detected")
	}
	// Missions that predate checksums.b3 have no hash to compare, so size and
	// mtime stand in rather than forcing a second full read of the footage.
	noHash := clipMeta{Size: 100, SrcMtime: 7}
	if noHash.stale(100, 7, "") {
		t.Error("an unchanged source with no recorded hash was reported stale")
	}
	if !noHash.stale(101, 7, "") {
		t.Error("a changed size was not detected")
	}
	if !noHash.stale(100, 8, "") {
		t.Error("a changed mtime was not detected")
	}
}

// Hardware decode is worth it for HEVC and a net loss for H.264: Apple silicon
// decodes H.264 in software about as fast as the media engine returns frames,
// so the round-trip is pure overhead. Measured 1.6x SLOWER on GoPro 1080p.
func TestHardwareDecodeIsGatedOnCodecAndChroma(t *testing.T) {
	for _, tc := range []struct {
		codec, pixFmt string
		want          bool
	}{
		{"hevc", "yuv420p10le", true}, // DJI 4K/8K — the case worth having
		{"hevc", "yuv420p", true},
		{"hevc", "p010le", true},
		{"hevc", "nv12", true},
		{"hevc", "yuv422p10le", false}, // 4:2:2 is slower on the GPU whatever the codec
		{"h264", "yuvj420p", false},    // GoPro 1080p — 1.6x slower with hwaccel
		{"h264", "yuv420p", false},
		{"h264", "yuv422p10le", false}, // Sony XAVC-I Intra
		{"prores", "yuv422p10le", false},
		{"", "", false}, // unprobed: the CPU path is the one always correct
	} {
		if got := hwDecodes(tc.codec, tc.pixFmt); got != tc.want {
			t.Errorf("hwDecodes(%q, %q) = %v, want %v", tc.codec, tc.pixFmt, got, tc.want)
		}
	}
}

// The flag has to reach ffmpeg ahead of the input it applies to, and must not
// appear at all when the gate is closed.
func TestHardwareDecodeFlagPlacement(t *testing.T) {
	on := strings.Join(encodeArgs("/src/dji.MP4", true, "", "/p/browse/dji.mp4", "", true, 6_000_000), " ")
	if !strings.Contains(on, "-hwaccel videotoolbox -i /src/dji.MP4") {
		t.Errorf("hwaccel is not applied to the input: %s", on)
	}
	off := strings.Join(encodeArgs("/src/923.MXF", true, "", "/p/browse/923.mp4", "", false, 6_000_000), " ")
	if strings.Contains(off, "hwaccel") {
		t.Errorf("hwaccel leaked into an ungated encode: %s", off)
	}
	// Still one decode feeding every rendition.
	if n := strings.Count(on, " -i "); n != 1 {
		t.Errorf("expected a single decode, got %d inputs", n)
	}
}

// The browse width is a ceiling, not a target. Upscaling a 432x240 clip from
// 2014 to 1920 spends bitrate manufacturing detail that is not in the source
// and makes the proxy larger than the footage it stands in for.
func TestBrowseNeverUpscales(t *testing.T) {
	for _, lut := range []string{"", "/p/luts/x.cube"} {
		f := browseFilter(lut)
		if !strings.Contains(f, "min(1920,iw)") {
			t.Errorf("browse filter can upscale (lut=%q): %s", lut, f)
		}
		if strings.Contains(f, "scale=1920:") || strings.Contains(f, "scale=1280:") {
			t.Errorf("browse filter uses an absolute width (lut=%q): %s", lut, f)
		}
	}
}

// Resolution and bitrate have to move together: 1080p at the old 2.5 Mbps
// measured worse than 720p at 2.5 Mbps, so a bump to 1080p that kept the old
// bitrate would be a downgrade shipped as an improvement.
func TestBrowseBitrateMatchesTheResolution(t *testing.T) {
	if browseWidth >= 1920 && browseBitrate <= 2_500_000 {
		t.Error("1080p at 2.5 Mbps is worse than 720p at 2.5 Mbps — raise the bitrate")
	}
	args := strings.Join(encodeArgs("/src/x.mp4", false, "", "/p/browse/x.mp4", "", false, 6_000_000), " ")
	if !strings.Contains(args, "-b:v 6000000") {
		t.Errorf("browse leg does not carry the given bitrate: %s", args)
	}
}

// A rendition built with different settings must not read as up to date, or
// changing the tier would silently leave the archive holding a mix of two.
func TestTierChangeInvalidatesACachedEntry(t *testing.T) {
	if (clipMeta{BrowseSpec: browseSpec()}).BrowseSpec != browseSpec() {
		t.Fatal("spec did not round-trip")
	}
	old := clipMeta{BrowseSpec: "1280w/2500k"}
	if old.BrowseSpec == browseSpec() {
		t.Error("an entry from the previous tier compares equal to the current one")
	}
	// Entries written before the tier was recorded have no spec at all.
	if (clipMeta{}).BrowseSpec == browseSpec() {
		t.Error("an entry with no recorded tier compares equal to the current one")
	}
}

// A proxy must never be bigger than the footage it stands in for. A flat tier
// bitrate applied to a 320x240 clip produced a proxy 7.7x the size of its
// source, so the rate scales with the output frame and is capped at the
// source's own rate.
func TestBrowseRateFollowsTheFrame(t *testing.T) {
	full := browseRate(1920, 1080, 0)
	if full != browseBitrate {
		t.Errorf("a 1080p frame should get the full tier rate, got %d", full)
	}
	small := browseRate(320, 240, 0)
	if small >= full {
		t.Errorf("a 320x240 frame got %d, not below the 1080p rate %d", small, full)
	}
	if small < browseFloor {
		t.Errorf("rate %d fell below the floor", small)
	}
	// Never spend more than the source does — there is no detail up there.
	if got := browseRate(1920, 1080, 1_200_000); got != 1_200_000 {
		t.Errorf("rate %d exceeded a 1.2 Mbps source", got)
	}
	// ...but the floor still applies to a pathologically low source.
	if got := browseRate(320, 240, 1000); got != browseFloor {
		t.Errorf("rate %d ignored the floor", got)
	}
}

// The width is a ceiling and the height follows the source aspect, rounded even.
func TestBrowseSizeCapsWidthAndKeepsAspect(t *testing.T) {
	for _, tc := range []struct{ w, h, wantW, wantH int }{
		{3840, 2160, 1920, 1080}, // 4K 16:9
		{5312, 4648, 1920, 1680}, // GoPro 8:7
		{1280, 720, 1280, 720},   // already below the cap: untouched
		{320, 240, 320, 240},     // legacy SD: untouched
		{0, 0, 0, 0},             // unprobed
	} {
		gw, gh := browseSize(tc.w, tc.h)
		if gw != tc.wantW || gh != tc.wantH {
			t.Errorf("browseSize(%d,%d) = %d,%d want %d,%d", tc.w, tc.h, gw, gh, tc.wantW, tc.wantH)
		}
		if gh%2 != 0 {
			t.Errorf("browseSize(%d,%d) height %d is odd", tc.w, tc.h, gh)
		}
	}
}
