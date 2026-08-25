package main

import (
	"encoding/binary"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// Neutrals must survive the gamut conversion untouched. Both gamuts share D65,
// so each row has to sum to exactly 1 — if it does not, grey turns coloured and
// every browse proxy in the library is subtly wrong.
func TestGamutMatricesPreserveNeutrals(t *testing.T) {
	for _, tc := range []struct {
		name string
		p    primaries
	}{
		{"S-Gamut3", sGamut3},
		{"S-Gamut3.Cine", sGamut3Cine},
	} {
		m := gamutMatrix(tc.p)
		for i, row := range m {
			sum := row[0] + row[1] + row[2]
			if math.Abs(sum-1) > 1e-6 {
				t.Errorf("%s row %d sums to %.9f, want 1", tc.name, i, sum)
			}
		}
	}
}

// The three code values the S-Log3 curve is defined by: the toe joins at black,
// CV 420 is 18% grey by definition, and CV 598 is the published landmark well
// up the log segment.
func TestSLog3Landmarks(t *testing.T) {
	for _, tc := range []struct {
		cv, want, tol float64
	}{
		{95, 0, 1e-9},
		{420, 0.18, 1e-9},
		{598, 0.9008, 1e-4},
	} {
		got := slog3ToLinear(tc.cv / 1023)
		if math.Abs(got-tc.want) > tc.tol {
			t.Errorf("CV %.0f decoded to %.6f, want %.4f", tc.cv, got, tc.want)
		}
	}
}

// The shoulder is what stops a technical CST blowing the sky to paper white, so
// its two fixed points matter: 18% grey must land where a Rec.709 grade expects
// it, and +5.5 stops must reach 1.0 without clipping anything below it.
func TestToneMapLandmarks(t *testing.T) {
	for _, tc := range []struct {
		lin, want float64
	}{
		{0, 0},
		{0.18, 0.4574},
		{8.0, 1.0},
	} {
		got := encodeRec709(tc.lin)
		if math.Abs(got-tc.want) > 1e-4 {
			t.Errorf("encodeRec709(%.2f) = %.6f, want %.4f", tc.lin, got, tc.want)
		}
	}
}

func TestPickTransform(t *testing.T) {
	for _, tc := range []struct {
		gamma, prim string
		want        colourTransform
	}{
		{"s-log3", "s-gamut3", transformSGamut3},
		{"S-Log3", "S-Gamut3", transformSGamut3},
		{"s-log3-cine", "s-gamut3-cine", transformSGamut3Cine},
		{"s-log3", "s-gamut3-cine", transformSGamut3Cine},
		{"s-cinetone", "rec709", transformNone},
		{"rec709", "rec709", transformNone},
		{"", "", transformNone},
		// A gamma we do not know how to decode passes through rather than
		// guessing — a wrong bake is worse than a flat thumbnail.
		{"s-log2", "s-gamut", transformNone},
		{"hlg", "rec2020", transformNone},
	} {
		if got := pickTransform(tc.gamma, tc.prim); got.ID != tc.want.ID {
			t.Errorf("pickTransform(%q, %q) = %s, want %s", tc.gamma, tc.prim, got.ID, tc.want.ID)
		}
	}
}

// Seven of 2,530 MXF in the library have no sidecar. Capture settings do not
// change mid-card, so those inherit whatever the rest of their card used.
func TestFillMissingColour(t *testing.T) {
	cine := clipColour{Gamma: "s-log3-cine", Prim: "s-gamut3-cine", Found: true}
	plain := clipColour{Gamma: "s-log3", Prim: "s-gamut3", Found: true}
	cinetone := clipColour{Gamma: "s-cinetone", Prim: "rec709", Found: true}
	none := clipColour{}

	for _, tc := range []struct {
		name  string
		clips []clipColour
		want  []string
	}{
		{
			"missing inherits the mission majority",
			[]clipColour{plain, plain, cine, none},
			[]string{transformSGamut3.ID, transformSGamut3.ID, transformSGamut3Cine.ID, transformSGamut3.ID},
		},
		{
			"a mission with nothing to inherit passes through",
			[]clipColour{none, none},
			[]string{transformNone.ID, transformNone.ID},
		},
		{
			"pass-through clips count towards the majority",
			[]clipColour{cinetone, cinetone, cine, none},
			[]string{transformNone.ID, transformNone.ID, transformSGamut3Cine.ID, transformNone.ID},
		},
		{
			"a lone sidecar is enough to inherit from",
			[]clipColour{none, cine, none},
			[]string{transformSGamut3Cine.ID, transformSGamut3Cine.ID, transformSGamut3Cine.ID},
		},
		{
			// 002_Portugal: 147 Sony MXF at the mission root beside 19 DJI
			// clips under Drone_Andu/. Inheriting mission-wide handed the
			// Sony conversion to the drone card and baked a grade onto
			// footage that never was S-Log3.
			"a second camera's card does not inherit the Sony conversion",
			[]clipColour{
				{Card: "", Gamma: "s-log3-cine", Prim: "s-gamut3-cine", Found: true},
				{Card: "", Gamma: "s-log3-cine", Prim: "s-gamut3-cine", Found: true},
				{Card: "Drone_Andu"},
				{Card: "Drone_Andu"},
			},
			[]string{transformSGamut3Cine.ID, transformSGamut3Cine.ID, transformNone.ID, transformNone.ID},
		},
		{
			"a sidecar-less clip still inherits from its own card",
			[]clipColour{
				{Card: "A", Gamma: "s-log3", Prim: "s-gamut3", Found: true},
				{Card: "A"},
				{Card: "B", Gamma: "s-log3-cine", Prim: "s-gamut3-cine", Found: true},
				{Card: "B"},
			},
			[]string{transformSGamut3.ID, transformSGamut3.ID, transformSGamut3Cine.ID, transformSGamut3Cine.ID},
		},
	} {
		got := fillMissingColour(tc.clips)
		if len(got) != len(tc.want) {
			t.Fatalf("%s: got %d transforms, want %d", tc.name, len(got), len(tc.want))
		}
		for i := range got {
			if got[i].ID != tc.want[i] {
				t.Errorf("%s: clip %d got %s, want %s", tc.name, i, got[i].ID, tc.want[i])
			}
		}
	}
}

// The generated cube has to survive the trip through ffmpeg's lut3d as well as
// it does in Go, or the whole browse tier is graded by something other than
// what these tests check. A synthetic neutral ramp at known code values is run
// through the documented filter chain and compared against lutSample.
//
// The chain stops before the final format=yuv420p: the point is to measure the
// LUT, not the 8-bit quantisation that follows it.
func TestLUTRoundTripThroughFFmpeg(t *testing.T) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg not installed")
	}
	dir := t.TempDir()
	cube, err := ensureLUT(dir, transformSGamut3Cine)
	if err != nil {
		t.Fatal(err)
	}

	cvs := []float64{95, 171, 300, 420, 500, 598, 700, 800, 940}
	const h = 2
	w := 2 * len(cvs) // two pixels per code value keeps the width even for 4:2:2

	// yuv422p10le, full range, neutral chroma: Y = CV gives R = G = B = CV.
	raw := make([]byte, 0, w*h*2+2*(w/2)*h*2)
	put := func(v uint16) { raw = binary.LittleEndian.AppendUint16(raw, v) }
	for y := 0; y < h; y++ {
		for x := 0; x < w; x++ {
			put(uint16(cvs[x/2]))
		}
	}
	for p := 0; p < 2; p++ {
		for y := 0; y < h; y++ {
			for x := 0; x < w/2; x++ {
				put(512)
			}
		}
	}
	in := filepath.Join(dir, "ramp.raw")
	if err := os.WriteFile(in, raw, 0644); err != nil {
		t.Fatal(err)
	}

	out, err := exec.Command("ffmpeg", "-hide_banner", "-loglevel", "error",
		"-f", "rawvideo", "-pix_fmt", "yuv422p10le", "-s", strconv.Itoa(w)+"x"+strconv.Itoa(h), "-i", in,
		"-vf", "scale="+strconv.Itoa(w)+":-2:in_range=full:out_range=full,format=gbrp10le,"+
			"lut3d="+escapeFilterArg(cube)+":interp=tetrahedral",
		"-frames:v", "1", "-f", "rawvideo", "-pix_fmt", "gbrp10le", "-",
	).Output()
	if err != nil {
		t.Fatalf("ffmpeg: %v", err)
	}
	if len(out) != w*h*3*2 {
		t.Fatalf("ffmpeg returned %d bytes, want %d", len(out), w*h*3*2)
	}

	// gbrp10le is three planes in G, B, R order.
	plane := func(p, x int) float64 {
		off := (p*w*h + x) * 2
		return float64(binary.LittleEndian.Uint16(out[off:])) / 1023
	}

	for i, cv := range cvs {
		x := i * 2
		g, b, r := plane(0, x), plane(1, x), plane(2, x)
		wr, wg, wb := lutSample(transformSGamut3Cine.Mat, cv/1023, cv/1023, cv/1023)
		// CV 95 reads ~0.023 rather than 0: the LUT's lowest non-zero node sits
		// above it and gamma is near-vertical there. Inherent to the encoding,
		// not a bug, and invisible in a 720p thumbnail.
		if cv < 171 {
			if r > 0.04 || g > 0.04 || b > 0.04 {
				t.Errorf("CV %.0f: got (%.4f %.4f %.4f), expected near black", cv, r, g, b)
			}
			continue
		}
		for _, c := range []struct {
			name      string
			got, want float64
		}{{"R", r, wr}, {"G", g, wg}, {"B", b, wb}} {
			if math.Abs(c.got-c.want) > 0.003 {
				t.Errorf("CV %.0f %s: ffmpeg %.4f, Go %.4f (diff %.4f)",
					cv, c.name, c.got, c.want, c.got-c.want)
			}
		}
	}
}

// writeLook puts a cube with the given contents on disk and returns its path.
func writeLook(t *testing.T, dir, name, body string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(p), 0777); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, []byte(body), 0644); err != nil {
		t.Fatal(err)
	}
	return p
}

// A look replaces the whole technical conversion rather than layering on it,
// so it must land as its own transform with its own cache entry — two looks
// must never collide in one proxy tree, and a tree must record which was used.
func TestLookTransformIsSelfIdentifying(t *testing.T) {
	dir := t.TempDir()
	path := writeLook(t, dir, "Super Hero Final-33x.cube", "LUT_3D_SIZE 2\n")
	lt, err := lookTransform(path)
	if err != nil {
		t.Fatal(err)
	}
	if lt.passthrough() {
		t.Fatal("a look must not pass through")
	}
	if lt.Look != path {
		t.Errorf("source path not carried: %q", lt.Look)
	}
	// The cached name is derived from the look's own, so the proxy tree says
	// which look it was baked with.
	if !strings.HasPrefix(lt.LUT, "look_Super_Hero_Final-33x_") || !strings.HasSuffix(lt.LUT, ".cube") {
		t.Errorf("cache name = %q", lt.LUT)
	}
	// Spaces and punctuation must not reach a filesystem path or a filter arg.
	if strings.ContainsAny(lt.LUT, " '\\:") {
		t.Errorf("cache name is not filter-safe: %q", lt.LUT)
	}
	// Two different looks must not share a cache entry — including two that
	// share a filename in different directories.
	other, err := lookTransform(writeLook(t, dir, "Other/Moody.cube", "LUT_3D_SIZE 2\n# moody\n"))
	if err != nil {
		t.Fatal(err)
	}
	if other.LUT == lt.LUT || other.ID == lt.ID {
		t.Error("two looks collide in the cache")
	}
	same, err := lookTransform(writeLook(t, dir, "Elsewhere/Super Hero Final-33x.cube", "LUT_3D_SIZE 2\n# different\n"))
	if err != nil {
		t.Fatal(err)
	}
	if same.LUT == lt.LUT || same.ID == lt.ID {
		t.Error("two looks sharing a filename collide in the cache")
	}
	// The ID is what marks a cached clip stale when the look changes, so it
	// must differ from the technical transform it replaces.
	if lt.ID == transformSGamut3Cine.ID || lt.ID == transformNone.ID {
		t.Errorf("look ID %q does not distinguish itself", lt.ID)
	}
	// A look that is not there is an error, not a silent pass through: baking
	// the technical conversion instead grades every clip with something other
	// than what was asked for.
	if _, err := lookTransform(filepath.Join(dir, "absent.cube")); err == nil {
		t.Error("a missing look was accepted")
	}
}

// Editing a look in place must reach the proxies. The identity of a look is
// its contents, not its name: the ID marks every clip baked with the old cube
// stale, and the cache entry is a different file, so the rebuild reads the new
// cube rather than the copy already in the tree.
func TestLookEditedInPlaceIsPickedUp(t *testing.T) {
	dir := t.TempDir()
	path := writeLook(t, dir, "My Look.cube", "LUT_3D_SIZE 2\n0 0 0\n1 1 1\n")
	before, err := lookTransform(path)
	if err != nil {
		t.Fatal(err)
	}
	cache := t.TempDir()
	cubeBefore, err := ensureLUT(cache, before)
	if err != nil {
		t.Fatal(err)
	}

	writeLook(t, dir, "My Look.cube", "LUT_3D_SIZE 2\n0 0 0\n0.5 0.5 0.5\n")
	after, err := lookTransform(path)
	if err != nil {
		t.Fatal(err)
	}
	if after.ID == before.ID {
		t.Error("the edit left the transform ID unchanged — no clip is marked stale")
	}
	if after.LUT == before.LUT {
		t.Fatal("the edit left the cache entry unchanged — the old cube is served")
	}
	cubeAfter, err := ensureLUT(cache, after)
	if err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(cubeAfter)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(got), "0.5 0.5 0.5") {
		t.Errorf("cached cube is not the edited one: %q", got)
	}
	if cubeAfter == cubeBefore {
		t.Error("both versions share one cached path")
	}
}

// A look only ever displaces a conversion that was going to happen anyway.
// Rec.709 sources have no log to give the cube and must be left alone.
func TestLookOnlyDisplacesALogConversion(t *testing.T) {
	clips := []clipColour{
		{Gamma: "s-log3-cine", Prim: "s-gamut3-cine", Found: true},
		{Gamma: "rec709", Prim: "rec709", Found: true}, // GoPro / DJI
		{Card: "Drone_Andu"}, // GoPro / DJI, no sidecar to read
	}
	got := fillMissingColour(clips)
	if got[0].passthrough() {
		t.Fatal("the log clip should have had a conversion to displace")
	}
	if !got[1].passthrough() {
		t.Fatal("the Rec.709 clip should pass through before any look is applied")
	}
	if !got[2].passthrough() {
		t.Fatal("a card with no sidecar on it has nothing to inherit and must pass through")
	}
	// planMission substitutes only where a conversion already existed; mirror
	// that rule here so the invariant is pinned even if the caller moves.
	lt, err := lookTransform(writeLook(t, t.TempDir(), "x.cube", "LUT_3D_SIZE 2\n"))
	if err != nil {
		t.Fatal(err)
	}
	for i := range got {
		if !got[i].passthrough() {
			got[i] = lt
		}
	}
	if got[0].ID != lt.ID {
		t.Error("the log clip did not take the look")
	}
	if !got[1].passthrough() {
		t.Error("the Rec.709 clip was graded — it has no log to give the cube")
	}
	if !got[2].passthrough() {
		t.Error("the sidecar-less drone card was graded — it has no log to give the cube")
	}
}
