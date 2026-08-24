package main

import (
	"encoding/binary"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
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
// change mid-card, so those inherit whatever the rest of the mission used.
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
