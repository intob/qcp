package main

import (
	"bytes"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// Colour handling for the browse tier.
//
// A log thumbnail is a flat grey wash and tells you nothing about the shot, so
// the browse tier bakes a log → Rec.709 transform. Sony only ships .Cine LUTs,
// which leaves 78% of the Sony footage here with no vendor LUT at all, so the
// transform is computed rather than loaded: S-Log3 decode, a gamut matrix built
// from the primaries, a tone-map with a real shoulder, then the Rec.709 OETF.
//
// The edit tier is never colour-transformed — see PROXIES.md.

// ── S-Log3 ──────────────────────────────────────────────────────────────────

// slog3Break is the code value where S-Log3 switches from its linear toe to the
// logarithmic segment.
const slog3Break = 171.2102946929

// slog3ToLinear converts a normalised S-Log3 code value (x = CV/1023) to scene
// linear. CV 95 is black, CV 420 is 18% grey.
func slog3ToLinear(x float64) float64 {
	cv := x * 1023
	if cv >= slog3Break {
		return math.Pow(10, (cv-420)/261.5)*(0.18+0.01) - 0.01
	}
	return (cv - 95) * 0.01125 / (slog3Break - 95)
}

// ── Gamut ───────────────────────────────────────────────────────────────────

// d65 is the white point every gamut here is defined against, so the matrices
// below need no chromatic adaptation.
const d65x, d65y = 0.3127, 0.3290

// primaries is a set of RGB primaries plus white, as CIE xy.
type primaries struct {
	rx, ry float64
	gx, gy float64
	bx, by float64
	wx, wy float64
}

var (
	sGamut3     = primaries{0.730, 0.280, 0.140, 0.855, 0.100, -0.050, d65x, d65y}
	sGamut3Cine = primaries{0.766, 0.275, 0.225, 0.800, 0.089, -0.087, d65x, d65y}
	rec709      = primaries{0.640, 0.330, 0.300, 0.600, 0.150, 0.060, d65x, d65y}
)

type mat3 [3][3]float64

// npm builds the normalised primary matrix taking linear RGB in this gamut to
// CIE XYZ.
func (p primaries) npm() mat3 {
	xyz := func(x, y float64) [3]float64 {
		return [3]float64{x / y, 1, (1 - x - y) / y}
	}
	r, g, b := xyz(p.rx, p.ry), xyz(p.gx, p.gy), xyz(p.bx, p.by)
	w := xyz(p.wx, p.wy)

	// Solve P·c = W for the per-primary scale factors that make (1,1,1) land
	// exactly on the white point.
	var pm mat3
	for i := 0; i < 3; i++ {
		pm[i][0], pm[i][1], pm[i][2] = r[i], g[i], b[i]
	}
	c := pm.inverse().apply(w)

	var m mat3
	for i := 0; i < 3; i++ {
		m[i][0], m[i][1], m[i][2] = r[i]*c[0], g[i]*c[1], b[i]*c[2]
	}
	return m
}

func (m mat3) apply(v [3]float64) [3]float64 {
	var out [3]float64
	for i := 0; i < 3; i++ {
		out[i] = m[i][0]*v[0] + m[i][1]*v[1] + m[i][2]*v[2]
	}
	return out
}

func (a mat3) mul(b mat3) mat3 {
	var out mat3
	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			out[i][j] = a[i][0]*b[0][j] + a[i][1]*b[1][j] + a[i][2]*b[2][j]
		}
	}
	return out
}

func (m mat3) inverse() mat3 {
	det := m[0][0]*(m[1][1]*m[2][2]-m[1][2]*m[2][1]) -
		m[0][1]*(m[1][0]*m[2][2]-m[1][2]*m[2][0]) +
		m[0][2]*(m[1][0]*m[2][1]-m[1][1]*m[2][0])
	var out mat3
	out[0][0] = (m[1][1]*m[2][2] - m[1][2]*m[2][1]) / det
	out[0][1] = (m[0][2]*m[2][1] - m[0][1]*m[2][2]) / det
	out[0][2] = (m[0][1]*m[1][2] - m[0][2]*m[1][1]) / det
	out[1][0] = (m[1][2]*m[2][0] - m[1][0]*m[2][2]) / det
	out[1][1] = (m[0][0]*m[2][2] - m[0][2]*m[2][0]) / det
	out[1][2] = (m[0][2]*m[1][0] - m[0][0]*m[1][2]) / det
	out[2][0] = (m[1][0]*m[2][1] - m[1][1]*m[2][0]) / det
	out[2][1] = (m[0][1]*m[2][0] - m[0][0]*m[2][1]) / det
	out[2][2] = (m[0][0]*m[1][1] - m[0][1]*m[1][0]) / det
	return out
}

// gamutMatrix returns the 3×3 taking linear RGB in src to linear Rec.709.
// Both gamuts share D65, so every row sums to 1 and neutrals stay neutral.
func gamutMatrix(src primaries) mat3 {
	return rec709.npm().inverse().mul(src.npm())
}

// ── Tone-map and OETF ───────────────────────────────────────────────────────

// tonemapWhite is where the shoulder puts the white point, in stops over middle
// grey: 8.0 linear is +5.5 stops. A technical CST without a shoulder blows the
// sky and the wing to paper white — the median pixel on a representative flying
// shot is +3.31 stops and p95 is +5.25, well past what Rec.709 holds.
const tonemapWhite = 8.0

// displayGamma is the Rec.709 OETF applied after the tone-map.
const displayGamma = 2.4

// tonemap is extended Reinhard: 18% grey lands at 0.457 and tonemapWhite maps
// to exactly 1.0.
func tonemap(x float64) float64 {
	return x * (1 + x/(tonemapWhite*tonemapWhite)) / (1 + x)
}

// encodeRec709 clamps and gamma-encodes a tone-mapped linear value.
func encodeRec709(lin float64) float64 {
	v := tonemap(lin)
	if v <= 0 {
		return 0
	}
	if v >= 1 {
		return 1
	}
	return math.Pow(v, 1/displayGamma)
}

// ── LUT generation ──────────────────────────────────────────────────────────

// lutSize is the cube edge. 65³ was tested and changed nothing meaningful: the
// residual black lift of ~6/255 comes from gamma-encoding steepness near zero,
// not grid resolution.
const lutSize = 33

// lutSample runs one RGB triple through the whole chain: log decode per
// channel, gamut matrix, clamp to >= 0, tone-map, gamma.
func lutSample(m mat3, r, g, b float64) (float64, float64, float64) {
	lin := m.apply([3]float64{
		slog3ToLinear(r),
		slog3ToLinear(g),
		slog3ToLinear(b),
	})
	for i := range lin {
		if lin[i] < 0 {
			lin[i] = 0
		}
	}
	return encodeRec709(lin[0]), encodeRec709(lin[1]), encodeRec709(lin[2])
}

// buildCube renders the transform as a 33³ .cube. The LUT is computed against
// raw code values, so the ffmpeg chain that applies it must use in_range=full —
// see the range trap in PROXIES.md.
func buildCube(title string, m mat3) []byte {
	var buf bytes.Buffer
	fmt.Fprintf(&buf, "# Generated by qcp — S-Log3 to Rec.709, extended Reinhard shoulder at W=%g.\n", tonemapWhite)
	fmt.Fprintf(&buf, "# Computed against raw code values: apply with in_range=full.\n")
	fmt.Fprintf(&buf, "TITLE \"%s\"\n", title)
	fmt.Fprintf(&buf, "LUT_3D_SIZE %d\n", lutSize)
	fmt.Fprintf(&buf, "DOMAIN_MIN 0 0 0\n")
	fmt.Fprintf(&buf, "DOMAIN_MAX 1 1 1\n")
	// .cube varies red fastest.
	for bi := 0; bi < lutSize; bi++ {
		for gi := 0; gi < lutSize; gi++ {
			for ri := 0; ri < lutSize; ri++ {
				const last = lutSize - 1
				r, g, b := lutSample(m,
					float64(ri)/last, float64(gi)/last, float64(bi)/last)
				fmt.Fprintf(&buf, "%.6f %.6f %.6f\n", r, g, b)
			}
		}
	}
	return buf.Bytes()
}

// ── Transform selection ─────────────────────────────────────────────────────

// colourTransform is the conversion baked into one clip's browse proxy. The
// zero value passes through: the clip is already Rec.709 (S-Cinetone, GoPro,
// DJI) and touching it would be wrong.
type colourTransform struct {
	// ID is recorded per clip in the proxy manifest so the choice stays
	// auditable, and so the index can filter on it.
	ID  string
	LUT string // .cube basename in the LUT cache; "" means pass through
	Mat mat3

	// Look is the absolute path to a user-supplied creative cube, copied into
	// the cache rather than generated. A look takes S-Log3 straight to finished
	// Rec.709, so it replaces the whole technical chain — gamut matrix,
	// tone-map and gamma encode alike — and nothing may be applied after it.
	Look string
}

func (t colourTransform) passthrough() bool { return t.LUT == "" }

func (t colourTransform) String() string {
	if t.passthrough() {
		return "none"
	}
	return t.ID
}

var (
	transformNone        = colourTransform{ID: "none"}
	transformSGamut3     = colourTransform{ID: "s-log3/s-gamut3", LUT: "slog3_sgamut3_to_rec709.cube", Mat: gamutMatrix(sGamut3)}
	transformSGamut3Cine = colourTransform{ID: "s-log3/s-gamut3-cine", LUT: "slog3_sgamut3cine_to_rec709.cube", Mat: gamutMatrix(sGamut3Cine)}
)

// lookTransform is the transform for a clip when a creative look is
// configured. The cube carries S-Log3 all the way to finished Rec.709, so the
// gamut matrix, tone-map and gamma encode it would otherwise go through are all
// replaced — a look is the whole conversion, not a layer on top of one.
//
// The cached basename is derived from the look's own name so two different
// looks cannot collide in one proxy tree, and so a tree records which was used.
func lookTransform(path string) colourTransform {
	base := strings.TrimSuffix(filepath.Base(path), filepath.Ext(path))
	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			return r
		}
		return '_'
	}, base)
	return colourTransform{ID: "look/" + base, LUT: "look_" + safe + ".cube", Look: path}
}

// pickTransform maps a clip's capture gamma and colour primaries onto a
// transform. Anything not recognised passes through: baking a guessed
// conversion is worse than leaving the clip alone.
func pickTransform(gamma, prim string) colourTransform {
	g := strings.ToLower(strings.TrimSpace(gamma))
	p := strings.ToLower(strings.TrimSpace(prim))
	if !strings.HasPrefix(g, "s-log3") {
		return transformNone
	}
	switch {
	case strings.HasPrefix(p, "s-gamut3-cine"), strings.HasPrefix(p, "s-gamut3.cine"):
		return transformSGamut3Cine
	case strings.HasPrefix(p, "s-gamut3"):
		return transformSGamut3
	}
	return transformNone
}

// ensureLUT writes t's cube into dir unless it is already there, and returns
// its path. Nothing binary is committed to the repo and nothing is downloaded:
// generating a 33³ cube costs milliseconds.
func ensureLUT(dir string, t colourTransform) (string, error) {
	if t.passthrough() {
		return "", nil
	}
	path := filepath.Join(dir, t.LUT)
	if fi, err := os.Stat(path); err == nil && fi.Size() > 0 {
		return path, nil
	}
	if err := os.MkdirAll(dir, 0777); err != nil {
		return "", err
	}
	// A look is copied, not generated: it lives beside the proxies it was
	// baked into so the tree stays self-describing once the original moves.
	var data []byte
	if t.Look != "" {
		raw, err := os.ReadFile(t.Look)
		if err != nil {
			return "", fmt.Errorf("look %s: %w", t.Look, err)
		}
		data = raw
	} else {
		data = buildCube(t.ID, t.Mat)
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0644); err != nil {
		return "", err
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return "", err
	}
	return path, nil
}

// ── Sidecar detection ───────────────────────────────────────────────────────

var sidecarItemRe = regexp.MustCompile(`<Item\s+name="([^"]+)"\s+value="([^"]*)"`)

// sidecarPath returns the Sony XDCAM sidecar beside a clip, or "" if there is
// none. "923_0272.MXF" → "923_0272M01.XML".
func sidecarPath(clip string) string {
	stem := strings.TrimSuffix(clip, filepath.Ext(clip))
	for _, suffix := range []string{"M01.XML", "M01.xml"} {
		p := stem + suffix
		if fi, err := os.Stat(p); err == nil && !fi.IsDir() {
			return p
		}
	}
	return ""
}

// readSidecarColour extracts CaptureGammaEquation and CaptureColorPrimaries
// from a Sony sidecar. Detection is free — it is right there next to every
// Sony clip — so it is always worth doing properly rather than assuming one
// transform for the whole library.
func readSidecarColour(clip string) (gamma, prim string, ok bool) {
	p := sidecarPath(clip)
	if p == "" {
		return "", "", false
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return "", "", false
	}
	for _, m := range sidecarItemRe.FindAllStringSubmatch(string(data), -1) {
		switch m[1] {
		case "CaptureGammaEquation":
			gamma = m[2]
		case "CaptureColorPrimaries":
			prim = m[2]
		}
	}
	if gamma == "" && prim == "" {
		return "", "", false
	}
	return gamma, prim, true
}

// clipColour is what a clip's sidecar said about how it was shot, and which
// card it came off. Found is false when there was no sidecar to read.
type clipColour struct {
	// Card is the card volume subfolder, "" for a flat mission. It is the
	// scope inheritance works within — see fillMissingColour.
	Card  string
	Gamma string
	Prim  string
	Found bool
}

// fillMissingColour resolves the transform for every clip in one mission,
// giving a clip with no sidecar the most common transform among the clips that
// share its card. Seven of the 2,530 MXF in the library are missing a sidecar
// and capture settings do not change mid-card, so a card is the right scope to
// inherit within. It has to be the card and not the mission, because a mission
// routinely holds more than one camera: 002_Portugal pairs 147 Sony MXF with 19
// DJI clips under Drone_Andu/, and inheriting mission-wide handed the Sony
// S-Log3 conversion to every one of the DJI ones — which, once a look was
// configured, baked an S-Log3 grade onto footage that never was S-Log3.
//
// A card with no sidecar anywhere on it has nothing to inherit and passes
// through, which is the right answer for a card full of GoPro or DJI clips.
func fillMissingColour(clips []clipColour) []colourTransform {
	out := make([]colourTransform, len(clips))
	counts := make(map[string]map[string]int)
	byID := make(map[string]colourTransform)
	for i, c := range clips {
		if !c.Found {
			continue
		}
		t := pickTransform(c.Gamma, c.Prim)
		out[i] = t
		if counts[c.Card] == nil {
			counts[c.Card] = make(map[string]int)
		}
		counts[c.Card][t.ID]++
		byID[t.ID] = t
	}

	fallback := make(map[string]colourTransform, len(counts))
	for card, tally := range counts {
		best, bestN := transformNone, 0
		// Ties break on ID so a re-run picks the same transform as the last one.
		for id, n := range tally {
			if n > bestN || (n == bestN && id < best.ID) {
				best, bestN = byID[id], n
			}
		}
		fallback[card] = best
	}
	for i, c := range clips {
		if c.Found {
			continue
		}
		// Explicitly transformNone rather than the zero value, so a card with
		// nothing to inherit records "none" in the manifest like any other
		// pass-through clip instead of an empty transform ID.
		t, ok := fallback[c.Card]
		if !ok {
			t = transformNone
		}
		out[i] = t
	}
	return out
}
