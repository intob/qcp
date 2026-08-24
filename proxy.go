package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/vbauerster/mpb/v8"
)

// -proxy generates derived renditions of every clip in a mission: a 720p
// browse tier that the index is built from, and, only when asked for, a ProRes
// edit tier.
//
// Nothing here is archived. The proxy tree lives outside the footage root, is
// never synced to cold storage, and can always be regenerated — see the design
// rules in PROXIES.md.

// proxyRootName is the directory proxy trees live in, at the drive root rather
// than under the footage root. Nothing that walks a year directory can then
// mistake derived media for mission content.
const proxyRootName = "proxies"

const (
	proxyManifestName = "proxies.b3"
	proxyMetaName     = "proxies.json"
	lutDirName        = "luts"
)

// videoExts are the extensions treated as clips. Sidecars, stills and GoPro's
// own .lrv/.thm companions are not proxied.
var videoExts = map[string]bool{
	".mxf": true, ".mp4": true, ".mov": true, ".m4v": true,
	".avi": true, ".mts": true, ".m2ts": true, ".mpg": true,
	".mpeg": true, ".mkv": true, ".wmv": true, ".3gp": true,
	".insv": true, ".mod": true, ".vob": true,
}

// isProxyDir reports whether a directory name is a proxy tree. Proxies live at
// the drive root rather than inside the footage root, so nothing that walks a
// year directory should see one — this is the guard that keeps it that way if
// the layout ever moves. Proxies are derived and regenerable and cold space is
// the scarce resource, so -sync and -replicate must never carry them across.
func isProxyDir(name string) bool { return name == proxyRootName }

func isVideoFile(rel string) bool {
	return videoExts[strings.ToLower(filepath.Ext(rel))]
}

// proxyTiers is which renditions a run was asked for.
type proxyTiers struct {
	edit   bool
	browse bool
}

func parseTiers(s string) (proxyTiers, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "browse":
		return proxyTiers{browse: true}, nil
	case "edit":
		return proxyTiers{edit: true}, nil
	case "both":
		return proxyTiers{edit: true, browse: true}, nil
	}
	return proxyTiers{}, fmt.Errorf("unknown tier %q — use browse, edit or both", s)
}

// clipMeta is one clip's entry in proxies.json: enough to skip regeneration,
// to build the index without touching the footage again, and to audit which
// colour transform was baked in and why.
type clipMeta struct {
	Rel      string `json:"rel"`  // source path, relative to the mission directory
	Card     string `json:"card"` // card volume subfolder, "" for a flat mission
	Size     int64  `json:"size"`
	SrcHash  string `json:"src_hash,omitempty"` // BLAKE3 from checksums.b3, when it has one
	SrcMtime int64  `json:"src_mtime"`          // staleness fallback where no hash is recorded

	Duration float64 `json:"duration"`
	Width    int     `json:"width"`
	Height   int     `json:"height"`
	FPS      float64 `json:"fps"`
	Codec    string  `json:"codec"`

	Gamma     string `json:"gamma,omitempty"`
	Primaries string `json:"primaries,omitempty"`
	Sidecar   bool   `json:"sidecar"`   // false = inherited from the rest of the mission
	Transform string `json:"transform"` // transform ID actually applied

	Browse string `json:"browse,omitempty"` // paths relative to the mission proxy directory
	Edit   string `json:"edit,omitempty"`
	Poster string `json:"poster,omitempty"`
	Sprite string `json:"sprite,omitempty"`
}

// stale reports whether the source has changed since this entry was written.
// The BLAKE3 comes from checksums.b3 rather than a second full read of the
// footage; missions that predate the manifest fall back to size and mtime.
func (c clipMeta) stale(size int64, mtime int64, hash string) bool {
	if c.SrcHash != "" && hash != "" {
		return c.SrcHash != hash
	}
	return c.Size != size || c.SrcMtime != mtime
}

type proxyManifest struct {
	Version int        `json:"version"`
	Year    int        `json:"year"`
	Mission string     `json:"mission"`
	Clips   []clipMeta `json:"clips"`
}

const proxyManifestVersion = 1

func readProxyManifest(dir string) proxyManifest {
	var m proxyManifest
	data, err := os.ReadFile(filepath.Join(dir, proxyMetaName))
	if err != nil {
		return m
	}
	if err := json.Unmarshal(data, &m); err != nil {
		return proxyManifest{}
	}
	return m
}

func (m proxyManifest) byRel() map[string]clipMeta {
	out := make(map[string]clipMeta, len(m.Clips))
	for _, c := range m.Clips {
		out[c.Rel] = c
	}
	return out
}

// ── paths ───────────────────────────────────────────────────────────────────

func proxyRoot(base string) string { return filepath.Join(base, proxyRootName) }

func proxyMissionDir(base string, year int, slug string) string {
	return filepath.Join(proxyRoot(base), strconv.Itoa(year), slug)
}

// proxyRel maps a source path relative to the mission onto its rendition,
// keeping the stem and changing the extension. Resolve links externally
// generated proxies by filename excluding extension, so the stem has to
// survive — see PROXIES.md.
func proxyRel(tier, rel, ext string) string {
	return filepath.Join(tier, strings.TrimSuffix(rel, filepath.Ext(rel))+ext)
}

// escapeFilterArg quotes a path for use inside an ffmpeg filter argument,
// where a colon separates options and a comma separates filters.
func escapeFilterArg(p string) string {
	r := strings.NewReplacer(`\`, `\\`, `:`, `\:`, `,`, `\,`, `'`, `\'`,
		`[`, `\[`, `]`, `\]`, `;`, `\;`)
	return r.Replace(p)
}

// ── ffprobe ─────────────────────────────────────────────────────────────────

type probeResult struct {
	Duration float64
	Width    int
	Height   int
	FPS      float64
	Codec    string
	HasAudio bool
}

type ffprobeStreams struct {
	Streams []struct {
		CodecType    string `json:"codec_type"`
		CodecName    string `json:"codec_name"`
		Width        int    `json:"width"`
		Height       int    `json:"height"`
		RFrameRate   string `json:"r_frame_rate"`
		AvgFrameRate string `json:"avg_frame_rate"`
		Duration     string `json:"duration"`
	} `json:"streams"`
	Format struct {
		Duration string `json:"duration"`
	} `json:"format"`
}

func probeClip(path string) (probeResult, error) {
	out, err := exec.Command("ffprobe", "-v", "quiet", "-print_format", "json",
		"-show_entries", "stream=codec_type,codec_name,width,height,r_frame_rate,avg_frame_rate,duration:format=duration",
		path).Output()
	if err != nil {
		return probeResult{}, err
	}
	var raw ffprobeStreams
	if err := json.Unmarshal(out, &raw); err != nil {
		return probeResult{}, err
	}
	var r probeResult
	r.Duration, _ = strconv.ParseFloat(raw.Format.Duration, 64)
	for _, s := range raw.Streams {
		switch s.CodecType {
		case "video":
			if r.Codec != "" {
				continue // first video stream only; some cameras attach a thumbnail
			}
			r.Codec, r.Width, r.Height = s.CodecName, s.Width, s.Height
			r.FPS = parseRational(s.AvgFrameRate)
			if r.FPS == 0 {
				r.FPS = parseRational(s.RFrameRate)
			}
			if r.Duration == 0 {
				r.Duration, _ = strconv.ParseFloat(s.Duration, 64)
			}
		case "audio":
			r.HasAudio = true
		}
	}
	if r.Codec == "" {
		return r, fmt.Errorf("no video stream")
	}
	return r, nil
}

func parseRational(s string) float64 {
	num, den, ok := strings.Cut(s, "/")
	n, err := strconv.ParseFloat(num, 64)
	if err != nil {
		return 0
	}
	if !ok {
		return n
	}
	d, err := strconv.ParseFloat(den, 64)
	if err != nil || d == 0 {
		return 0
	}
	return n / d
}

// ── encoding ────────────────────────────────────────────────────────────────

// browseFilter builds the browse-tier filter chain. The scale comes before the
// LUT because scaling first is materially cheaper, and in_range=full is not
// optional: the generated cubes are computed against raw code values, so
// in_range=limited is off by up to 14/255 — see the range trap in PROXIES.md.
func browseFilter(lut string) string {
	if lut == "" {
		return "scale=1280:-2"
	}
	return "scale=1280:-2:in_range=full:out_range=full,format=gbrp10le," +
		"lut3d=" + escapeFilterArg(lut) + ":interp=tetrahedral,format=yuv420p"
}

// encodeArgs builds one ffmpeg invocation covering every rendition a clip still
// needs. Both tiers come off a single decode: that costs 3.20× realtime against
// 4.19× for the browse leg alone, so the second rendition is nearly free — the
// same instinct as reading a source once and fanning out to N writers.
//
// The audio map is optional (0:a:0?) so a silent clip still encodes.
func encodeArgs(src string, hasAudio bool, edit, browse, lut string) []string {
	args := []string{
		"-nostdin", "-y", "-hide_banner", "-loglevel", "error",
		"-progress", "pipe:1", "-nostats",
		"-i", src,
	}
	amap := []string{"-map", "0:a:0?"}
	if !hasAudio {
		amap = nil
	}
	if edit != "" {
		// Never colour-transformed. If the proxy is graded and the camera
		// original is not, toggling proxies in Resolve makes the image jump.
		args = append(args, "-map", "0:v:0")
		args = append(args, amap...)
		args = append(args,
			"-vf", "scale=1920:-2",
			"-c:v", "prores_videotoolbox", "-profile:v", "0")
		if hasAudio {
			args = append(args, "-c:a", "pcm_s16le")
		}
		args = append(args, edit)
	}
	if browse != "" {
		args = append(args, "-map", "0:v:0")
		args = append(args, amap...)
		args = append(args,
			"-vf", browseFilter(lut),
			"-c:v", "h264_videotoolbox", "-b:v", "2500k")
		if hasAudio {
			args = append(args, "-c:a", "aac", "-b:a", "96k")
		}
		args = append(args, "-movflags", "+faststart", browse)
	}
	return args
}

// posterArgs and spriteArgs both work off the browse proxy rather than the
// source. Off an MXF a sprite sheet costs a full decode — 44s for a 7-minute
// clip — because it has to walk every frame; off the 720p proxy it is nearly
// free.
func posterArgs(browse, out string, duration float64) []string {
	at := 5.0
	if duration > 0 && duration < 2*at {
		at = duration / 2
	}
	return []string{"-nostdin", "-y", "-hide_banner", "-loglevel", "error",
		"-ss", strconv.FormatFloat(at, 'f', 3, 64), "-i", browse,
		"-frames:v", "1", "-vf", "scale=480:-2", "-q:v", "4", out}
}

const spriteTiles = 100

func spriteArgs(browse, out string, duration float64) []string {
	// One frame per 1/100th of the clip, tiled 10×10.
	rate := float64(spriteTiles) / duration
	return []string{"-nostdin", "-y", "-hide_banner", "-loglevel", "error",
		"-i", browse,
		"-vf", fmt.Sprintf("fps=%s,scale=192:-2,tile=10x10", strconv.FormatFloat(rate, 'f', 6, 64)),
		"-frames:v", "1", "-q:v", "5", out}
}

// runFFmpeg runs one invocation, reporting progress as a fraction of the
// clip's duration so the bar advances within a long clip rather than jumping
// per file. Any stderr is folded into the error.
func runFFmpeg(args []string, duration float64, onProgress func(frac float64)) error {
	cmd := exec.Command("ffmpeg", args...)
	var errBuf strings.Builder
	cmd.Stderr = &errBuf

	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			key, val, ok := strings.Cut(sc.Text(), "=")
			if !ok || key != "out_time_us" || onProgress == nil || duration <= 0 {
				continue
			}
			us, err := strconv.ParseFloat(strings.TrimSpace(val), 64)
			if err != nil {
				continue
			}
			if frac := us / 1e6 / duration; frac >= 0 {
				onProgress(min(frac, 1))
			}
		}
		io.Copy(io.Discard, stdout)
	}()
	<-done
	if err := cmd.Wait(); err != nil {
		msg := strings.TrimSpace(errBuf.String())
		if msg == "" {
			return err
		}
		return fmt.Errorf("%v: %s", err, lastLine(msg))
	}
	return nil
}

func lastLine(s string) string {
	lines := strings.Split(strings.TrimSpace(s), "\n")
	return strings.TrimSpace(lines[len(lines)-1])
}

// encodeOutputs runs one ffmpeg invocation, writing every output through a
// temporary name and renaming on success, so an interrupted run never leaves a
// truncated file that looks finished. Temporaries keep the real extension
// because ffmpeg picks the muxer from it.
func encodeOutputs(outs []string, build func(tmps []string) []string, duration float64, onProgress func(float64)) error {
	tmps := make([]string, len(outs))
	for i, out := range outs {
		if err := os.MkdirAll(filepath.Dir(out), 0777); err != nil {
			return err
		}
		ext := filepath.Ext(out)
		tmps[i] = strings.TrimSuffix(out, ext) + ".qcp-part" + ext
	}
	cleanup := func() {
		for _, t := range tmps {
			os.Remove(t)
		}
	}
	if err := runFFmpeg(build(tmps), duration, onProgress); err != nil {
		cleanup()
		return err
	}
	for i, out := range outs {
		if err := os.Rename(tmps[i], out); err != nil {
			cleanup()
			return err
		}
	}
	return nil
}

// ── mission resolution ──────────────────────────────────────────────────────

// proxySource is a mission located on whichever mounted drive can supply it
// fastest.
type proxySource struct {
	num   int
	slug  string
	dir   string
	vol   string
	base  string
	clips []fileEntry
	size  int64
	// hashes is the mission's checksums.b3, used to detect a source that has
	// changed under an existing proxy without reading the footage twice.
	hashes map[string]string
}

// resolveProxySource picks the drive holding the most of a mission, preferring
// hot drives: the archive HDD reads at 104 MB/s against T9's 996 MB/s, so
// sourcing from a hot drive where the mission exists there is 10× faster.
func resolveProxySource(cfg Config, yearStr string, num int) (proxySource, error) {
	slug, err := findMissionSlug(cfg.Drives, yearStr, num)
	if err != nil {
		return proxySource{}, err
	}
	return proxySourceForSlug(cfg, yearStr, slug)
}

func proxySourceForSlug(cfg Config, yearStr, slug string) (proxySource, error) {
	num, _ := strconv.Atoi(strings.SplitN(slug, "_", 2)[0])
	var best proxySource
	bestScore := -1
	for _, d := range cfg.Drives {
		dir := filepath.Join(d.basePath(), d.Root, yearStr, slug)
		if !dirExists(dir) {
			continue
		}
		found, err := findFiles(dir)
		if err != nil {
			continue
		}
		var clips []fileEntry
		var size int64
		for _, f := range found {
			if isVideoFile(f.rel) {
				clips = append(clips, f)
				size += f.size
			}
		}
		// Rank on clip count first, then prefer a hot drive.
		score := len(clips)*2 + btoi(d.Role == "hot")
		if score > bestScore {
			bestScore = score
			best = proxySource{
				num: num, slug: slug, dir: dir, vol: d.name(), base: d.basePath(),
				clips: clips, size: size,
				hashes: readChecksumFile(filepath.Join(dir, "checksums.b3")),
			}
		}
	}
	if bestScore < 0 {
		return proxySource{}, fmt.Errorf("mission %s not found on any mounted drive", slug)
	}
	if len(best.clips) == 0 {
		return proxySource{}, fmt.Errorf("mission %s holds no video files", best.slug)
	}
	sort.Slice(best.clips, func(i, j int) bool { return best.clips[i].rel < best.clips[j].rel })
	return best, nil
}

// missionsInYear lists every numbered mission on the mounted drives.
func missionsInYear(cfg Config, yearStr string) []int {
	seen := make(map[int]bool)
	var nums []int
	for _, d := range cfg.Drives {
		entries, err := os.ReadDir(filepath.Join(d.basePath(), d.Root, yearStr))
		if err != nil {
			continue
		}
		for _, e := range entries {
			if !e.IsDir() || !isNumberedMission(e.Name()) {
				continue
			}
			n, err := strconv.Atoi(strings.SplitN(e.Name(), "_", 2)[0])
			if err != nil || seen[n] {
				continue
			}
			seen[n] = true
			nums = append(nums, n)
		}
	}
	sort.Ints(nums)
	return nums
}

// ── planning ────────────────────────────────────────────────────────────────

// clipJob is one clip's work: the renditions it is missing, and everything the
// manifest needs to record about it.
type clipJob struct {
	rel      string
	src      string
	size     int64
	mtime    int64
	srcHash  string
	colour   clipColour
	trans    colourTransform
	meta     clipMeta // carried forward from a previous run where unchanged
	cached   bool
	needEdit bool
	needBrow bool
	needStil bool
}

func (j clipJob) work() bool { return j.needEdit || j.needBrow || j.needStil }

// missionPlan is one mission's proxy work, resolved against what is already on
// the destination drive.
type missionPlan struct {
	src      proxySource
	outDir   string
	existing map[string]clipMeta
	jobs     []clipJob
	todo     int
	todoSize int64
}

// planMission decides, per clip, what still has to be generated. A clip is
// skipped when its renditions are on disk and the recorded source hash still
// matches — the manifest also carries the parsed sidecar, so a re-run does not
// re-read every XML.
func planMission(src proxySource, outDir string, tiers proxyTiers) missionPlan {
	p := missionPlan{src: src, outDir: outDir, existing: readProxyManifest(outDir).byRel()}

	// Sidecars are only read for clips with no usable cached entry.
	colours := make([]clipColour, len(src.clips))
	cached := make([]bool, len(src.clips))
	jobs := make([]clipJob, len(src.clips))

	for i, f := range src.clips {
		j := clipJob{rel: f.rel, src: filepath.Join(src.dir, f.rel), size: f.size}
		if fi, err := os.Stat(j.src); err == nil {
			j.mtime = fi.ModTime().Unix()
		}
		j.srcHash = src.hashes[f.rel]

		prev, ok := p.existing[f.rel]
		if ok && !prev.stale(j.size, j.mtime, j.srcHash) {
			j.meta, j.cached, cached[i] = prev, true, true
			colours[i] = clipColour{Gamma: prev.Gamma, Prim: prev.Primaries, Found: prev.Sidecar}
		} else {
			gamma, prim, found := readSidecarColour(j.src)
			colours[i] = clipColour{Gamma: gamma, Prim: prim, Found: found}
		}
		jobs[i] = j
	}

	transforms := fillMissingColour(colours)
	for i := range jobs {
		jobs[i].colour = colours[i]
		jobs[i].trans = transforms[i]

		rel := jobs[i].rel
		editRel := proxyRel("edit", rel, ".mov")
		browseRel := proxyRel("browse", rel, ".mp4")
		posterRel := proxyRel("stills", rel, ".poster.jpg")
		spriteRel := proxyRel("stills", rel, ".sprite.jpg")

		if tiers.edit {
			jobs[i].needEdit = !fileExists(filepath.Join(outDir, editRel)) || !cached[i]
		}
		if tiers.browse {
			jobs[i].needBrow = !fileExists(filepath.Join(outDir, browseRel)) || !cached[i]
			// A clip whose duration could not be read has no sprite to make, so
			// a missing one is not work — otherwise every run redoes its poster.
			wantSprite := !cached[i] || jobs[i].meta.Duration > 0
			jobs[i].needStil = jobs[i].needBrow ||
				!fileExists(filepath.Join(outDir, posterRel)) ||
				(wantSprite && !fileExists(filepath.Join(outDir, spriteRel)))
		}
		// A cached entry whose transform selection has changed — a sidecar
		// appearing elsewhere in the mission can move a fallback clip — has to
		// be re-baked, since the browse tier carries the grade.
		if tiers.browse && cached[i] && jobs[i].meta.Transform != transforms[i].ID {
			jobs[i].needBrow, jobs[i].needStil = true, true
		}
		if jobs[i].work() {
			p.todo++
			p.todoSize += jobs[i].size
		}
	}
	p.jobs = jobs
	return p
}

func fileExists(p string) bool {
	fi, err := os.Stat(p)
	return err == nil && !fi.IsDir() && fi.Size() > 0
}

// ── generation ──────────────────────────────────────────────────────────────

// proxyWorkers bounds concurrent ffmpeg invocations. The source drive's read
// limit still applies — the archive HDD collapses under parallel seeks — and
// the CPU cap keeps a fast SSD from spawning one decode per core.
func proxyWorkers(info driveInfo) int {
	n := max(1, runtime.NumCPU()/4)
	if info.concurrency < n {
		n = info.concurrency
	}
	return n
}

// generateClip produces whatever renditions the clip is missing and returns its
// manifest entry. It never touches the source beyond reading it.
func generateClip(j *clipJob, outDir, lutDir string, tiers proxyTiers, onProgress func(float64)) (clipMeta, []string, error) {
	meta := j.meta
	meta.Rel = j.rel
	meta.Card = cardOf(j.rel)
	meta.Size = j.size
	meta.SrcMtime = j.mtime
	meta.SrcHash = j.srcHash
	meta.Gamma = j.colour.Gamma
	meta.Primaries = j.colour.Prim
	meta.Sidecar = j.colour.Found
	meta.Transform = j.trans.ID

	var probe probeResult
	haveProbe := false
	if !j.cached || meta.Duration == 0 || meta.Codec == "" {
		p, err := probeClip(j.src)
		if err != nil {
			return meta, nil, fmt.Errorf("ffprobe: %w", err)
		}
		probe, haveProbe = p, true
		meta.Duration, meta.Width, meta.Height = p.Duration, p.Width, p.Height
		meta.FPS, meta.Codec = p.FPS, p.Codec
	}

	editRel := proxyRel("edit", j.rel, ".mov")
	browseRel := proxyRel("browse", j.rel, ".mp4")
	posterRel := proxyRel("stills", j.rel, ".poster.jpg")
	spriteRel := proxyRel("stills", j.rel, ".sprite.jpg")

	var written []string

	if j.needEdit || j.needBrow {
		var outs []string
		var editIdx, browseIdx = -1, -1
		if j.needEdit {
			editIdx = len(outs)
			outs = append(outs, filepath.Join(outDir, editRel))
		}
		if j.needBrow {
			browseIdx = len(outs)
			outs = append(outs, filepath.Join(outDir, browseRel))
		}
		lut := ""
		if j.needBrow && !j.trans.passthrough() {
			p, err := ensureLUT(lutDir, j.trans)
			if err != nil {
				return meta, nil, fmt.Errorf("lut: %w", err)
			}
			lut = p
		}
		hasAudio := !haveProbe || probe.HasAudio
		err := encodeOutputs(outs, func(tmps []string) []string {
			edit, browse := "", ""
			if editIdx >= 0 {
				edit = tmps[editIdx]
			}
			if browseIdx >= 0 {
				browse = tmps[browseIdx]
			}
			return encodeArgs(j.src, hasAudio, edit, browse, lut)
		}, meta.Duration, onProgress)
		if err != nil {
			return meta, nil, err
		}
		if j.needEdit {
			written = append(written, editRel)
		}
		if j.needBrow {
			written = append(written, browseRel)
		}
	}
	if j.needEdit || fileExists(filepath.Join(outDir, editRel)) {
		meta.Edit = editRel
	}
	if j.needBrow || fileExists(filepath.Join(outDir, browseRel)) {
		meta.Browse = browseRel
	}

	// Stills come off the browse proxy: a sprite sheet decodes every frame, and
	// doing that against an MXF costs 44s for a 7-minute clip.
	if j.needStil && meta.Browse != "" {
		browse := filepath.Join(outDir, meta.Browse)
		poster := filepath.Join(outDir, posterRel)
		if err := encodeOutputs([]string{poster}, func(t []string) []string {
			return posterArgs(browse, t[0], meta.Duration)
		}, 0, nil); err != nil {
			return meta, written, fmt.Errorf("poster: %w", err)
		}
		meta.Poster, written = posterRel, append(written, posterRel)

		if meta.Duration > 0 {
			sprite := filepath.Join(outDir, spriteRel)
			if err := encodeOutputs([]string{sprite}, func(t []string) []string {
				return spriteArgs(browse, t[0], meta.Duration)
			}, 0, nil); err != nil {
				return meta, written, fmt.Errorf("sprite: %w", err)
			}
			meta.Sprite, written = spriteRel, append(written, spriteRel)
		}
	}
	if meta.Poster == "" && fileExists(filepath.Join(outDir, posterRel)) {
		meta.Poster = posterRel
	}
	if meta.Sprite == "" && fileExists(filepath.Join(outDir, spriteRel)) {
		meta.Sprite = spriteRel
	}
	return meta, written, nil
}

// cardOf returns the card volume subfolder a clip came from, or "" for a
// mission whose files sit flat at the top level.
func cardOf(rel string) string {
	if i := strings.Index(rel, string(os.PathSeparator)); i > 0 {
		return rel[:i]
	}
	return ""
}

// writeProxyManifests writes proxies.json and merges the newly written
// renditions into proxies.b3. The manifest is byte-identical in format to
// checksums.b3 — sorted "hash  relative_path" — so -verify and -check read it
// without changes.
func writeProxyManifests(outDir string, m proxyManifest, written []string) error {
	sort.Slice(m.Clips, func(i, j int) bool { return m.Clips[i].Rel < m.Clips[j].Rel })
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		return err
	}
	if err := os.MkdirAll(outDir, 0777); err != nil {
		return err
	}
	if err := os.WriteFile(filepath.Join(outDir, proxyMetaName), append(data, '\n'), 0644); err != nil {
		return err
	}

	// proxies.json is hashed too, so the manifest covers the whole tree.
	written = append(written, proxyMetaName)
	manifestPath := filepath.Join(outDir, proxyManifestName)
	existing := readChecksumFile(manifestPath)
	for _, rel := range written {
		h, err := hashFile(filepath.Join(outDir, rel), nil)
		if err != nil {
			return err
		}
		existing[rel] = h
	}
	delete(existing, proxyManifestName) // a manifest cannot describe itself

	lines := make([]string, 0, len(existing))
	for rel, h := range existing {
		lines = append(lines, fmt.Sprintf("%s  %s", h, rel))
	}
	sort.Strings(lines)
	return os.WriteFile(manifestPath, []byte(strings.Join(lines, "\n")+"\n"), 0644)
}

// ── the command ─────────────────────────────────────────────────────────────

// proxyDest resolves where the proxy tree lands: the named drive, or the first
// mounted hot drive. Proxies are derived, so they belong on fast working
// storage, never on the archive.
func proxyDest(cfg Config, to string) (DriveConfig, error) {
	if to != "" {
		for _, d := range cfg.Drives {
			if normaliseVol(d.name()) != normaliseVol(to) {
				continue
			}
			if !dirExists(d.basePath()) {
				return DriveConfig{}, fmt.Errorf("drive %s is not mounted", d.name())
			}
			return d, nil
		}
		return DriveConfig{}, fmt.Errorf("no configured drive named %q", to)
	}
	for _, d := range cfg.Drives {
		if d.Role == "hot" && dirExists(d.basePath()) {
			return d, nil
		}
	}
	return DriveConfig{}, fmt.Errorf("no hot drive mounted")
}

func runProxy(cfg Config, missions []int, year int, all bool, tiers proxyTiers, to string, skipConf bool) bool {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		exit(1, "ffmpeg not found — install it to generate proxies")
	}
	yearStr := strconv.Itoa(year)

	dst, err := proxyDest(cfg, to)
	if err != nil {
		exit(1, "err resolving proxy destination: %v", err)
	}
	yearRoot := filepath.Join(proxyRoot(dst.basePath()), yearStr)

	if all {
		missions = missionsInYear(cfg, yearStr)
		if len(missions) == 0 {
			fmt.Println(dim("no missions found"))
			return true
		}
	}

	// Resolve everything before generating anything, so a bad mission number
	// surfaces up front rather than mid-batch.
	var plans []missionPlan
	failed := 0
	for _, num := range missions {
		src, err := resolveProxySource(cfg, yearStr, num)
		if err != nil {
			fmt.Printf("%s mission %03d: %v\n", red("ERROR"), num, err)
			failed++
			continue
		}
		plans = append(plans, planMission(src, filepath.Join(yearRoot, src.slug), tiers))
	}
	if len(plans) == 0 {
		exit(1, "nothing to proxy")
	}

	var tierNames []string
	if tiers.edit {
		tierNames = append(tierNames, "edit")
	}
	if tiers.browse {
		tierNames = append(tierNames, "browse")
	}

	var totalTodo int
	var totalSize, totalClips int64
	var regenerating bool
	for _, p := range plans {
		totalTodo += p.todo
		totalSize += p.todoSize
		totalClips += int64(len(p.jobs))
		if len(p.existing) > 0 && p.todo > 0 {
			regenerating = true
		}
	}

	fmt.Printf("\n  %s  %s  %s\n", dim("tier   "), bold(strings.Join(tierNames, " + ")), dim("(proxies are derived — never archived)"))
	fmt.Printf("  %s  %s  %s\n\n", dim("to     "), bold(dst.name()), dim(yearRoot))
	for _, p := range plans {
		status := dim("up to date")
		if p.todo > 0 {
			status = fmt.Sprintf("%s of %d clip(s)  %s", bold(strconv.Itoa(p.todo)), len(p.jobs), dim(fmtSize(uint64(p.todoSize))))
		}
		fmt.Printf("  %s  %s  %s\n", bold(p.src.slug), dim("from "+p.src.vol), status)
	}
	if totalTodo == 0 {
		fmt.Printf("\n  %s\n", dim("nothing to generate"))
		return failed == 0
	}
	fmt.Printf("\n  %s %d clip(s) across %d mission(s), %s to read\n\n",
		dim("total"), totalTodo, len(plans), fmtSize(uint64(totalSize)))
	if regenerating {
		fmt.Printf("  %s\n\n", yellow("⚠  an existing proxy tree will be overwritten where the source has changed"))
	}
	if !skipConf && !confirm() {
		exit(0, "aborted")
	}

	lutDir := filepath.Join(proxyRoot(dst.basePath()), lutDirName)
	if ok := generatePlans(plans, tiers, lutDir); !ok {
		failed++
	}
	return failed == 0
}

// generatePlans runs the encode for a set of planned missions and writes their
// manifests. Concurrency is bounded per source drive, so a batch spanning the
// archive HDD and a hot SSD does not thrash the platter.
func generatePlans(plans []missionPlan, tiers proxyTiers, lutDir string) bool {
	type work struct {
		plan *missionPlan
		job  *clipJob
	}
	byVol := make(map[string][]work)
	volBase := make(map[string]string)
	var volOrder []string
	var totalBytes int64
	for i := range plans {
		p := &plans[i]
		for j := range p.jobs {
			if !p.jobs[j].work() {
				continue
			}
			vol := p.src.vol
			if _, seen := volBase[vol]; !seen {
				volBase[vol] = p.src.base
				volOrder = append(volOrder, vol)
			}
			byVol[vol] = append(byVol[vol], work{p, &p.jobs[j]})
			totalBytes += p.jobs[j].size
		}
	}

	var done atomic.Int64
	var failures atomic.Int64
	total := 0
	for _, ws := range byVol {
		total += len(ws)
	}

	fmt.Println()
	for _, vol := range volOrder {
		info := probeDrive(volBase[vol])
		fmt.Printf("  %s %s: %s %s\n", dim("from"), bold(vol), info,
			dim(fmt.Sprintf("· %d encoder(s)", proxyWorkers(info))))
	}
	fmt.Printf("\n%s\n\n", dim("encoding..."))

	// An interrupt kills the ffmpeg children along with us, so let the workers
	// see it, clean up their temporaries and stop. What already finished is
	// still written to the manifest — a re-run then skips it.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)
	defer signal.Stop(sigCh)
	go func() {
		<-sigCh
		cancel()
	}()

	pr := mpb.NewWithContext(ctx, mpb.WithWidth(64))
	bar := addBarDynamic(pr, "proxy", totalBytes, func() string {
		return fmt.Sprintf("%d/%d", done.Load(), total)
	})

	metas := make(map[*missionPlan][]clipMeta)
	writes := make(map[*missionPlan][]string)
	var mu sync.Mutex

	var pools []*pool
	var submitters []func()
	for _, vol := range volOrder {
		ws := byVol[vol]
		wp := newPool(proxyWorkers(probeDrive(volBase[vol])))
		pools = append(pools, wp)
		submitters = append(submitters, func() {
			for _, w := range ws {
				w := w
				wp.run(func() {
					if ctx.Err() != nil {
						return
					}
					var last float64
					meta, written, err := generateClip(w.job, w.plan.outDir, lutDir, tiers, func(frac float64) {
						if d := frac - last; d > 0 {
							bar.incr(int(d * float64(w.job.size)))
							last = frac
						}
					})
					if d := 1 - last; d > 0 {
						bar.incr(int(d * float64(w.job.size)))
					}
					done.Add(1)
					if err != nil {
						if ctx.Err() != nil {
							return // interrupted, not a real failure
						}
						fmt.Printf("\n%s %s: %v\n", red("ERROR"), w.job.rel, err)
						failures.Add(1)
					}
					mu.Lock()
					metas[w.plan] = append(metas[w.plan], meta)
					writes[w.plan] = append(writes[w.plan], written...)
					mu.Unlock()
				})
			}
		})
	}
	submitAll(submitters)
	for _, wp := range pools {
		wp.wait()
	}
	bar.finish()
	pr.Wait()

	ok := failures.Load() == 0
	for i := range plans {
		p := &plans[i]
		generated := make(map[string]bool)
		for _, m := range metas[p] {
			generated[m.Rel] = true
		}
		clips := append([]clipMeta(nil), metas[p]...)
		// Clips that needed no work keep the entry they already had, so a
		// partial run never drops what a previous one recorded.
		for _, j := range p.jobs {
			if !generated[j.rel] && j.cached {
				clips = append(clips, j.meta)
			}
		}
		if len(clips) == 0 {
			continue
		}
		m := proxyManifest{
			Version: proxyManifestVersion,
			Year:    yearOfPath(p.outDir),
			Mission: p.src.slug,
			Clips:   clips,
		}
		if err := writeProxyManifests(p.outDir, m, writes[p]); err != nil {
			fmt.Printf("%s writing %s: %v\n", red("ERROR"), proxyManifestName, err)
			ok = false
		}
	}

	if ctx.Err() != nil {
		fmt.Printf("\n  %s  interrupted — %d clip(s) finished and recorded\n", yellow("⚠"), done.Load())
		os.Exit(130)
	}
	if ok {
		fmt.Printf("\n  %s  %s  %s\n", green("✓"), bold("Done"),
			dim(fmt.Sprintf("%d clip(s) proxied", done.Load())))
	} else {
		fmt.Printf("\n  %s  %d clip(s) failed\n", red("✗"), failures.Load())
	}
	return ok
}

// yearOfPath reads the year back out of a proxy mission directory
// (.../proxies/2026/042_Foo).
func yearOfPath(dir string) int {
	y, _ := strconv.Atoi(filepath.Base(filepath.Dir(dir)))
	return y
}

// runIngestProxies generates the browse tier for a mission that has just been
// ingested and verified, while the cards are still mounted. Doing it here makes
// backfilling the existing archive a one-time event rather than a permanent
// chore.
//
// The edit tier is never generated at ingest: the concurrent decode
// measurements say the camera originals already play fine off a hot SSD, so
// ProRes proxies are an on-demand tool, not a standing ~1TB commitment.
func runIngestProxies(cfg Config, year int, slug string) {
	if _, err := exec.LookPath("ffmpeg"); err != nil {
		fmt.Printf("\n  %s  %s\n", yellow("⚠"), dim("ffmpeg not found — skipping proxies"))
		return
	}
	dst, err := proxyDest(cfg, "")
	if err != nil {
		fmt.Printf("\n  %s  %s\n", yellow("⚠"), dim("no proxy destination: "+err.Error()))
		return
	}
	yearStr := strconv.Itoa(year)
	src, err := proxySourceForSlug(cfg, yearStr, slug)
	if err != nil {
		fmt.Printf("\n  %s  %s\n", yellow("⚠"), dim("no clips to proxy: "+err.Error()))
		return
	}
	tiers := proxyTiers{browse: true}
	outDir := filepath.Join(proxyRoot(dst.basePath()), yearStr, slug)
	plan := planMission(src, outDir, tiers)
	if plan.todo == 0 {
		return
	}
	fmt.Printf("\n  %s  %s  %s\n", cyan("◈"), bold("Proxies"),
		dim(fmt.Sprintf("browse tier · %d clip(s) · → %s", plan.todo, dst.name())))
	generatePlans([]missionPlan{plan}, tiers, filepath.Join(proxyRoot(dst.basePath()), lutDirName))
}
