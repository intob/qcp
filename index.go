package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vbauerster/mpb/v8"
)

// -index emits a self-contained static site built from the proxy manifests and
// the checksums.b3 files on whatever drives are mounted.
//
// The point is that it works with nothing plugged in: plain HTML, CSS and JS,
// no build step, no CDN, no fetch. The stills tier is ~750MB for the whole
// 3,781-clip library, small enough to live on the laptop permanently. The 720p
// browse proxies play when a drive happens to be mounted, and the index falls
// back to showing the path when one is not.

const indexDirName = "qcp-index"

type indexDrive struct {
	Name string `json:"name"`
	Role string `json:"role"`
	Base string `json:"base"` // mount path, for composing absolute clip paths
	Root string `json:"root"`
}

type indexClip struct {
	Rel       string  `json:"rel"`
	Card      string  `json:"card,omitempty"`
	Size      int64   `json:"size"`
	Duration  float64 `json:"dur,omitempty"`
	Width     int     `json:"w,omitempty"`
	Height    int     `json:"h,omitempty"`
	FPS       float64 `json:"fps,omitempty"`
	Codec     string  `json:"codec,omitempty"`
	Gamma     string  `json:"gamma,omitempty"`
	Primaries string  `json:"prim,omitempty"`
	Transform string  `json:"xf,omitempty"`
	Browse    string  `json:"browse,omitempty"` // relative to the mission proxy dir
	Poster    bool    `json:"poster,omitempty"` // present in this index's stills tree
	Sprite    bool    `json:"sprite,omitempty"`
}

type indexMission struct {
	Num      int         `json:"num"`
	Slug     string      `json:"slug"`
	Name     string      `json:"name"`
	Drives   []string    `json:"drives"` // drive names holding a copy
	Verified []string    `json:"verified,omitempty"`
	Files    int         `json:"files"`
	Size     int64       `json:"size"`
	ProxyDir string      `json:"proxyDir,omitempty"` // absolute, for playback
	Clips    []indexClip `json:"clips"`
}

type indexYear struct {
	Year     int            `json:"year"`
	Missions []indexMission `json:"missions"`
}

type indexData struct {
	Generated string       `json:"generated"`
	Version   string       `json:"version"`
	Drives    []indexDrive `json:"drives"`
	Years     []indexYear  `json:"years"`
}

func (d indexData) counts() (missions, clips int, size int64) {
	for _, y := range d.Years {
		missions += len(y.Missions)
		for _, m := range y.Missions {
			clips += len(m.Clips)
			size += m.Size
		}
	}
	return
}

// missionName turns "042_Altissimo_with_Anton" into "Altissimo with Anton".
func missionName(slug string) string {
	_, rest, ok := strings.Cut(slug, "_")
	if !ok {
		return slug
	}
	return strings.ReplaceAll(rest, "_", " ")
}

func missionNum(slug string) int {
	n, _ := strconv.Atoi(strings.SplitN(slug, "_", 2)[0])
	return n
}

// stillRel is where a clip's still lands inside the index tree. It mirrors the
// proxy layout so a mission's stills can be copied in with a plain walk.
func stillRel(year int, slug, clipRel, suffix string) string {
	return filepath.Join("stills", strconv.Itoa(year), slug,
		strings.TrimSuffix(clipRel, filepath.Ext(clipRel))+suffix)
}

// indexOutDir resolves the -to flag to the directory the index lives in,
// defaulting to ~/qcp-index. Shared by -index and -serve so both agree.
func indexOutDir(out string) string {
	if out == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			exit(1, "err resolving home directory: %v", err)
		}
		return filepath.Join(home, indexDirName)
	}
	p, err := expandPath(out)
	if err != nil {
		exit(1, "err resolving %s: %v", out, err)
	}
	return p
}

func runIndex(cfg Config, out string) bool {
	out = indexOutDir(out)

	var mounted []DriveConfig
	for _, d := range cfg.Drives {
		if dirExists(d.basePath()) {
			mounted = append(mounted, d)
			continue
		}
		fmt.Printf("%s %s %s\n", yellow("warning:"), bold(d.name()), dim("not mounted — its missions will be missing from the index"))
	}
	if len(mounted) == 0 {
		exit(1, "no drives mounted")
	}

	data := indexData{
		Generated: time.Now().Format(time.RFC3339),
		Version:   version,
	}
	for _, d := range mounted {
		data.Drives = append(data.Drives, indexDrive{
			Name: d.name(), Role: d.Role, Base: d.basePath(), Root: d.Root,
		})
	}

	years := allYears(cfg)
	if len(years) == 0 {
		exit(1, "no missions found on the mounted drives")
	}
	sort.Sort(sort.Reverse(sort.IntSlice(years)))

	fmt.Printf("\n  %s  %s\n  %s  %s\n\n", dim("out    "), bold(out),
		dim("drives "), dim(strings.Join(driveNames(mounted), ", ")))
	fmt.Printf("%s\n", dim("scanning..."))

	// stills to copy, gathered while scanning so the copy is one pass.
	type stillCopy struct {
		src, dst string
		size     int64
	}
	var stills []stillCopy
	var stillBytes int64

	for _, year := range years {
		yearStr := strconv.Itoa(year)

		// Which drives hold each mission, and how complete each copy is.
		slugSet := make(map[string]bool)
		for _, d := range mounted {
			entries, err := os.ReadDir(filepath.Join(d.basePath(), d.Root, yearStr))
			if err != nil {
				continue
			}
			for _, e := range entries {
				if e.IsDir() && isNumberedMission(e.Name()) {
					slugSet[e.Name()] = true
				}
			}
		}
		if len(slugSet) == 0 {
			continue
		}
		slugs := make([]string, 0, len(slugSet))
		for s := range slugSet {
			slugs = append(slugs, s)
		}
		sort.Strings(slugs)
		scans := scanMissions(mounted, yearStr, slugs)

		iy := indexYear{Year: year}
		for _, slug := range slugs {
			m := indexMission{
				Num:  missionNum(slug),
				Slug: slug,
				Name: missionName(slug),
			}
			for _, d := range mounted {
				sc, ok := scans[slug][d.name()]
				if !ok {
					continue
				}
				m.Drives = append(m.Drives, d.name())
				if sc.checksummed {
					m.Verified = append(m.Verified, d.name())
				}
				if sc.files > m.Files {
					m.Files, m.Size = sc.files, sc.size
				}
			}
			if len(m.Drives) == 0 {
				continue
			}

			// Proxy metadata: first mounted drive that has a manifest for it.
			var pm proxyManifest
			for _, d := range mounted {
				dir := proxyMissionDir(d.basePath(), year, slug)
				if got := readProxyManifest(dir); len(got.Clips) > 0 {
					pm, m.ProxyDir = got, dir
					break
				}
			}
			for _, c := range pm.Clips {
				ic := indexClip{
					Rel: c.Rel, Card: c.Card, Size: c.Size, Duration: c.Duration,
					Width: c.Width, Height: c.Height, FPS: c.FPS, Codec: c.Codec,
					Gamma: c.Gamma, Primaries: c.Primaries, Transform: c.Transform,
					Browse: c.Browse,
				}
				for _, s := range []struct {
					rel    string
					suffix string
					flag   *bool
				}{
					{c.Poster, ".poster.jpg", &ic.Poster},
					{c.Sprite, ".sprite.jpg", &ic.Sprite},
				} {
					if s.rel == "" {
						continue
					}
					src := filepath.Join(m.ProxyDir, s.rel)
					fi, err := os.Stat(src)
					if err != nil {
						continue
					}
					*s.flag = true
					dst := filepath.Join(out, stillRel(year, slug, c.Rel, s.suffix))
					stills = append(stills, stillCopy{src, dst, fi.Size()})
					stillBytes += fi.Size()
				}
				m.Clips = append(m.Clips, ic)
			}
			if m.Clips == nil {
				m.Clips = []indexClip{} // an empty list, never JSON null
			}
			iy.Missions = append(iy.Missions, m)
		}
		sort.Slice(iy.Missions, func(i, j int) bool { return iy.Missions[i].Num < iy.Missions[j].Num })
		if len(iy.Missions) > 0 {
			data.Years = append(data.Years, iy)
		}
	}

	missions, clips, size := data.counts()
	withProxies := 0
	for _, y := range data.Years {
		for _, m := range y.Missions {
			if len(m.Clips) > 0 {
				withProxies++
			}
		}
	}
	fmt.Printf("\n  %s  %d mission(s), %d proxied, %d clip(s), %s of footage\n",
		dim("found  "), missions, withProxies, clips, fmtSize(uint64(size)))
	fmt.Printf("  %s  %d file(s), %s\n\n", dim("stills "), len(stills), fmtSize(uint64(stillBytes)))

	if err := os.MkdirAll(out, 0777); err != nil {
		exit(1, "err creating %s: %v", out, err)
	}

	if len(stills) > 0 {
		p := mpb.New(mpb.WithWidth(64))
		bar := addBar(p, "stills", stillBytes)
		var failed int
		var mu sync.Mutex
		wp := newPool(4)
		for _, s := range stills {
			s := s
			wp.run(func() {
				if err := copyStill(s.src, s.dst); err != nil {
					mu.Lock()
					failed++
					mu.Unlock()
					fmt.Printf("\n%s %s: %v\n", red("ERROR"), s.src, err)
				}
				bar.incr(int(s.size))
			})
		}
		wp.wait()
		bar.finish()
		p.Wait()
		if failed > 0 {
			fmt.Printf("%s %d still(s) failed to copy\n", red("ERROR"), failed)
		}
	}

	raw, err := json.Marshal(data)
	if err != nil {
		exit(1, "err encoding index: %v", err)
	}
	if err := os.WriteFile(filepath.Join(out, "index.json"), raw, 0644); err != nil {
		exit(1, "err writing index.json: %v", err)
	}
	// The same data is inlined into the page. A file:// page cannot fetch a
	// sibling JSON file — every browser blocks it as a cross-origin request —
	// and the whole point of this index is that it opens with nothing mounted.
	// index.json is kept alongside as the machine-readable copy.
	page := strings.Replace(indexHTML, "/*INDEX_DATA*/null", string(raw), 1)
	if err := os.WriteFile(filepath.Join(out, "index.html"), []byte(page), 0644); err != nil {
		exit(1, "err writing index.html: %v", err)
	}

	fmt.Printf("\n  %s  %s  %s\n", green("✓"), bold("Done"), dim(filepath.Join(out, "index.html")))
	return true
}

func driveNames(drives []DriveConfig) []string {
	out := make([]string, len(drives))
	for i, d := range drives {
		out[i] = d.name()
	}
	return out
}

// copyStill copies one still into the index tree, skipping files already there
// at the same size so a re-run is cheap.
func copyStill(src, dst string) error {
	si, err := os.Stat(src)
	if err != nil {
		return err
	}
	if di, err := os.Stat(dst); err == nil && di.Size() == si.Size() {
		return nil
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0777); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	tmp := dst + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	if _, err := io.Copy(f, in); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, dst)
}
