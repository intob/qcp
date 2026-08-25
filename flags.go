package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"time"
)

// Flags are the one piece of state in qcp a person creates rather than derives:
// a clip marked in the browser index as worth coming back to. They live beside
// the footage they describe, in a dotfile, because findFiles skips any path
// component starting with "." — so checksums.b3, -verify, -check, -sync and
// -replicate never see them. That invisibility is the point: a flag must not be
// able to make a mission look corrupt or make an archive drive look out of
// date. The cost is that flags are not carried to cold storage; they are cheap
// to recreate and the footage is what the archive is for.
const flagsFileName = ".qcp-flags.json"

// flagColour is the one colour written to Resolve. It has to be a name that is
// valid both as a flag and as a clip colour, which not every Resolve colour is.
const flagColour = "Blue"

type clipFlag struct {
	Colour string `json:"colour"`
	At     string `json:"at"` // RFC3339, so the newest wins when drives disagree
}

type missionFlags struct {
	Version int                 `json:"version"`
	Flags   map[string]clipFlag `json:"flags"` // clip path relative to the mission dir
}

// missionDir is where a mission sits on one drive.
func missionDir(d DriveConfig, year int, slug string) string {
	return filepath.Join(d.basePath(), d.Root, strconv.Itoa(year), slug)
}

func readMissionFlags(dir string) (missionFlags, error) {
	f := missionFlags{Version: 1, Flags: map[string]clipFlag{}}
	raw, err := os.ReadFile(filepath.Join(dir, flagsFileName))
	if err != nil {
		if os.IsNotExist(err) {
			return f, nil
		}
		return f, err
	}
	if err := json.Unmarshal(raw, &f); err != nil {
		return f, fmt.Errorf("%s: %w", flagsFileName, err)
	}
	if f.Flags == nil {
		f.Flags = map[string]clipFlag{}
	}
	return f, nil
}

// writeMissionFlags replaces the file, or removes it once nothing is flagged so
// unflagging everything leaves no trace behind.
func writeMissionFlags(dir string, f missionFlags) error {
	path := filepath.Join(dir, flagsFileName)
	if len(f.Flags) == 0 {
		err := os.Remove(path)
		if err != nil && !os.IsNotExist(err) {
			return err
		}
		return nil
	}
	f.Version = 1
	raw, err := json.MarshalIndent(f, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, append(raw, '\n'), 0644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// mergeMissionFlags unions what several drives hold, newest timestamp winning a
// disagreement. Drives go out of sync whenever one was unmounted during an edit.
func mergeMissionFlags(all []missionFlags) missionFlags {
	out := missionFlags{Version: 1, Flags: map[string]clipFlag{}}
	for _, f := range all {
		for rel, c := range f.Flags {
			if prev, ok := out.Flags[rel]; !ok || c.At > prev.At {
				out.Flags[rel] = c
			}
		}
	}
	return out
}

// flagStore reads and writes flags across every mounted drive holding a
// mission. Reads merge all of them; writes go to hot drives only, so toggling a
// flag never spins up an archive HDD.
type flagStore struct {
	drives []DriveConfig
}

func newFlagStore(cfg Config) *flagStore {
	var mounted []DriveConfig
	for _, d := range cfg.Drives {
		if dirExists(d.basePath()) {
			mounted = append(mounted, d)
		}
	}
	return &flagStore{drives: mounted}
}

// read returns the merged flags and refuses to guess. A drive that holds the
// mission but whose flags file will not parse is an error, not an empty set:
// every write starts from what read returns, so treating an unreadable file as
// "nothing flagged" would let the next toggle overwrite it with a single entry
// and silently destroy the rest. Flags are the one thing here a person typed.
func (s *flagStore) read(year int, slug string) (missionFlags, error) {
	var all []missionFlags
	for _, d := range s.drives {
		dir := missionDir(d, year, slug)
		if !dirExists(dir) {
			continue
		}
		f, err := readMissionFlags(dir)
		if err != nil {
			return missionFlags{}, fmt.Errorf("%s: %w", d.name(), err)
		}
		all = append(all, f)
	}
	return mergeMissionFlags(all), nil
}

// get is the lenient read, for display only. Nothing writes back from it.
func (s *flagStore) get(year int, slug string) missionFlags {
	f, err := s.read(year, slug)
	if err != nil {
		fmt.Printf("%s %s\n", yellow("warning:"), dim(err.Error()))
		return missionFlags{Version: 1, Flags: map[string]clipFlag{}}
	}
	return f
}

// set toggles one clip and persists the result. It returns the merged state so
// a caller can report what is now true rather than what it asked for.
func (s *flagStore) set(year int, slug, rel string, on bool) (missionFlags, error) {
	cur, err := s.read(year, slug)
	if err != nil {
		return missionFlags{}, fmt.Errorf("refusing to write over flags that could not be read: %w", err)
	}
	if on {
		cur.Flags[rel] = clipFlag{Colour: flagColour, At: time.Now().UTC().Format(time.RFC3339)}
	} else {
		delete(cur.Flags, rel)
	}
	var wrote int
	var firstErr error
	for _, d := range s.drives {
		if d.Role != "hot" {
			continue
		}
		dir := missionDir(d, year, slug)
		if !dirExists(dir) {
			continue
		}
		if err := writeMissionFlags(dir, cur); err != nil && firstErr == nil {
			firstErr = err
		} else if err == nil {
			wrote++
		}
	}
	if wrote == 0 && firstErr == nil {
		return cur, fmt.Errorf("mission %d/%s is not on a mounted hot drive", year, slug)
	}
	return cur, firstErr
}

// flaggedClip is one flag resolved to the absolute path Resolve will report for
// the clip, which is the only thing the two sides need to agree on.
type flaggedClip struct {
	Year   int
	Slug   string
	Rel    string
	Path   string
	Colour string
}

// all walks every mounted drive and returns one entry per flagged clip, keyed
// by the absolute source path. A mission on two drives yields the path on
// whichever drive is listed first, matching how Resolve would have imported it.
func (s *flagStore) all() []flaggedClip {
	seen := map[string]bool{}
	var out []flaggedClip
	for _, d := range s.drives {
		root := filepath.Join(d.basePath(), d.Root)
		years, err := os.ReadDir(root)
		if err != nil {
			continue
		}
		for _, y := range years {
			year, err := strconv.Atoi(y.Name())
			if !y.IsDir() || err != nil {
				continue
			}
			for _, slug := range missionDirs(filepath.Join(root, y.Name())) {
				dir := filepath.Join(root, y.Name(), slug)
				f, err := readMissionFlags(dir)
				if err != nil || len(f.Flags) == 0 {
					continue
				}
				for rel, c := range f.Flags {
					key := strconv.Itoa(year) + "/" + slug + "/" + rel
					if seen[key] {
						continue
					}
					seen[key] = true
					colour := c.Colour
					if colour == "" {
						colour = flagColour
					}
					out = append(out, flaggedClip{
						Year: year, Slug: slug, Rel: rel,
						Path: filepath.Join(dir, rel), Colour: colour,
					})
				}
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Year != out[j].Year {
			return out[i].Year < out[j].Year
		}
		if out[i].Slug != out[j].Slug {
			return out[i].Slug < out[j].Slug
		}
		return out[i].Rel < out[j].Rel
	})
	return out
}
