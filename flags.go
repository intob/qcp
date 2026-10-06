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
	At     string `json:"at"`            // RFC3339, so the newest wins when drives disagree
	Off    bool   `json:"off,omitempty"` // an unflag, kept so it outranks an older flag
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

// writeMissionFlags replaces the file, or removes it once it records nothing.
// An unflagged clip is still a record — see set — so the file only goes away
// when it is written empty.
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
// Timestamps are to the second, so an unflag wins a tie: flagging and then
// unflagging within one second must not come out flagged.
func mergeMissionFlags(all []missionFlags) missionFlags {
	out := missionFlags{Version: 1, Flags: map[string]clipFlag{}}
	for _, f := range all {
		for rel, c := range f.Flags {
			prev, ok := out.Flags[rel]
			if !ok || c.At > prev.At || (c.At == prev.At && c.Off && !prev.Off) {
				out.Flags[rel] = c
			}
		}
	}
	return out
}

// flagged is the clips that are flagged now, leaving out the unflags.
func (f missionFlags) flagged() map[string]clipFlag {
	out := make(map[string]clipFlag, len(f.Flags))
	for rel, c := range f.Flags {
		if !c.Off {
			out[rel] = c
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

// get is the lenient read, for display only, and holds only the clips that are
// flagged now. Nothing writes back from it.
func (s *flagStore) get(year int, slug string) missionFlags {
	f, err := s.read(year, slug)
	if err != nil {
		fmt.Printf("%s %s\n", yellow("warning:"), dim(err.Error()))
		return missionFlags{Version: 1, Flags: map[string]clipFlag{}}
	}
	return missionFlags{Version: f.Version, Flags: f.flagged()}
}

// set toggles one clip and persists the result. It returns the merged state so
// a caller can report what is now true rather than what it asked for.
//
// An unflag is recorded rather than deleted. Writes reach only the hot drives
// that are mounted, and reads merge every drive with the newest entry winning,
// so a deleted entry left nothing to outrank the flag still held by a drive
// that was away, or by the cold copy -evict carried it to: the clip came back
// flagged as soon as that drive was mounted again.
func (s *flagStore) set(year int, slug, rel string, on bool) (missionFlags, error) {
	cur, err := s.read(year, slug)
	if err != nil {
		return missionFlags{}, fmt.Errorf("refusing to write over flags that could not be read: %w", err)
	}
	now := time.Now().UTC().Format(time.RFC3339)
	if on {
		cur.Flags[rel] = clipFlag{Colour: flagColour, At: now}
	} else {
		cur.Flags[rel] = clipFlag{At: now, Off: true}
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
// by the absolute source path. Each mission is merged across the drives that
// hold it, as read does, so an unflag on one drive outranks an older flag on
// another. A clip's path is on the first listed drive whose own file flags it,
// matching how Resolve would have imported it.
func (s *flagStore) all() []flaggedClip {
	type copyFlags struct {
		dir   string
		flags missionFlags
	}
	type mission struct {
		year   int
		slug   string
		copies []copyFlags
	}
	missions := map[string]*mission{}
	var order []string
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
				key := strconv.Itoa(year) + "/" + slug
				m := missions[key]
				if m == nil {
					m = &mission{year: year, slug: slug}
					missions[key] = m
					order = append(order, key)
				}
				m.copies = append(m.copies, copyFlags{dir, f})
			}
		}
	}

	var out []flaggedClip
	for _, key := range order {
		m := missions[key]
		all := make([]missionFlags, len(m.copies))
		for i, c := range m.copies {
			all[i] = c.flags
		}
		for rel, c := range mergeMissionFlags(all).flagged() {
			dir := m.copies[0].dir
			for _, cp := range m.copies {
				if own, ok := cp.flags.Flags[rel]; ok && !own.Off {
					dir = cp.dir
					break
				}
			}
			colour := c.Colour
			if colour == "" {
				colour = flagColour
			}
			out = append(out, flaggedClip{
				Year: m.year, Slug: m.slug, Rel: rel,
				Path: filepath.Join(dir, rel), Colour: colour,
			})
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
