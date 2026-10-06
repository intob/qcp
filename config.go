package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	Cards  []CardConfig  `json:"cards"`
	Drives []DriveConfig `json:"drives"`

	// Look is an optional creative .cube baked into the browse tier in place of
	// the generated technical conversion. It must take S-Log3 in and deliver
	// finished Rec.709 out, because nothing is applied after it. Only log clips
	// get it — Rec.709 sources have no log to give it and are left alone.
	Look string `json:"look"`
}

type CardConfig struct {
	Volume string `json:"volume"`
	Sub    string `json:"sub"`
}

type DriveConfig struct {
	Volume   string `json:"volume"` // display name and /Volumes/<volume> path (if Path not set)
	Path     string `json:"path"`   // explicit path, e.g. "~/Footage" (overrides Volume for path)
	Root     string `json:"root"`
	Role     string `json:"role"`      // "hot" or "cold"
	Pull     *bool  `json:"pull"`      // nil/true = pull allowed (default), false = excluded from pull
	YearFrom int    `json:"year_from"` // first year this drive is responsible for (0 = no lower bound)
	YearTo   int    `json:"year_to"`   // last year this drive is responsible for (0 = no upper bound)
}

// coversYear reports whether this drive is responsible for the given year.
func (d DriveConfig) coversYear(year int) bool {
	if d.YearFrom > 0 && year < d.YearFrom {
		return false
	}
	if d.YearTo > 0 && year > d.YearTo {
		return false
	}
	return true
}

func (d DriveConfig) pullAllowed() bool {
	return d.Pull == nil || *d.Pull
}

func (d DriveConfig) basePath() string {
	if d.Path != "" {
		if strings.HasPrefix(d.Path, "~/") {
			home, _ := os.UserHomeDir()
			return filepath.Join(home, d.Path[2:])
		}
		return d.Path
	}
	return filepath.Join("/Volumes", d.Volume)
}

func (d DriveConfig) name() string {
	if d.Volume != "" {
		return d.Volume
	}
	return filepath.Base(d.basePath())
}

func loadConfig() Config {
	p, err := expandPath("~/.qcp")
	if err != nil {
		exit(1, "err resolving config path: %v", err)
	}
	data, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		exit(1, "config not found — create %s with your drive settings", p)
	}
	if err != nil {
		exit(1, "err reading config: %v", err)
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		exit(1, "err parsing config: %v", err)
	}
	if problems := validateConfig(cfg); len(problems) > 0 {
		fmt.Printf("%s %s has %d problem(s):\n", red("ERROR"), p, len(problems))
		for _, pr := range problems {
			fmt.Printf("  %s\n", pr)
		}
		quit(1)
	}
	return cfg
}

// validateConfig lists everything wrong with a config, so it can all be fixed
// in one go.
//
// Nothing was checked before. A role typo — "Cold" — left a drive out of
// every hot and cold path, so it was never synced to or checked, yet it still
// took a copy of every ingest. Two drives with the same name collided in every
// map keyed by name and in the catalog; two resolving to one folder made one
// copy look like two. A card with no sub matched any external volume, so a
// stray USB stick would have been ingested whole.
func validateConfig(cfg Config) []string {
	var out []string
	if len(cfg.Drives) == 0 {
		out = append(out, "no drives configured")
	}
	names := map[string]int{}
	paths := map[string]int{}
	for i, d := range cfg.Drives {
		label := fmt.Sprintf("drive %d", i+1)
		if d.Volume == "" && d.Path == "" {
			out = append(out, label+": needs a volume or a path")
			continue
		}
		label = fmt.Sprintf("drive %q", d.name())
		if d.Role != "hot" && d.Role != "cold" {
			out = append(out, fmt.Sprintf(`%s: role is %q — must be "hot" or "cold"`, label, d.Role))
		}
		if d.YearFrom != 0 && (d.YearFrom < 2000 || d.YearFrom > 2099) {
			out = append(out, fmt.Sprintf("%s: year_from %d is not a year from 2000 to 2099", label, d.YearFrom))
		}
		if d.YearTo != 0 && (d.YearTo < 2000 || d.YearTo > 2099) {
			out = append(out, fmt.Sprintf("%s: year_to %d is not a year from 2000 to 2099", label, d.YearTo))
		}
		if d.YearFrom != 0 && d.YearTo != 0 && d.YearFrom > d.YearTo {
			out = append(out, fmt.Sprintf("%s: year_from %d is after year_to %d", label, d.YearFrom, d.YearTo))
		}
		if strings.Contains(d.Root, "..") || filepath.IsAbs(d.Root) {
			out = append(out, fmt.Sprintf("%s: root %q must be a folder inside the drive", label, d.Root))
		}
		if j, dup := names[normaliseVol(d.name())]; dup {
			out = append(out, fmt.Sprintf("%s: same name as drive %d — every drive needs its own", label, j+1))
		} else {
			names[normaliseVol(d.name())] = i
		}
		footage := filepath.Clean(filepath.Join(d.basePath(), d.Root))
		if j, dup := paths[footage]; dup {
			out = append(out, fmt.Sprintf("%s: same footage folder as drive %d (%s)", label, j+1, footage))
		} else {
			paths[footage] = i
		}
	}
	for i, c := range cfg.Cards {
		switch {
		case strings.TrimSpace(c.Sub) == "":
			out = append(out, fmt.Sprintf("card %d: needs a sub folder, or any external volume would match it", i+1))
		case filepath.IsAbs(c.Sub) || strings.Contains(c.Sub, ".."):
			out = append(out, fmt.Sprintf("card %d: sub %q must be a folder inside the card", i+1, c.Sub))
		}
	}
	return out
}
