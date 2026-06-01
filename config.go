package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
)

type Config struct {
	Cards  []CardConfig  `json:"cards"`
	Drives []DriveConfig `json:"drives"`
}

type CardConfig struct {
	Volume string `json:"volume"`
	Sub    string `json:"sub"`
}

type DriveConfig struct {
	Volume   string `json:"volume"`    // display name and /Volumes/<volume> path (if Path not set)
	Path     string `json:"path"`      // explicit path, e.g. "~/Footage" (overrides Volume for path)
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
	return cfg
}
