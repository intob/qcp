package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// The catalog remembers what each drive held when qcp last saw it, so that the
// drives that are not plugged in — the archive in a drawer, the hot drive left
// at home — still count. Without it every answer depended on what happened to
// be mounted: the counter check had to stay silent, -list and -status showed an
// unmounted drive as empty, and the duplicate-ingest warning could not see
// anything that had been evicted to the archive.
//
// It is a cache and never an authority. The drives and their checksums.b3 are
// the truth, which is why the catalog holds no hashes: it only says which
// missions and files a drive had, and when. A mounted drive is always read
// directly and overwrites what the catalog said about it. Nothing is ever
// deleted, renamed or rewound on the catalog's word alone — it can raise the
// mission counter, which is always safe, and it can inform, but moving the
// counter back still needs the drives themselves. Losing it costs nothing:
// every mounted drive is re-catalogued as qcp finishes.
//
// One file per drive under ~/.qcp_catalog/, so a drive's entry is replaced
// whole and the drives never contend for one file.

const catalogDirPath = "~/.qcp_catalog"

const catalogVersion = 1

type driveCatalog struct {
	Version int                    `json:"version"`
	Drive   string                 `json:"drive"`
	Years   map[string]catalogYear `json:"years"` // keyed by year, as on the drive
	Space   driveSpace             `json:"space,omitzero"`
}

// catalogYear is one year directory as it was when Scanned. A drive that was
// mounted with no directory for the year is recorded with no missions, which
// is a fact about the drive; a year that is absent from Years was never seen.
type catalogYear struct {
	Scanned  time.Time                 `json:"scanned"`
	Missions map[string]catalogMission `json:"missions"`
}

type catalogMission struct {
	Files       map[string]int64 `json:"files"` // content rel → size, as contentFiles lists it
	Size        int64            `json:"size"`
	Checksummed bool             `json:"checksummed"`       // checksums.b3 covered every file
	Verified    time.Time        `json:"verified,omitzero"` // last full -verify of this copy, if it still counts
}

func catalogPath(drive string) (string, error) {
	dir, err := expandPath(catalogDirPath)
	if err != nil {
		return "", err
	}
	// A drive name is a volume name or a path's basename; keep it a filename.
	safe := strings.Map(func(r rune) rune {
		if r == '/' || r == os.PathSeparator || r == 0 {
			return '_'
		}
		return r
	}, drive)
	return filepath.Join(dir, safe+".json"), nil
}

// readCatalog returns what the catalog holds for a drive. A drive that was
// never catalogued, or whose entry cannot be read, has an empty catalog: the
// catalog is only ever a cache, so a damaged entry is dropped and rebuilt the
// next time the drive is mounted rather than stopping anything.
func readCatalog(drive string) driveCatalog {
	empty := driveCatalog{Version: catalogVersion, Drive: drive, Years: map[string]catalogYear{}}
	p, err := catalogPath(drive)
	if err != nil {
		return empty
	}
	data, err := os.ReadFile(p)
	if err != nil {
		return empty
	}
	var c driveCatalog
	if err := json.Unmarshal(data, &c); err != nil || c.Version != catalogVersion {
		return empty
	}
	if c.Years == nil {
		c.Years = map[string]catalogYear{}
	}
	return c
}

func writeCatalog(c driveCatalog) error {
	p, err := catalogPath(c.Drive)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(p), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(c, "", " ")
	if err != nil {
		return err
	}
	return writeFileAtomic(p, append(data, '\n'), 0644)
}

// scanCatalogYear reads one year directory into a catalogYear. A year
// directory that does not exist is an empty year; any other failure is an
// error, so that a drive which could not be read is not recorded as empty.
func scanCatalogYear(yearDir string) (catalogYear, error) {
	cy := catalogYear{Scanned: time.Now(), Missions: map[string]catalogMission{}}
	if _, err := os.Stat(yearDir); os.IsNotExist(err) {
		return cy, nil
	} else if err != nil {
		return cy, err
	}
	if _, err := os.ReadDir(yearDir); err != nil {
		return cy, err
	}
	for _, slug := range missionDirs(yearDir) {
		dir := filepath.Join(yearDir, slug)
		files, err := contentFiles(dir)
		if err != nil {
			return cy, fmt.Errorf("%s: %w", slug, err)
		}
		manifest, err := readChecksums(filepath.Join(dir, "checksums.b3"))
		if err != nil {
			return cy, fmt.Errorf("%s: %w", slug, err)
		}
		m := catalogMission{
			Files:       make(map[string]int64, len(files)),
			Checksummed: len(manifest) > 0 && len(files) > 0,
			Verified:    lastVerified(dir),
		}
		for _, f := range files {
			m.Files[f.rel] = f.size
			m.Size += f.size
			if manifest[f.rel] == "" {
				m.Checksummed = false
			}
		}
		cy.Missions[slug] = m
	}
	return cy, nil
}

// refreshCatalog re-catalogues every mounted drive for the given years, or
// for every year on it when years is nil. A year that could not be read keeps
// what the catalog said before. With years nil, a catalogued year whose
// directory is no longer on the drive is recorded as empty, since the whole
// drive was seen.
func refreshCatalog(cfg Config, years []int) {
	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		root := filepath.Join(base, d.Root)
		c := readCatalog(d.name())

		scan := years
		if scan == nil {
			seen := map[int]bool{}
			for _, dir := range cleanRoots(root, 0, false) {
				y, _ := strconv.Atoi(filepath.Base(dir))
				seen[y] = true
			}
			for k := range c.Years {
				if y, err := strconv.Atoi(k); err == nil {
					seen[y] = true // gone from the drive: rescanned as empty
				}
			}
			for y := range seen {
				scan = append(scan, y)
			}
		}

		changed := false
		if sp, err := readDriveSpace(base); err == nil {
			c.Space = sp
			changed = true
		}
		for _, y := range scan {
			cy, err := scanCatalogYear(filepath.Join(root, strconv.Itoa(y)))
			if err != nil {
				fmt.Printf("%s cataloguing %s %d: %v %s\n", yellow("warning:"), d.name(), y, err, dim("(kept the previous entry)"))
				continue
			}
			c.Years[strconv.Itoa(y)] = cy
			changed = true
		}
		if !changed {
			continue
		}
		if err := writeCatalog(c); err != nil {
			fmt.Printf("%s writing catalog for %s: %v\n", yellow("warning:"), d.name(), err)
		}
	}
}

// catalogued returns, for each configured drive that is not mounted, what the
// catalog last saw of the year on it. Drives never seen with that year are
// left out.
func catalogued(cfg Config, year int) map[string]catalogYear {
	out := make(map[string]catalogYear)
	for _, d := range cfg.Drives {
		if dirExists(d.basePath()) {
			continue
		}
		if cy, ok := readCatalog(d.name()).Years[strconv.Itoa(year)]; ok {
			out[d.name()] = cy
		}
	}
	return out
}

// maxMission is the highest mission number the year held.
func (cy catalogYear) maxMission() int {
	max := 0
	for slug := range cy.Missions {
		if n, ok := parseMissionNum(slug); ok && n > max {
			max = n
		}
	}
	return max
}

// scan presents a catalogued mission the way scanMissions presents a mounted one.
func (cy catalogYear) scan(slug string) (missionScan, bool) {
	m, ok := cy.Missions[slug]
	if !ok {
		return missionScan{}, false
	}
	return missionScan{size: m.Size, files: len(m.Files), checksummed: m.Checksummed}, true
}

// lastSeen renders when a drive was last catalogued, for messages.
func lastSeen(t time.Time) string {
	t = t.Local() // verified stamps are kept in UTC
	if t.Year() == time.Now().Year() {
		return t.Format("2 Jan")
	}
	return t.Format("2 Jan 2006")
}

// runCatalog re-catalogues every mounted drive in full and reports what the
// catalog holds for every configured drive.
func runCatalog(cfg Config) {
	fmt.Println(dim("cataloguing mounted drives..."))
	refreshCatalog(cfg, nil)
	fmt.Println()

	width := 0
	for _, d := range cfg.Drives {
		width = max(width, len(d.name()))
	}
	for _, d := range cfg.Drives {
		c := readCatalog(d.name())
		state := dim("not mounted")
		if dirExists(d.basePath()) {
			state = green("mounted")
		}
		if sp := c.Space; sp.Total > 0 {
			state += "  " + dim(fmt.Sprintf("%s free of %s · seen %s", fmtSize(sp.Avail), fmtSize(sp.Total), lastSeen(sp.Seen)))
		}
		fmt.Printf("%s  %s\n", bold(fmt.Sprintf("%-*s", width, d.name())), state)
		if len(c.Years) == 0 {
			fmt.Printf("  %s\n", dim("never catalogued"))
			continue
		}
		var keys []string
		for k := range c.Years {
			keys = append(keys, k)
		}
		sort.Sort(sort.Reverse(sort.StringSlice(keys)))
		for _, k := range keys {
			cy := c.Years[k]
			if len(cy.Missions) == 0 {
				fmt.Printf("  %s  %s\n", k, dim("no missions · seen "+lastSeen(cy.Scanned)))
				continue
			}
			var size int64
			for _, m := range cy.Missions {
				size += m.Size
			}
			fmt.Printf("  %s  %3d mission(s), up to %03d  %s\n", k, len(cy.Missions), cy.maxMission(),
				dim(fmtSize(uint64(size))+" · seen "+lastSeen(cy.Scanned)+" · "+cy.verified().String()))
		}
	}
}
