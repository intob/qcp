package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// runInit resets the mission counter to the highest mission number found on the
// mounted drives.
//
// scopeToYear limits the scan to one year directory rather than every year on
// the drives. rewindOK allows the counter to move *down*, which only a -year the
// user actually typed grants: the counter is a promise never to mint a number
// twice, and what a scan can see depends on what happens to be plugged in. The
// archive being unmounted is the normal state, and -evict exists to take old
// missions off the hot drives, so a bare -init could find nothing above 030 for
// a year whose counter had reached 042 and hand the next ingest a number that
// already names a mission on a drive in a drawer.
func runInit(cfg Config, year int, scopeToYear, rewindOK bool) {
	seq, err := readSeq()
	if err != nil {
		exit(1, "err reading seq: %v", err)
	}

	maxByYear := make(map[int]int)

	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		rootDir := filepath.Join(base, d.Root)
		if scopeToYear {
			scanYearDir(filepath.Join(rootDir, strconv.Itoa(year)), year, maxByYear)
		} else {
			entries, err := os.ReadDir(rootDir)
			if err != nil {
				continue
			}
			for _, e := range entries {
				if !e.IsDir() {
					continue
				}
				y, err := strconv.Atoi(e.Name())
				if err != nil {
					continue
				}
				scanYearDir(filepath.Join(rootDir, e.Name()), y, maxByYear)
			}
		}
	}

	if len(maxByYear) == 0 {
		fmt.Println("no missions found on any mounted drive")
		return
	}

	var years []int
	for y := range maxByYear {
		years = append(years, y)
	}
	sort.Ints(years)

	changed := false
	for _, y := range years {
		max := maxByYear[y]
		current := seq[y]
		// Raising is always safe. Lowering is the repair for a counter that ran
		// ahead of the drives, and needs the year to have been asked for by
		// name — it is only ever right when the person running it knows every
		// drive holding that year is mounted.
		if max == current {
			fmt.Printf("  %d: %03d (already up to date)\n", y, current)
			continue
		}
		if max < current && !rewindOK {
			fmt.Printf("  %d: %03d %s\n", y, current,
				dim(fmt.Sprintf("(drives show %03d — pass -year %d to move the counter back)", max, y)))
			continue
		}
		fmt.Printf("  %d: %03d → %03d\n", y, current, max)
		if max < current {
			fmt.Printf("     %s\n", yellow("counter moved back — every drive holding this year must be mounted"))
		}
		seq[y] = max
		changed = true
	}

	if !changed {
		fmt.Println("seq already up to date")
		return
	}
	if err := writeSeq(seq); err != nil {
		exit(1, "err writing seq: %v", err)
	}
	fmt.Println("seq updated")
}

func scanYearDir(yearDir string, year int, maxByYear map[int]int) {
	entries, err := os.ReadDir(yearDir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		parts := strings.SplitN(e.Name(), "_", 2)
		if len(parts) < 2 {
			continue
		}
		n, err := strconv.Atoi(parts[0])
		if err != nil || n <= 0 {
			continue
		}
		if n > maxByYear[year] {
			maxByYear[year] = n
		}
	}
}
