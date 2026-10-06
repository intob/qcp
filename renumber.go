package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// runRenumber closes gaps and duplicates in a year's mission numbers, on every
// drive at once.
//
// It refuses unless every drive that can hold the year is mounted. A number
// names one mission across every drive, so renaming only the drives that are
// plugged in left the archive under the old numbers — the same mission under
// two numbers, and a number on the hot drive naming a different mission than
// the same number on the archive. The missions on an unmounted drive were also
// invisible to the numbering, so they could be handed numbers that were taken,
// and the counter was then set to the count of missions it could see, moving
// it back below numbers that were already spent.
func runRenumber(cfg Config, year int, skipConf bool) {
	yearStr := strconv.Itoa(year)

	type driveYear struct {
		d       DriveConfig
		yearDir string
	}
	var drives []driveYear
	slugSet := make(map[string]bool)

	var absent []string
	for _, d := range cfg.Drives {
		if d.coversYear(year) && !dirExists(d.basePath()) {
			absent = append(absent, d.name())
		}
	}
	if len(absent) > 0 {
		fmt.Printf("%s every drive that can hold %d must be mounted to renumber it — not mounted: %s\n",
			red("ERROR"), year, bold(strings.Join(absent, ", ")))
		quit(1)
	}

	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		yearDir := filepath.Join(base, d.Root, yearStr)
		if !dirExists(yearDir) {
			continue
		}
		drives = append(drives, driveYear{d, yearDir})
		entries, err := os.ReadDir(yearDir)
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
		fmt.Println(dim("no numbered missions found"))
		return
	}

	var slugs []string
	for s := range slugSet {
		slugs = append(slugs, s)
	}
	// sort by current number, then name for deterministic tie-breaking
	sort.Slice(slugs, func(i, j int) bool {
		ni := slugNum(slugs[i])
		nj := slugNum(slugs[j])
		if ni != nj {
			return ni < nj
		}
		return slugs[i] < slugs[j]
	})

	type rename struct{ from, to string }
	var renames []rename
	for i, slug := range slugs {
		parts := strings.SplitN(slug, "_", 2)
		newSlug := fmt.Sprintf("%03d_%s", i+1, parts[1])
		if newSlug != slug {
			renames = append(renames, rename{slug, newSlug})
		}
	}

	if len(renames) == 0 {
		fmt.Println(dim("missions are already numbered sequentially"))
		return
	}

	fmt.Printf("renumbering %s in %d:\n\n", bold(fmt.Sprintf("%d mission(s)", len(renames))), year)
	for _, r := range renames {
		fmt.Printf("  %s → %s\n", dim(r.from), bold(r.to))
	}
	fmt.Println()

	if !skipConf && !confirm() {
		return
	}

	for _, dy := range drives {
		// two-pass rename via tmp to avoid collisions between old and new slugs
		for _, r := range renames {
			src := filepath.Join(dy.yearDir, r.from)
			if !dirExists(src) {
				continue
			}
			tmp := filepath.Join(dy.yearDir, "__rnm_"+r.from)
			if err := os.Rename(src, tmp); err != nil {
				fmt.Printf("%s rename %s on %s: %v\n", red("ERROR"), r.from, dy.d.name(), err)
			}
		}
		var done int
		for _, r := range renames {
			tmp := filepath.Join(dy.yearDir, "__rnm_"+r.from)
			if !dirExists(tmp) {
				continue
			}
			dst := filepath.Join(dy.yearDir, r.to)
			if err := os.Rename(tmp, dst); err != nil {
				fmt.Printf("%s rename %s on %s: %v\n", red("ERROR"), r.from, dy.d.name(), err)
				// restore original name so the dir isn't stranded as __rnm_...
				if restoreErr := os.Rename(tmp, filepath.Join(dy.yearDir, r.from)); restoreErr != nil {
					fmt.Printf("%s could not restore %s — directory stuck as %s\n",
						red("ERROR"), r.from, "__rnm_"+r.from)
				}
				continue
			}
			// checksums.b3 stays: its paths are relative to the mission
			// directory, so renaming the directory changes none of them.
			done++
		}
		fmt.Printf("%s %s: renumbered %d mission(s)\n", green("✓"), bold(dy.d.name()), done)
	}

	seq, err := readSeq()
	if err != nil {
		fmt.Printf("%s reading seq: %v\n", red("ERROR"), err)
		return
	}
	seq[year] = len(slugs)
	if err := writeSeq(seq); err != nil {
		fmt.Printf("%s writing seq: %v\n", red("ERROR"), err)
	}
}

func slugNum(slug string) int {
	n, _ := strconv.Atoi(strings.SplitN(slug, "_", 2)[0])
	return n
}
