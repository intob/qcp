package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

func runRenumber(cfg Config, year int, skipConf bool) {
	yearStr := strconv.Itoa(year)

	type driveYear struct {
		d       DriveConfig
		yearDir string
	}
	var drives []driveYear
	slugSet := make(map[string]bool)

	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			fmt.Printf("%s %s %s\n", yellow("warning:"), bold(d.name()), dim("not mounted, skipping"))
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
			// remove stale checksums.b3 — path has changed
			if err := os.Remove(filepath.Join(dst, "checksums.b3")); err == nil {
				fmt.Printf("removed stale checksums: %s\n", filepath.Join(dst, "checksums.b3"))
			}
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
