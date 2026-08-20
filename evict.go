package main

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/vbauerster/mpb/v8"
)

// evictTarget is one hot copy of a mission that would be deleted.
type evictTarget struct {
	vol   string
	dir   string
	files []fileEntry
	size  int64
}

// evictBackup is a cold copy that qualifies as the surviving copy.
type evictBackup struct {
	vol      string
	dir      string
	base     string
	manifest map[string]string // rel → hash; the authority on what must be there
	sizes    map[string]int64  // rel → size on disk, for progress only
	size     int64
}

// evictPlan is one mission's eviction: the hot copies to remove and the cold
// copies that justify removing them.
type evictPlan struct {
	num     int
	slug    string
	targets []evictTarget
	backups []evictBackup
}

// runEvict deletes missions from hot drives once cold copies are proven good.
//
// "Proven" means, for every cold copy relied on: every file on the hot copy is
// listed in that cold copy's checksums.b3, the two manifests agree wherever
// both mention a file, and — unless quick is set — every file the manifest
// lists is re-read from the cold drive and hashed. Nothing is deleted until
// every mission in the batch has cleared that bar.
func runEvict(cfg Config, missions []int, year int, from []string, minCopies int, quick, skipConf bool) {
	yearStr := strconv.Itoa(year)

	wanted := make(map[string]bool, len(from))
	for _, n := range from {
		wanted[normaliseVol(n)] = true
	}

	var plans []evictPlan
	var skipped, refused int
	for _, num := range missions {
		slug, err := findMissionSlug(cfg.Drives, yearStr, num)
		if err != nil {
			fmt.Printf("%s mission %03d: %v\n", red("ERROR"), num, err)
			refused++
			continue
		}

		// hot copies that would be removed
		var targets []evictTarget
		for _, d := range cfg.Drives {
			if d.Role != "hot" {
				continue
			}
			if len(wanted) > 0 && !wanted[normaliseVol(d.name())] {
				continue
			}
			dir := filepath.Join(d.basePath(), d.Root, yearStr, slug)
			if !dirExists(dir) {
				continue
			}
			files, _, _, err := missionFiles(dir)
			if err != nil {
				fmt.Printf("%s scanning %s on %s: %v\n", red("ERROR"), slug, bold(d.name()), err)
				refused++
				targets = nil
				break
			}
			var size int64
			for _, f := range files {
				size += f.size
			}
			targets = append(targets, evictTarget{d.name(), dir, files, size})
		}
		if len(targets) == 0 {
			fmt.Printf("%s\n", dim(fmt.Sprintf("%s not on any hot drive, nothing to evict", slug)))
			skipped++
			continue
		}

		backups, problems := qualifyBackups(cfg, yearStr, slug, num, targets, minCopies)
		if len(problems) > 0 {
			fmt.Printf("%s %s\n", red("✗"), bold(slug))
			for _, p := range problems {
				fmt.Printf("    %s\n", p)
			}
			refused++
			continue
		}
		plans = append(plans, evictPlan{num, slug, targets, backups})
	}

	if len(plans) == 0 {
		if refused > 0 {
			exit(1, "nothing to evict")
		}
		fmt.Println(dim("nothing to evict"))
		return
	}

	printEvictPlan(plans, minCopies, quick)
	if !skipConf && !confirm() {
		exit(0, "aborted")
	}

	if !quick && !verifyBackups(plans) {
		exit(1, "cold copies failed verification — nothing deleted")
	}

	var freed int64
	var removed, failed int
	fmt.Println()
	for _, p := range plans {
		for _, t := range p.targets {
			if err := os.RemoveAll(t.dir); err != nil {
				fmt.Printf("%s removing %s: %v\n", red("ERROR"), t.dir, err)
				failed++
				continue
			}
			fmt.Printf("%s %s %s\n", green("✓"), dim("removed"), t.dir)
			freed += t.size
			removed++
		}
	}
	fmt.Printf("\n%s %s freed from %d location(s)\n", green("✓"), fmtSize(uint64(freed)), removed)
	if failed > 0 || refused > 0 {
		os.Exit(1)
	}
}

// qualifyBackups returns the cold copies that justify deleting the hot ones,
// or the reasons no set of them does.
func qualifyBackups(cfg Config, yearStr, slug string, num int, targets []evictTarget, minCopies int) ([]evictBackup, []string) {
	// every file across every hot copy must survive on cold
	hotFiles := make(map[string]bool)
	for _, t := range targets {
		for _, f := range t.files {
			hotFiles[f.rel] = true
		}
	}

	var backups []evictBackup
	var notes []string
	for _, d := range cfg.Drives {
		if d.Role != "cold" || len(backups) >= minCopies {
			continue
		}
		base := d.basePath()
		dir := filepath.Join(base, d.Root, yearStr, slug)
		if !dirExists(dir) {
			continue
		}
		manifest := readChecksumFile(filepath.Join(dir, "checksums.b3"))
		if len(manifest) == 0 {
			notes = append(notes, fmt.Sprintf("%s has no checksums.b3 — run %s first",
				bold(d.name()), bold(fmt.Sprintf("-checksum %03d", num))))
			continue
		}

		files, err := findFiles(dir)
		if err != nil {
			notes = append(notes, fmt.Sprintf("%s could not be scanned: %v", bold(d.name()), err))
			continue
		}
		sizes := make(map[string]int64, len(files))
		for _, f := range files {
			sizes[f.rel] = f.size
		}

		// Every hot file must be on the cold disk *and* in its manifest. On
		// disk, or there is nothing to keep; in the manifest, or verifying
		// that manifest proves nothing about the file being deleted.
		var uncovered []string
		for rel := range hotFiles {
			if _, ok := sizes[rel]; !ok {
				uncovered = append(uncovered, rel+" (not on disk)")
			} else if manifest[rel] == "" {
				uncovered = append(uncovered, rel+" (not in checksums.b3)")
			}
		}
		if len(uncovered) > 0 {
			sort.Strings(uncovered)
			notes = append(notes, fmt.Sprintf("%s is missing %d file(s) present on the hot drive, first: %s",
				bold(d.name()), len(uncovered), uncovered[0]))
			continue
		}

		// the two copies must not disagree about any file they both record
		var conflicts int
		for _, t := range targets {
			conflicts += len(manifestConflicts(readChecksumFile(filepath.Join(t.dir, "checksums.b3")), manifest))
		}
		if conflicts > 0 {
			notes = append(notes, fmt.Sprintf("%s records %d file(s) with different hashes than the hot copy — run %s",
				bold(d.name()), conflicts, bold(fmt.Sprintf("-check %03d", num))))
			continue
		}

		var size int64
		for rel := range manifest {
			size += sizes[rel] // absent files contribute 0 and fail on read
		}
		backups = append(backups, evictBackup{d.name(), dir, base, manifest, sizes, size})
	}

	if len(backups) < minCopies {
		switch {
		case minCopies > 1:
			notes = append(notes, fmt.Sprintf("only %d of %d required cold copies qualify",
				len(backups), minCopies))
		case len(notes) == 0:
			notes = append(notes, fmt.Sprintf("no cold copy found — run %s first", bold("-sync")))
		}
		return nil, notes
	}
	return backups, nil
}

func printEvictPlan(plans []evictPlan, minCopies int, quick bool) {
	var freed int64
	width := 0
	for _, p := range plans {
		for _, t := range p.targets {
			freed += t.size
		}
		width = max(width, len(p.slug))
	}
	fmt.Printf("evict: %s mission(s), freeing %s\n\n",
		bold(strconv.Itoa(len(plans))), bold(fmtSize(uint64(freed))))
	for _, p := range plans {
		var from, keep []string
		for _, t := range p.targets {
			from = append(from, fmt.Sprintf("%s %s", t.vol, dim(fmtSize(uint64(t.size)))))
		}
		for _, b := range p.backups {
			keep = append(keep, b.vol)
		}
		fmt.Printf("  %-*s  %s %s  %s %s\n", width, p.slug,
			red("−"), strings.Join(from, ", "), dim("keeping"), green(strings.Join(keep, ", ")))
	}
	fmt.Println()
	if quick {
		fmt.Printf("  %s\n", yellow("-quick: trusting checksums.b3 on the cold drives without re-reading them"))
	} else {
		fmt.Printf("  %s\n", dim(fmt.Sprintf("every file will be re-read and hashed on %d cold copy/copies first", minCopies)))
	}
	fmt.Printf("  %s\n\n", dim("the hot copies are deleted only if that passes"))
}

// verifyBackups re-hashes every cold copy the plans rely on.
func verifyBackups(plans []evictPlan) bool {
	type job struct {
		backup evictBackup
		slug   string
	}
	var jobs []job
	sizeByVol := make(map[string]int64)
	baseByVol := make(map[string]string)
	var volOrder []string
	for _, p := range plans {
		for _, b := range p.backups {
			jobs = append(jobs, job{b, p.slug})
			if _, seen := baseByVol[b.vol]; !seen {
				baseByVol[b.vol] = b.base
				volOrder = append(volOrder, b.vol)
			}
			sizeByVol[b.vol] += b.size
		}
	}

	fmt.Printf("%s\n\n", dim("verifying cold copies..."))
	volInfo := make(map[string]driveInfo)
	for _, vol := range volOrder {
		volInfo[vol] = probeDrive(baseByVol[vol])
		fmt.Printf("  %s: %s\n", bold(vol), volInfo[vol])
	}
	fmt.Println()

	p := mpb.New(mpb.WithWidth(64))
	bars := make(map[string]*barTracker, len(volOrder))
	for _, vol := range volOrder {
		bars[vol] = addBar(p, vol, sizeByVol[vol])
	}

	jobsByVol := make(map[string][]job)
	for _, j := range jobs {
		jobsByVol[j.backup.vol] = append(jobsByVol[j.backup.vol], j)
	}

	var failed atomic.Int64
	var pools []*pool
	var submitters []func()
	for vol, volJobs := range jobsByVol {
		bar := bars[vol]
		wp := newPool(volInfo[vol].concurrency)
		pools = append(pools, wp)
		submitters = append(submitters, func() {
			for _, j := range volJobs {
				// Iterate the manifest, not the directory listing: a file the
				// manifest requires but the drive no longer holds must fail,
				// and walking the disk would silently skip it.
				rels := make([]string, 0, len(j.backup.manifest))
				for rel := range j.backup.manifest {
					rels = append(rels, rel)
				}
				sort.Strings(rels)
				for _, rel := range rels {
					want := j.backup.manifest[rel]
					path := filepath.Join(j.backup.dir, rel)
					wp.run(func() {
						got, err := hashFile(path, bar)
						if err != nil {
							fmt.Printf("\n%s %s: %v\n", red("ERROR"), path, err)
							failed.Add(1)
							return
						}
						if got != want {
							fmt.Printf("\n%s %s\n", red("FAIL:"), path)
							failed.Add(1)
						}
					})
				}
			}
		})
	}
	submitAll(submitters)
	for _, wp := range pools {
		wp.wait()
	}
	for _, t := range bars {
		t.flush()
	}
	p.Wait()

	if n := failed.Load(); n > 0 {
		fmt.Printf("\n%s %d file(s) failed on the cold copies\n", red("ERROR"), n)
		return false
	}
	fmt.Printf("\n%s cold copies verified\n", green("✓"))
	return true
}
