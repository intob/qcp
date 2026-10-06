package main

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

type dayGroup struct {
	date      string
	cards     []scannedCard // files filtered to this date only
	fileCount int
	totalSize int64
}

var mSuffixRe = regexp.MustCompile(`M\d+$`)

// clipStem derives the clip base name, stripping the extension and XDCAM M-suffix.
// "923_0272M01.XML" → "923_0272"
// "923_0272.MXF"    → "923_0272"
func clipStem(name string) string {
	base := name[:len(name)-len(filepath.Ext(name))]
	return mSuffixRe.ReplaceAllString(base, "")
}

// parseCreationDate reads the CreationDate field from an XDCAM XML sidecar.
// Returns "YYYY-MM-DD" on success.
func parseCreationDate(xmlPath string) (string, error) {
	data, err := os.ReadFile(xmlPath)
	if err != nil {
		return "", err
	}
	const marker = `<CreationDate value="`
	idx := bytes.Index(data, []byte(marker))
	if idx < 0 {
		return "", fmt.Errorf("no CreationDate")
	}
	s := idx + len(marker)
	if s+10 > len(data) {
		return "", fmt.Errorf("truncated")
	}
	return string(data[s : s+10]), nil
}

// groupAllByDate groups files from all scanned cards by recording date.
// Dates are derived from XDCAM XML sidecars; non-XML files whose stem matches
// a sidecar inherit that date; unmatched files fall back to filesystem mtime.
func groupAllByDate(scanned []scannedCard) []dayGroup {
	type slot struct {
		mc    mountedCard
		files []fileEntry
	}
	dateSlots := map[string][]slot{}

	for _, sc := range scanned {
		// Build clip-stem → date from XML sidecars
		stemDate := map[string]string{}
		for _, f := range sc.files {
			if !strings.EqualFold(filepath.Ext(f.rel), ".xml") {
				continue
			}
			date, err := parseCreationDate(filepath.Join(sc.src, f.rel))
			if err != nil {
				continue
			}
			stem := clipStem(filepath.Base(f.rel))
			if stem != "" {
				stemDate[stem] = date
			}
		}

		byDate := map[string][]fileEntry{}
		for _, f := range sc.files {
			stem := clipStem(filepath.Base(f.rel))
			date, ok := stemDate[stem]
			if !ok {
				info, err := os.Stat(filepath.Join(sc.src, f.rel))
				if err != nil {
					date = time.Now().Format("2006-01-02")
				} else {
					date = info.ModTime().Format("2006-01-02")
				}
			}
			byDate[date] = append(byDate[date], f)
		}

		for date, files := range byDate {
			dateSlots[date] = append(dateSlots[date], slot{sc.mountedCard, files})
		}
	}

	dates := make([]string, 0, len(dateSlots))
	for d := range dateSlots {
		dates = append(dates, d)
	}
	sort.Strings(dates)

	var groups []dayGroup
	for _, date := range dates {
		var dayCards []scannedCard
		var count int
		var size int64
		for _, sl := range dateSlots[date] {
			dayCards = append(dayCards, scannedCard{sl.mc, sl.files})
			count += len(sl.files)
			for _, f := range sl.files {
				size += f.size
			}
		}
		groups = append(groups, dayGroup{date, dayCards, count, size})
	}
	return groups
}

// promptMissionForDay asks the user to assign a recording day to a mission.
// nextNum is the mission number to use if a new mission is created (caller increments between days).
// suggestion is shown in brackets and accepted on empty input.
// Returns slug, whether it's a new mission, the new mission number (0 if appending), and whether the day was skipped.
func promptMissionForDay(cfg Config, year, nextNum int, date, suggestion string) (slug string, isNew bool, num int, skip bool, err error) {
	yearStr := strconv.Itoa(year)
	for {
		if suggestion != "" {
			fmt.Printf("  %s [%s]: ", date, suggestion)
		} else {
			fmt.Printf("  %s: ", date)
		}
		// The read error used to be ignored, so with input closed or used up
		// an empty answer was asked for again, forever.
		line, readErr := readLine()
		if readErr != nil && line == "" {
			return "", false, 0, false, fmt.Errorf("no answer for %s: input ended", date)
		}
		if line == "" {
			if suggestion != "" {
				line = suggestion
			} else {
				continue
			}
		}
		// - → skip this day
		if line == "-" {
			return "", false, 0, true, nil
		}
		// Number → append to existing mission
		if n, e := strconv.Atoi(line); e == nil && n > 0 {
			s, e := findMissionSlug(cfg.Drives, yearStr, n)
			if e != nil {
				fmt.Printf("  mission %03d not found\n", n)
				continue
			}
			return s, false, 0, false, nil
		}
		// Name → new mission using the pre-computed nextNum
		name := sanitizeMission(line)
		if name == "" {
			fmt.Printf("  %q is not usable as a mission name\n", line)
			continue
		}
		return fmt.Sprintf("%03d_%s", nextNum, name), true, nextNum, false, nil
	}
}

// checkDuplicateIngest walks the year directory on each mounted drive and
// returns a map of mission slug → number of card filenames already present
// in that mission. Only missions with at least one match are returned.
func checkDuplicateIngest(drives []DriveConfig, yearStr string, scanned []scannedCard) map[string]int {
	// Collect all base filenames from the cards.
	cardFiles := make(map[string]bool, len(scanned)*100)
	for _, sc := range scanned {
		for _, f := range sc.files {
			cardFiles[filepath.Base(f.rel)] = true
		}
	}

	hits := map[string]int{}
	seen := map[string]bool{} // deduplicate across drives

	for _, d := range drives {
		base := d.basePath()
		yearDir := filepath.Join(base, d.Root, yearStr)
		if !dirExists(yearDir) {
			continue
		}
		entries, err := os.ReadDir(yearDir)
		if err != nil {
			continue
		}
		for _, mission := range entries {
			if !mission.IsDir() {
				continue
			}
			slug := mission.Name()
			fs.WalkDir(os.DirFS(filepath.Join(yearDir, slug)), ".", func(path string, de fs.DirEntry, err error) error {
				if err != nil || de.IsDir() {
					return nil
				}
				name := filepath.Base(path)
				key := slug + "/" + name
				if cardFiles[name] && !seen[key] {
					seen[key] = true
					hits[slug]++
				}
				return nil
			})
		}
	}

	// Drives that are not mounted, as the catalog last saw them: footage that
	// has been evicted to the archive is the likeliest to be ingested twice,
	// and the archive is the drive least likely to be plugged in.
	for _, d := range drives {
		if dirExists(d.basePath()) {
			continue
		}
		cy, ok := readCatalog(d.name()).Years[yearStr]
		if !ok {
			continue
		}
		for slug, m := range cy.Missions {
			for rel := range m.Files {
				name := filepath.Base(rel)
				key := slug + "/" + name
				if cardFiles[name] && !seen[key] {
					seen[key] = true
					hits[slug]++
				}
			}
		}
	}
	return hits
}

// presentFile is a card file whose destination name already exists.
type presentFile struct {
	src, dst, rel, dstRoot string
	size                   int64
}

// checkAlreadyCopied decides whether card files whose destination name is
// already taken really are already copied, before the ingest skips them.
//
// The ingest used to skip any file whose destination existed, by name alone.
// Cards land under <mission>/<volume name>/, and card volume names repeat —
// "Untitled", "NO NAME" — while camera clip counters can reset, so appending a
// second card into a mission could match a clip from the first card by name and
// never copy it. The card is formatted once the ingest says it is done, so that
// clip was lost. Now a taken name is only accepted when the sizes agree and the
// card file hashes to what the drive holds: its checksums.b3 entry when there
// is one, otherwise the file itself.
//
// It returns the manifest lines to add for files that matched but were not yet
// recorded (a run killed between copying and writing the manifest leaves those),
// and one message per file that does not match.
func checkAlreadyCopied(files []presentFile) (record map[string][]string, conflicts []string) {
	record = make(map[string][]string)
	if len(files) == 0 {
		return record, nil
	}
	manifests := make(map[string]map[string]string)
	manifestErrs := make(map[string]error)
	for _, f := range files {
		if _, ok := manifests[f.dstRoot]; !ok {
			manifests[f.dstRoot], manifestErrs[f.dstRoot] = readChecksums(filepath.Join(f.dstRoot, "checksums.b3"))
		}
	}

	var mu sync.Mutex
	srcHashes := make(map[string]string) // a card file shared by several drives is read once
	srcErrs := make(map[string]error)
	hashSrc := func(src string) (string, error) {
		mu.Lock()
		h, ok := srcHashes[src]
		err := srcErrs[src]
		mu.Unlock()
		if ok || err != nil {
			return h, err
		}
		h, err = hashFile(src, nil)
		mu.Lock()
		srcHashes[src], srcErrs[src] = h, err
		mu.Unlock()
		return h, err
	}

	conflict := func(f presentFile, why string) {
		mu.Lock()
		conflicts = append(conflicts, fmt.Sprintf("%s: %s", f.dst, why))
		mu.Unlock()
	}

	// Group by card so each card is read by one worker at a time; card readers
	// are the slow end and do not reward parallel reads.
	bySrc := make(map[string][]presentFile)
	var order []string
	for _, f := range files {
		if _, ok := bySrc[f.src]; !ok {
			order = append(order, f.src)
		}
		bySrc[f.src] = append(bySrc[f.src], f)
	}
	wp := newPool(4)
	for _, src := range order {
		group := bySrc[src]
		wp.run(func() {
			for _, f := range group {
				if err := manifestErrs[f.dstRoot]; err != nil {
					conflict(f, "checksums.b3 could not be read: "+err.Error())
					continue
				}
				info, err := os.Stat(f.dst)
				if err != nil {
					conflict(f, err.Error())
					continue
				}
				if info.Size() != f.size {
					conflict(f, fmt.Sprintf("already exists with a different size (%d bytes on the drive, %d on the card)", info.Size(), f.size))
					continue
				}
				h, err := hashSrc(f.src)
				if err != nil {
					conflict(f, "card file could not be read: "+err.Error())
					continue
				}
				if want := manifests[f.dstRoot][f.rel]; want != "" {
					if want != h {
						conflict(f, "already exists with different content (checksums.b3 disagrees with the card)")
					}
					continue
				}
				got, err := hashFile(f.dst, nil)
				if err != nil {
					conflict(f, "could not be read: "+err.Error())
					continue
				}
				if got != h {
					conflict(f, "already exists with different content")
					continue
				}
				mu.Lock()
				record[f.dstRoot] = append(record[f.dstRoot], fmt.Sprintf("%s  %s", h, f.rel))
				mu.Unlock()
			}
		})
	}
	wp.wait()
	sort.Strings(conflicts)
	return record, conflicts
}
