package main

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
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
// Returns slug, whether it's a new mission, and the new mission number (0 if appending).
func promptMissionForDay(cfg Config, year, nextNum int, date, suggestion string) (slug string, isNew bool, num int, err error) {
	reader := bufio.NewReader(os.Stdin)
	yearStr := strconv.Itoa(year)
	for {
		if suggestion != "" {
			fmt.Printf("  %s [%s]: ", date, suggestion)
		} else {
			fmt.Printf("  %s: ", date)
		}
		line, _ := reader.ReadString('\n')
		line = strings.TrimSpace(line)
		if line == "" {
			if suggestion != "" {
				line = suggestion
			} else {
				continue
			}
		}
		// Number → append to existing mission
		if n, e := strconv.Atoi(line); e == nil && n > 0 {
			s, e := findMissionSlug(cfg.Drives, yearStr, n)
			if e != nil {
				fmt.Printf("  mission %03d not found\n", n)
				continue
			}
			return s, false, 0, nil
		}
		// Name → new mission using the pre-computed nextNum
		return fmt.Sprintf("%03d_%s", nextNum, sanitizeMission(line)), true, nextNum, nil
	}
}
