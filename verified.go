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

// Footage on an archive drive sits unread for months, and a file that rots in
// that time is only found by reading it. -verify reads every file, but nothing
// recorded when it last did, so there was no telling which copies had gone
// longest unchecked, and no way to work through a large archive a few hours at
// a time.
//
// Each copy of a mission now carries a stamp of its last full verification, in
// a dotfile beside its checksums.b3. It lives on the drive rather than in the
// catalog because it is a fact about that copy, and the catalog is a cache that
// may be rebuilt from the drives at any time; the catalog keeps a copy of the
// date so that drives which are not mounted can still be reported on. Like the
// flags file it is invisible to findFiles, so it is never checksummed, synced
// or counted as footage.
//
// A stamp vouches for the manifest it was verified against: it records a hash
// of checksums.b3, and once the manifest changes — an append, a sync filling a
// gap, -organise moving files in or out — the stamp no longer counts, because
// the files it covered are no longer the files the manifest describes.

const verifiedFileName = ".qcp-verified.json"

type verifiedStamp struct {
	Version  int       `json:"version"`
	Verified time.Time `json:"verified"` // when the verification began
	Files    int       `json:"files"`
	Manifest string    `json:"manifest"` // BLAKE3 of checksums.b3 as verified
}

// manifestDigest hashes a copy's checksums.b3, so a stamp can tell whether the
// manifest is still the one it verified.
func manifestDigest(dir string) (string, error) {
	return hashFile(filepath.Join(dir, "checksums.b3"), nil)
}

// lastVerified returns when the copy at dir was last verified in full, or the
// zero time if it never was, or its manifest has changed since.
func lastVerified(dir string) time.Time {
	raw, err := os.ReadFile(filepath.Join(dir, verifiedFileName))
	if err != nil {
		return time.Time{}
	}
	var s verifiedStamp
	if json.Unmarshal(raw, &s) != nil || s.Version != 1 {
		return time.Time{}
	}
	if d, err := manifestDigest(dir); err != nil || d != s.Manifest {
		return time.Time{}
	}
	return s.Verified
}

// recordVerified stamps a copy that passed in full: every recorded file read
// back and matched, and nothing on disk left unrecorded. A drive that refuses
// the write is reported, never failed: the verification itself stands.
func recordVerified(vol, dir string, vc verifyCopy, began time.Time) {
	stampVerified(vol, dir, vc.digest, len(vc.entries), began)
}

// stampVerified writes the stamp for a copy whose manifest hashed to digest
// before any of its files were read.
func stampVerified(vol, dir, digest string, files int, began time.Time) {
	if digest == "" {
		return
	}
	data, err := json.MarshalIndent(verifiedStamp{
		Version: 1, Verified: began.UTC(), Files: files, Manifest: digest,
	}, "", "  ")
	if err == nil {
		err = writeFileAtomic(filepath.Join(dir, verifiedFileName), append(data, '\n'), 0644)
	}
	if err != nil {
		fmt.Printf("%s recording the verification on %s: %v\n", yellow("warning:"), vol, err)
	}
}

// verifiedSummary is how recently the missions on one drive were verified.
type verifiedSummary struct {
	missions int
	verified int
	oldest   time.Time // the longest-unchecked of the verified ones
}

func (s *verifiedSummary) add(t time.Time) {
	s.missions++
	if t.IsZero() {
		return
	}
	s.verified++
	if s.oldest.IsZero() || t.Before(s.oldest) {
		s.oldest = t
	}
}

func (s verifiedSummary) String() string {
	switch {
	case s.missions == 0:
		return ""
	case s.verified == 0:
		return "never verified"
	case s.verified == s.missions:
		return "verified, oldest " + lastSeen(s.oldest)
	default:
		return fmt.Sprintf("verified %d of %d, oldest %s", s.verified, s.missions, lastSeen(s.oldest))
	}
}

// yearVerified summarises a year directory on a mounted drive.
func yearVerified(yearDir string) verifiedSummary {
	var s verifiedSummary
	for _, slug := range missionDirs(yearDir) {
		s.add(lastVerified(filepath.Join(yearDir, slug)))
	}
	return s
}

// verified summarises a catalogued year.
func (cy catalogYear) verified() verifiedSummary {
	var s verifiedSummary
	for _, m := range cy.Missions {
		s.add(m.Verified)
	}
	return s
}

// scrubCopy is one copy of a mission that -verify oldest may re-read.
type scrubCopy struct {
	drive DriveConfig
	year  string
	slug  string
	last  time.Time
}

func (c scrubCopy) String() string {
	when := "never verified"
	if !c.last.IsZero() {
		when = "last verified " + lastSeen(c.last)
	}
	return fmt.Sprintf("%s/%s on %s, %s", c.year, c.slug, c.drive.name(), when)
}

// scrubQueue lists every copy on the mounted drives for the given years (all of
// them when years is nil), longest unchecked first: copies never verified, by
// year and then mission, then the rest by the date of their last verification.
// Copies with no checksums.b3 cannot be verified and are counted instead.
func scrubQueue(cfg Config, years []int) (queue []scrubCopy, unrecorded int) {
	for _, d := range cfg.Drives {
		base := d.basePath()
		if !dirExists(base) {
			continue
		}
		root := filepath.Join(base, d.Root)
		var yearDirs []string
		if years == nil {
			yearDirs = cleanRoots(root, 0, false)
		} else {
			for _, y := range years {
				yearDirs = append(yearDirs, cleanRoots(root, y, true)...)
			}
		}
		for _, yd := range yearDirs {
			for _, slug := range missionDirs(yd) {
				dir := filepath.Join(yd, slug)
				if !fileExists(filepath.Join(dir, "checksums.b3")) {
					unrecorded++
					continue
				}
				queue = append(queue, scrubCopy{d, filepath.Base(yd), slug, lastVerified(dir)})
			}
		}
	}
	sort.SliceStable(queue, func(i, j int) bool {
		a, b := queue[i], queue[j]
		if a.last.IsZero() != b.last.IsZero() {
			return a.last.IsZero()
		}
		if !a.last.Equal(b.last) {
			return a.last.Before(b.last)
		}
		if a.year != b.year {
			return a.year < b.year
		}
		return a.slug < b.slug
	})
	return queue, unrecorded
}

func copies(n int) string {
	if n == 1 {
		return "1 copy"
	}
	return strconv.Itoa(n) + " copies"
}

// runVerifyOldest re-verifies the copies that have gone longest unchecked, one
// copy at a time, until budget runs out. The copy in progress when it does is
// finished, and at least one copy is always verified, so a short budget still
// makes progress. Each copy that passes is stamped as it finishes, so stopping
// early — or a Ctrl-C — loses nothing already done.
func runVerifyOldest(cfg Config, years []int, budget time.Duration) bool {
	queue, unrecorded := scrubQueue(cfg, years)
	if len(queue) == 0 {
		fmt.Println(dim("no checksummed missions on any mounted drive"))
		return unrecorded == 0
	}

	never := 0
	for _, c := range queue {
		if c.last.IsZero() {
			never++
		}
	}
	fmt.Printf("%s %s on the mounted drives · %d never verified · budget %s\n",
		dim("verifying oldest first:"), copies(len(queue)), never, budget)
	if unrecorded > 0 {
		fmt.Printf("%s %s with no checksums.b3 cannot be verified — run -checksum on them\n",
			yellow("!"), copies(unrecorded))
	}
	fmt.Println()

	volInfos := make(map[string]driveInfo)
	for _, c := range queue {
		if _, ok := volInfos[c.drive.name()]; !ok {
			volInfos[c.drive.name()] = probeDrive(c.drive.basePath())
		}
	}

	start := time.Now()
	ok := true
	done, failed := 0, 0
	for _, c := range queue {
		if done > 0 && time.Since(start) >= budget {
			break
		}
		if !verifySlugOn([]DriveConfig{c.drive}, c.slug, c.year, volInfos, c.String()) {
			ok = false
			failed++
		}
		done++
	}

	elapsed := time.Since(start).Round(time.Second)
	fmt.Printf("\n%s %s in %s", dim("verified"), copies(done), elapsed)
	if failed > 0 {
		fmt.Printf(" · %s", red(strconv.Itoa(failed)+" failed"))
	}
	fmt.Println()
	if left := len(queue) - done; left > 0 {
		fmt.Printf("%s\n", dim(fmt.Sprintf("%d left · next: %s", left, queue[done])))
	}
	return ok && unrecorded == 0
}
