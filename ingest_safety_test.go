package main

import (
	"bufio"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The ingest skipped any card file whose destination name was already taken.
// Card volume names repeat and clip counters reset, so a second "Untitled"
// card appended into a mission could match the first card's C0001.MP4 by name
// and never be copied — and the card is formatted once the ingest says done.
func TestIngestOnlySkipsFilesThatAreReallyCopied(t *testing.T) {
	root := t.TempDir()
	card := filepath.Join(root, "card")
	mission := filepath.Join(root, "drive", "2026", "001_A")
	pf := func(name string, size int) presentFile {
		rel := filepath.Join("Untitled", name)
		return presentFile{
			src: filepath.Join(card, name), dst: filepath.Join(mission, rel),
			rel: rel, dstRoot: mission, size: int64(size),
		}
	}

	// already copied and recorded
	writeFile(t, filepath.Join(card, "C0001.MP4"), "first")
	writeFile(t, filepath.Join(mission, "Untitled", "C0001.MP4"), "first")
	// already copied, manifest never written (run killed in between)
	writeFile(t, filepath.Join(card, "C0002.MP4"), "second")
	writeFile(t, filepath.Join(mission, "Untitled", "C0002.MP4"), "second")
	// same name, a different card's clip of a different size
	writeFile(t, filepath.Join(card, "C0003.MP4"), "a longer clip")
	writeFile(t, filepath.Join(mission, "Untitled", "C0003.MP4"), "short")
	// same name and size, different content, unrecorded
	writeFile(t, filepath.Join(card, "C0004.MP4"), "AAAA")
	writeFile(t, filepath.Join(mission, "Untitled", "C0004.MP4"), "BBBB")
	// same name and size, recorded under a different hash
	writeFile(t, filepath.Join(card, "C0005.MP4"), "CCCC")
	writeFile(t, filepath.Join(mission, "Untitled", "C0005.MP4"), "CCCC")
	writeFile(t, filepath.Join(mission, "checksums.b3"),
		b3("first")+"  "+filepath.Join("Untitled", "C0001.MP4")+"\n"+
			b3("DDDD")+"  "+filepath.Join("Untitled", "C0005.MP4")+"\n")

	record, conflicts := checkAlreadyCopied([]presentFile{
		pf("C0001.MP4", 5), pf("C0002.MP4", 6), pf("C0003.MP4", 13), pf("C0004.MP4", 4), pf("C0005.MP4", 4),
	})

	joined := strings.Join(conflicts, "\n")
	for _, want := range []string{"C0003.MP4", "C0004.MP4", "C0005.MP4"} {
		if !strings.Contains(joined, want) {
			t.Errorf("%s was not reported as a collision; got:\n%s", want, joined)
		}
	}
	for _, ok := range []string{"C0001.MP4", "C0002.MP4"} {
		if strings.Contains(joined, ok) {
			t.Errorf("%s matches the card but was reported as a collision", ok)
		}
	}
	lines := record[mission]
	if len(lines) != 1 || !strings.HasSuffix(lines[0], "C0002.MP4") || !strings.HasPrefix(lines[0], b3("second")) {
		t.Errorf("only the unrecorded match should be recorded, got %v", lines)
	}
}

// Mission names went into a path as typed, with only spaces replaced: "/"
// nested directories, "../" escaped the year, and exFAT-illegal characters
// failed the copy part-way.
func TestSanitizeMission(t *testing.T) {
	cases := map[string]string{
		"Altissimo with Anton":     "Altissimo_with_Anton",
		"Alps/Day 1":               "Alps_Day_1",
		"../../Escape":             ".._.._Escape",
		`Q: what? "this" <x>|y\z*`: `Q__what___this___x__y_z_`,
		"tab\there":                "tab_here",
		"Trailing dots...":         "Trailing_dots",
		"Zürich Föhn":              "Zürich_Föhn",
		"///":                      "",
		" . ":                      "",
	}
	for in, want := range cases {
		got := sanitizeMission(in)
		if got != want {
			t.Errorf("sanitizeMission(%q) = %q, want %q", in, got, want)
		}
		if strings.ContainsAny(got, `/\:*?"<>|`) {
			t.Errorf("sanitizeMission(%q) = %q still holds a character a drive will not store", in, got)
		}
		if got != "" && filepath.Base(filepath.Join("2026", "045_"+got)) != "045_"+got {
			t.Errorf("sanitizeMission(%q) = %q is not a single path component", in, got)
		}
	}
}

// With input closed or used up, the read error was ignored and the empty
// answer was asked for again, forever.
func TestPromptStopsWhenInputEnds(t *testing.T) {
	old := stdin
	t.Cleanup(func() { stdin = old })

	answer := func(input string) (string, bool, bool, error) {
		stdin = bufio.NewReader(strings.NewReader(input))
		type res struct {
			slug        string
			isNew, skip bool
			err         error
		}
		done := make(chan res, 1)
		go func() {
			slug, isNew, _, skip, err := promptMissionForDay(Config{}, 2026, 45, "2026-10-06", "")
			done <- res{slug, isNew, skip, err}
		}()
		select {
		case r := <-done:
			return r.slug, r.isNew, r.skip, r.err
		case <-time.After(2 * time.Second):
			t.Fatalf("prompt still asking after input %q ended", input)
			return "", false, false, nil
		}
	}

	if _, _, _, err := answer(""); err == nil {
		t.Error("no input: want an error")
	}
	if _, _, _, err := answer("\n\n"); err == nil {
		t.Error("only blank lines: want an error")
	}
	if slug, isNew, _, err := answer("Alps"); err != nil || slug != "045_Alps" || !isNew {
		t.Errorf("a last line without a newline: %q %v %v", slug, isNew, err)
	}
	if _, _, skip, err := answer("-\n"); err != nil || !skip {
		t.Errorf("skip: %v %v", skip, err)
	}

	stdin = bufio.NewReader(strings.NewReader(""))
	if ask("Delete?") {
		t.Error("the end of input answered yes")
	}
	stdin = bufio.NewReader(strings.NewReader("maybe\n"))
	if askYesNo("Delete? ") {
		t.Error("askYesNo answered yes when input ran out after an invalid answer")
	}
}
