package main

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/vbauerster/mpb/v8"
)

// waitsWithin runs fn and fails if it has not returned within a second. Every
// case here deadlocks rather than misbehaving, so a timeout is the assertion.
func waitsWithin(t *testing.T, name string, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Errorf("%s: Progress.Wait did not return", name)
	}
}

// mpb reads a total of zero as "total unknown" and never completes such a bar,
// so Progress.Wait blocks on it forever. A bar with nothing to move is routine:
// -ingest gives every destination one and then finds a drive already holds the
// day's files, and -verify sizes a bar from files its manifest lists but the
// drive no longer has.
func TestZeroTotalBarDoesNotBlockWait(t *testing.T) {
	cases := []struct {
		name string
		run  func(*mpb.Progress)
	}{
		{"one destination already up to date", func(p *mpb.Progress) {
			up, work := addBar(p, "T9", 0), addBar(p, "T7", 100)
			work.incr(100)
			up.flush()
			work.flush()
		}},
		{"every destination already up to date", func(p *mpb.Progress) {
			a, b := addBar(p, "T9", 0), addBar(p, "T7", 0)
			a.flush()
			b.flush()
		}},
		{"manifest lists nothing left on disk", func(p *mpb.Progress) {
			addBar(p, "T9", 0).flush()
		}},
		{"dynamic bar with no work", func(p *mpb.Progress) {
			addBarDynamic(p, "proxy", 0, func() string { return "[0/0]" }).finish()
		}},
		{"bar that does fill", func(p *mpb.Progress) {
			bar := addBar(p, "T9", 100)
			bar.incr(100)
			bar.flush()
		}},
	}
	for _, c := range cases {
		waitsWithin(t, c.name, func() {
			p := mpb.NewWithContext(context.Background(), mpb.WithOutput(io.Discard))
			c.run(p)
			p.Wait()
		})
	}
}

// A byte-exact bar that lands short means a file did not make it: a copy that
// errored part-way, a hash that could not be read. mpb fires the complete event
// only when current reaches total, so Progress.Wait blocked on such a bar
// forever — and every phase in the tree reports its failures on the line *after*
// the Wait, so the ERROR line printed and then the run sat there with a stalled
// bar. Reproduced against -verify, -checksum and -copy on real directories with
// one unreadable file: each printed its error and never returned.
//
// stop() must not lie in either direction: a bar that filled has to come out
// complete, and one that did not has to come out aborted rather than topped up
// to a total it never reached.
func TestBarLeftShortDoesNotBlockWait(t *testing.T) {
	cases := []struct {
		name        string
		run         func(*mpb.Progress) *barTracker
		wantAborted bool
	}{
		{"copy failed part-way through a file", func(p *mpb.Progress) *barTracker {
			bar := addBar(p, "T9", 100)
			bar.incr(40)
			return bar
		}, true},
		{"file could not be opened at all", func(p *mpb.Progress) *barTracker {
			return addBar(p, "T9", 100)
		}, true},
		{"every byte arrived", func(p *mpb.Progress) *barTracker {
			bar := addBar(p, "T9", 100)
			bar.incr(100)
			return bar
		}, false},
		{"nothing to move", func(p *mpb.Progress) *barTracker {
			return addBar(p, "T9", 0)
		}, false},
	}
	for _, c := range cases {
		var bar *barTracker
		waitsWithin(t, c.name, func() {
			p := mpb.NewWithContext(context.Background(), mpb.WithOutput(io.Discard))
			bar = c.run(p)
			bar.stop()
			p.Wait()
		})
		if bar == nil {
			continue
		}
		if got := bar.bar.Aborted(); got != c.wantAborted {
			t.Errorf("%s: bar aborted = %v, want %v", c.name, got, c.wantAborted)
		}
	}
}

// Two bars on one container, one of which never fills: the whole container has
// to come back, not just the good bar. This is the shape of every multi-drive
// phase — one archive reads clean while the other has a bad sector.
func TestOneShortBarDoesNotStrandTheOthers(t *testing.T) {
	waitsWithin(t, "one drive of two could not be read", func() {
		p := mpb.NewWithContext(context.Background(), mpb.WithOutput(io.Discard))
		good, bad := addBar(p, "T9", 100), addBar(p, "T7", 100)
		good.incr(100)
		good.stop()
		bad.stop()
		p.Wait()
	})
}

// An error raised while a bar is drawing must reach the screen whole. A plain
// Printf landed on the bar's own line and the next redraw painted over its end.
// After the bars are done the line still has to come out, on stdout.
func TestBarPrintfKeepsTheWholeLine(t *testing.T) {
	orig := stdoutIsTerminal
	stdoutIsTerminal = func() bool { return true }
	defer func() { stdoutIsTerminal = orig }()

	var buf bytes.Buffer
	p := mpb.New(mpb.WithOutput(&buf), mpb.WithAutoRefresh(), mpb.WithWidth(64))
	bar := addBar(p, "proxy", 10)
	line := "ERROR Untitled/923_0544.MXF: lut: rename a.cube.tmp a.cube: no such file or directory"
	barPrintf(p, "%s\n", line)
	bar.bar.IncrBy(10)
	waitsWithin(t, "wait", p.Wait)
	if !strings.Contains(buf.String(), line+"\n") {
		t.Errorf("line did not survive the redraw:\n%q", buf.String())
	}

	out := captureStdout(t, func() { barPrintf(p, "%s\n", "after") })
	if out != "after\n" {
		t.Errorf("after Wait: got %q on stdout", out)
	}
}

// Off a terminal mpb draws nothing and never flushes what was written to it, so
// a line sent through it was lost — an error missing from a piped log.
func TestBarPrintfOffATerminalGoesToStdout(t *testing.T) {
	orig := stdoutIsTerminal
	stdoutIsTerminal = func() bool { return false }
	defer func() { stdoutIsTerminal = orig }()

	p := mpb.New(mpb.WithOutput(io.Discard))
	bar := addBar(p, "proxy", 10)
	out := captureStdout(t, func() { barPrintf(p, "%s\n", "ERROR x") })
	bar.bar.IncrBy(10)
	waitsWithin(t, "wait", p.Wait)
	if out != "ERROR x\n" {
		t.Errorf("got %q on stdout", out)
	}
}
