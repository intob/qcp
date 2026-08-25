package main

import (
	"context"
	"io"
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
