package main

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
)

const ewmaWindow = 250 * time.Millisecond

// barTracker aggregates increments from concurrent goroutines and feeds the
// EWMA with a single coherent elapsed-time stream. Each bar gets one tracker;
// all goroutines writing to that bar share it via incr().
type barTracker struct {
	bar     *mpb.Bar
	mu      sync.Mutex
	pending int64
	last    time.Time
}

func (t *barTracker) incr(n int) {
	if n <= 0 {
		return
	}
	t.mu.Lock()
	t.pending += int64(n)
	now := time.Now()
	if t.last.IsZero() {
		t.last = now
		t.mu.Unlock()
		return
	}
	if elapsed := now.Sub(t.last); elapsed >= ewmaWindow {
		t.bar.EwmaIncrBy(int(t.pending), elapsed)
		t.pending = 0
		t.last = now
	}
	t.mu.Unlock()
}

// flush sends any remaining pending bytes after all goroutines have finished.
func (t *barTracker) flush() {
	t.mu.Lock()
	if t.pending > 0 && !t.last.IsZero() {
		t.bar.EwmaIncrBy(int(t.pending), time.Since(t.last))
		t.pending = 0
	}
	t.mu.Unlock()
}

type progressWriter struct {
	w       io.Writer
	tracker *barTracker
}

func (pw *progressWriter) Write(p []byte) (int, error) {
	n, err := pw.w.Write(p)
	if pw.tracker != nil {
		pw.tracker.incr(n)
	}
	return n, err
}

type progressReader struct {
	r       io.Reader
	tracker *barTracker
}

func (pr *progressReader) Read(p []byte) (int, error) {
	n, err := pr.r.Read(p)
	if pr.tracker != nil {
		pr.tracker.incr(n)
	}
	return n, err
}

// addBarDynamic is like addBar but prepends a dynamic label (e.g. mission counter)
// returned by the label func, read on every render tick.
func addBarDynamic(p *mpb.Progress, name string, total int64, label func() string) *barTracker {
	style := mpb.BarStyle().
		Lbound("").
		Filler("█").
		Tip("▌").
		Padding("░").
		Rbound("")
	bar := p.New(total,
		style,
		mpb.PrependDecorators(
			decor.Any(func(_ decor.Statistics) string {
				return fmt.Sprintf("%-12s %-10s  ", name, label())
			}),
			decor.CountersKibiByte("% .1f / % .1f  "),
		),
		mpb.AppendDecorators(
			decor.EwmaSpeed(decor.SizeB1024(0), "% .1f  ", 30),
			decor.OnComplete(decor.EwmaETA(decor.ET_STYLE_GO, 150), "✓"),
		),
	)
	return &barTracker{bar: bar}
}

func addBar(p *mpb.Progress, name string, total int64) *barTracker {
	style := mpb.BarStyle().
		Lbound("").
		Filler("█").
		Tip("▌").
		Padding("░").
		Rbound("")
	bar := p.New(total,
		style,
		mpb.PrependDecorators(
			decor.Name(fmt.Sprintf("%-12s", name)),
			decor.CountersKibiByte("% .1f / % .1f  "),
		),
		mpb.AppendDecorators(
			decor.EwmaSpeed(decor.SizeB1024(0), "% .1f  ", 30),
			decor.OnComplete(decor.EwmaETA(decor.ET_STYLE_GO, 150), "✓"),
		),
	)
	return &barTracker{bar: bar}
}
