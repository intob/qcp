package main

import (
	"fmt"
	"sync"
)

type pool struct {
	sem chan struct{}
	wg  sync.WaitGroup
}

func newPool(n int) *pool {
	return &pool{sem: make(chan struct{}, n)}
}

// run submits fn, blocking until a concurrency slot is free.
func (p *pool) run(fn func()) {
	p.sem <- struct{}{}
	p.wg.Add(1)
	go func() {
		defer func() {
			<-p.sem
			p.wg.Done()
		}()
		fn()
	}()
}

func (p *pool) wait() {
	p.wg.Wait()
}

// sourceLimiter bounds how many files are read concurrently from each source
// drive. Copy pools are sized by the destination, so without this a single
// archive HDD would serve one read stream per destination worker — and one per
// worker per destination when several are mounted, which costs far more in
// seeks than the parallelism gains.
type sourceLimiter struct {
	info  map[string]driveInfo
	sem   map[string]chan struct{}
	order []string
}

func newSourceLimiter() *sourceLimiter {
	return &sourceLimiter{
		info: make(map[string]driveInfo),
		sem:  make(map[string]chan struct{}),
	}
}

// add probes vol once and registers its read limit. base is the drive's mount
// path, as passed to probeDrive.
func (l *sourceLimiter) add(vol, base string) {
	if _, seen := l.info[vol]; seen {
		return
	}
	info := probeDrive(base)
	l.info[vol] = info
	l.sem[vol] = make(chan struct{}, info.concurrency)
	l.order = append(l.order, vol)
}

// acquire takes a read slot on vol, blocking until one is free, and returns the
// release func. Volumes that were never added are unlimited.
func (l *sourceLimiter) acquire(vol string) func() {
	sem := l.sem[vol]
	if sem == nil {
		return func() {}
	}
	sem <- struct{}{}
	return func() { <-sem }
}

// report prints one line per source drive, in the order they were added.
func (l *sourceLimiter) report() {
	for _, vol := range l.order {
		fmt.Printf("  %s %s: %s\n", dim("from"), bold(vol), l.info[vol])
	}
}

// submitAll runs each submitter in its own goroutine and waits for them all.
// pool.run blocks its caller while the pool is full, so submitting several
// destinations from one goroutine would make the first drain before the next
// began.
func submitAll(submitters []func()) {
	var wg sync.WaitGroup
	for _, s := range submitters {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s()
		}()
	}
	wg.Wait()
}
