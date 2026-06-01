package main

import "sync"

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
