package main

import (
	"sync"
	"testing"
)

// The SIGINT handler reads the interrupt target from its own goroutine while
// the main goroutine sets it before each day and clears it after. Without the
// mutex this is a data race, and the handler can pair one mission's roots with
// another's isNew — the deletion prompt would then revert a counter it had no
// business touching.
func TestInterruptTargetIsRaceFree(t *testing.T) {
	var intr interruptTarget

	// isNew is true exactly for the missions whose roots start with "new",
	// so a torn read shows up as a mismatched pair.
	missions := []struct {
		roots []string
		isNew bool
	}{
		{[]string{"new/2026/001_A", "new/2026/001_A_backup"}, true},
		{[]string{"append/2026/002_B"}, false},
		{[]string{"new/2026/003_C"}, true},
	}

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			m := missions[i%len(missions)]
			intr.set(m.roots, m.isNew)
			intr.clear()
		}
	}()

	go func() {
		defer wg.Done()
		for i := 0; i < 2000; i++ {
			roots, isNew := intr.get()
			if len(roots) == 0 {
				continue
			}
			if want := roots[0][:3] == "new"; isNew != want {
				t.Errorf("torn read: roots %v with isNew=%v", roots, isNew)
				return
			}
		}
	}()

	wg.Wait()

	if roots, isNew := intr.get(); roots != nil || isNew {
		t.Errorf("after clear: got %v, %v; want nil, false", roots, isNew)
	}
}
