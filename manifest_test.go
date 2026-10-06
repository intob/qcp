package main

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
)

// checksums.b3 is the only record of what the footage hashed to when it was
// known good. It was written with os.WriteFile, which truncates the file before
// writing it, so a crash or an unplugged drive mid-write left a manifest cut
// short — and a reader racing the write saw the same thing. Every write now goes
// through a temporary and a rename.
func TestChecksumsAreNeverSeenHalfWritten(t *testing.T) {
	path := filepath.Join(t.TempDir(), "checksums.b3")
	var lines []string
	for i := 0; i < 2000; i++ {
		lines = append(lines, fmt.Sprintf("%064x  clip_%04d.mp4", i, i))
	}
	if err := writeChecksums(path, append([]string(nil), lines...)); err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	done := make(chan struct{})
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(done)
		for i := 0; i < 50; i++ {
			if err := writeChecksums(path, append([]string(nil), lines...)); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	for {
		select {
		case <-done:
			wg.Wait()
			return
		default:
		}
		if n := len(readChecksumFile(path)); n != len(lines) {
			t.Fatalf("reader saw %d entries, want %d", n, len(lines))
		}
	}
}
