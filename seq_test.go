package main

import (
	"os"
	"path/filepath"
	"testing"
)

func seqFor(t *testing.T, year int) int {
	t.Helper()
	seq, err := readSeq()
	if err != nil {
		t.Fatal(err)
	}
	return seq[year]
}

// A copy that failed or was interrupted gave its number back by decrementing
// the counter, whatever it held. A multi-day ingest had already committed every
// day's number up front, so abandoning the first day left the rest minted for
// missions that never existed, and the decrement could not tell a kept partial
// copy from one that was deleted. releaseMission gives back exactly the number
// that failed, and only when nothing on the drives still carries it.
func TestReleaseMission(t *testing.T) {
	drive := t.TempDir()
	cfg := Config{Drives: []DriveConfig{{Volume: "T9", Path: drive, Role: "hot"}}}

	t.Run("gives back a number that never landed", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		writeSeq(map[int]int{2026: 12})
		if ok, err := releaseMission(cfg.Drives, 2026, 12); err != nil || !ok {
			t.Fatalf("released = %v, %v; want true", ok, err)
		}
		if got := seqFor(t, 2026); got != 11 {
			t.Errorf("seq = %d, want 11", got)
		}
	})

	t.Run("leaves a later number alone", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		writeSeq(map[int]int{2026: 13})
		releaseMission(cfg.Drives, 2026, 12)
		if got := seqFor(t, 2026); got != 13 {
			t.Errorf("seq = %d, want 13", got)
		}
	})

	t.Run("keeps the number of a partial copy that was kept", func(t *testing.T) {
		t.Setenv("HOME", t.TempDir())
		writeSeq(map[int]int{2026: 12})
		if err := os.MkdirAll(filepath.Join(drive, "2026", "012_Partial"), 0o755); err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(filepath.Join(drive, "2026", "012_Partial"))
		releaseMission(cfg.Drives, 2026, 12)
		if got := seqFor(t, 2026); got != 12 {
			t.Errorf("seq = %d, want 12", got)
		}
	})
}

func TestCheckMissionCounter(t *testing.T) {
	drive := t.TempDir()
	if err := os.MkdirAll(filepath.Join(drive, "2026", "010_Latest"), 0o755); err != nil {
		t.Fatal(err)
	}
	mounted := Config{Drives: []DriveConfig{{Volume: "T9", Path: drive, Role: "hot"}}}
	withArchiveAway := Config{Drives: []DriveConfig{
		{Volume: "T9", Path: drive, Role: "hot"},
		{Volume: "ARCHIVE", Path: filepath.Join(t.TempDir(), "absent"), Role: "cold"},
	}}

	cases := []struct {
		name    string
		cfg     Config
		counter int
		want    int
	}{
		// would mint a duplicate: raised even under -y
		{"behind the drives is raised", mounted, 7, 10},
		// rewinding needs a typed yes, so -y only warns
		{"ahead is not rewound under -y", mounted, 12, 12},
		{"ahead with a drive away is left alone", withArchiveAway, 12, 12},
		{"in step is untouched", mounted, 10, 10},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("HOME", t.TempDir())
			writeSeq(map[int]int{2026: c.counter})
			checkMissionCounter(c.cfg, 2026, true)
			if got := seqFor(t, 2026); got != c.want {
				t.Errorf("seq = %d, want %d", got, c.want)
			}
		})
	}

	st, err := func() (counterState, error) {
		t.Setenv("HOME", t.TempDir())
		writeSeq(map[int]int{2026: 12})
		return readCounterState(withArchiveAway, 2026)
	}()
	if err != nil {
		t.Fatal(err)
	}
	if st.counter != 12 || st.onDrives != 10 || st.allMounted {
		t.Errorf("state = %+v, want counter 12, onDrives 10, not all mounted", st)
	}
}
