package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
)

const seqPath = "~/.qcp_seq"

func readSeq() (map[int]int, error) {
	p, err := expandPath(seqPath)
	if err != nil {
		return nil, err
	}
	data, err := os.ReadFile(p)
	if os.IsNotExist(err) {
		return map[int]int{}, nil
	}
	if err != nil {
		return nil, err
	}
	var raw map[string]int
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, fmt.Errorf("corrupt %s: %w", p, err)
	}
	seq := make(map[int]int, len(raw))
	for k, v := range raw {
		year, err := strconv.Atoi(k)
		if err != nil {
			return nil, fmt.Errorf("corrupt year key %q in %s", k, p)
		}
		seq[year] = v
	}
	return seq, nil
}

func writeSeq(seq map[int]int) error {
	p, err := expandPath(seqPath)
	if err != nil {
		return err
	}
	raw := make(map[string]int, len(seq))
	for k, v := range seq {
		raw[strconv.Itoa(k)] = v
	}
	data, err := json.MarshalIndent(raw, "", "  ")
	if err != nil {
		return err
	}
	// The counter is the one record of which numbers are spent, so it is
	// never left half-written either.
	return writeFileAtomic(p, data, 0644)
}

func peekMission(year int) (int, error) {
	seq, err := readSeq()
	if err != nil {
		return 0, err
	}
	return seq[year] + 1, nil
}

func commitMission(year, num int) error {
	seq, err := readSeq()
	if err != nil {
		return err
	}
	seq[year] = num
	return writeSeq(seq)
}

// releaseMission gives back a number minted for a mission that did not land.
// It only moves the counter if num is still the last number handed out, so it
// can never undo a later mission's number however it is called, and only if no
// mounted drive holds a mission with that number — a partial copy that was kept
// is a mission, and the counter must keep covering it.
func releaseMission(drives []DriveConfig, year, num int) (bool, error) {
	seq, err := readSeq()
	if err != nil {
		return false, err
	}
	if seq[year] != num {
		return false, nil
	}
	if _, err := findMissionSlug(drives, strconv.Itoa(year), num); err == nil {
		return false, nil
	}
	seq[year] = num - 1
	return true, writeSeq(seq)
}

// counterState is the mission counter for a year beside what the drives show.
type counterState struct {
	counter    int  // last number handed out, from ~/.qcp_seq
	mounted    int  // highest mission number on any mounted drive
	onDrives   int  // highest on any drive
	allMounted bool // every drive that can hold the year is mounted
}

func readCounterState(cfg Config, year int) (counterState, error) {
	seq, err := readSeq()
	if err != nil {
		return counterState{}, err
	}
	st := counterState{counter: seq[year], allMounted: true}
	maxByYear := make(map[int]int)
	for _, d := range cfg.Drives {
		if !d.coversYear(year) {
			continue
		}
		base := d.basePath()
		if dirExists(base) {
			scanYearDir(filepath.Join(base, d.Root, strconv.Itoa(year)), year, maxByYear)
			continue
		}
		st.allMounted = false
	}
	st.mounted = maxByYear[year]
	st.onDrives = st.mounted
	return st, nil
}

// checkMissionCounter warns when the counter and the drives disagree before an
// ingest mints a number, and offers to bring the counter into line.
//
// A counter behind the drives would hand out a number that already names a
// mission, so raising it is offered every time and done unasked under -y.
//
// A counter ahead of the drives is only provably wrong when every drive that
// can hold the year is mounted — otherwise the missing numbers may simply be
// on the archive, or evicted off the hot drives — so only then is moving it
// back offered, and only on a yes typed at the prompt.
func checkMissionCounter(cfg Config, year int, skipConf bool) {
	st, err := readCounterState(cfg, year)
	if err != nil {
		fmt.Printf("%s reading mission counter: %v\n", yellow("warning:"), err)
		return
	}
	set := func(n int) {
		seq, err := readSeq()
		if err == nil {
			seq[year] = n
			err = writeSeq(seq)
		}
		if err != nil {
			fmt.Printf("  %s writing mission counter: %v\n\n", red("ERROR"), err)
			return
		}
		fmt.Printf("  %s mission counter for %d set to %03d\n\n", green("✓"), year, n)
	}
	switch {
	case st.onDrives > st.counter:
		fmt.Printf("\n  %s\n", yellow(fmt.Sprintf("⚠  Mission counter for %d is %03d, but the drives hold %03d", year, st.counter, st.onDrives)))
		fmt.Printf("     %s\n\n", dim("a new mission would reuse a number that is already taken"))
		if skipConf || ask(fmt.Sprintf("Raise the counter to %03d?", st.onDrives)) {
			set(st.onDrives)
		} else {
			fmt.Println()
		}
	case st.onDrives < st.counter && st.allMounted:
		fmt.Printf("\n  %s\n", yellow(fmt.Sprintf("⚠  Mission counter for %d is %03d, but the highest mission on the drives is %03d", year, st.counter, st.onDrives)))
		fmt.Printf("     %s\n\n", dim("every drive for the year is mounted — likely left behind by a failed or abandoned copy"))
		if skipConf {
			fmt.Printf("     %s\n\n", dim(fmt.Sprintf("left as is under -y; run qcp -init -year %d to move it back", year)))
			return
		}
		if ask(fmt.Sprintf("Move the counter back to %03d?", st.onDrives)) {
			set(st.onDrives)
		} else {
			fmt.Println()
		}
	}
}
