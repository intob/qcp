package main

import (
	"encoding/json"
	"fmt"
	"os"
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
	return os.WriteFile(p, data, 0644)
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

func revertMission(year int) error {
	seq, err := readSeq()
	if err != nil {
		return err
	}
	if seq[year] > 0 {
		seq[year]--
	}
	return writeSeq(seq)
}
