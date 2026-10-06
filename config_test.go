package main

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestValidateConfig(t *testing.T) {
	bad := Config{
		Cards: []CardConfig{{Sub: ""}, {Sub: "/abs"}, {Sub: "DCIM/../.."}},
		Drives: []DriveConfig{
			{Volume: "T9", Role: "Cold"},
			{Volume: "t9", Role: "hot"},
			{Path: "/Volumes/T9", Role: "cold"},
			{Role: "hot"},
			{Volume: "OLD", Role: "cold", YearFrom: 2025, YearTo: 2020},
			{Volume: "ODD", Role: "cold", YearFrom: 25, Root: "../x"},
		},
	}
	got := strings.Join(validateConfig(bad), "\n")
	for _, want := range []string{
		`role is "Cold"`,
		"same name as drive 1",
		"same footage folder as drive 1",
		"needs a volume or a path",
		"year_from 2025 is after year_to 2020",
		"year_from 25 is not a year",
		`root "../x"`,
		"card 1: needs a sub folder",
		"card 2: sub",
		"card 3: sub",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("missing %q in:\n%s", want, got)
		}
	}

	good := Config{
		Cards: []CardConfig{{Sub: "XDROOT/Clip"}, {Sub: "DCIM/100GOPRO"}},
		Drives: []DriveConfig{
			{Volume: "T9", Root: "", Role: "hot"},
			{Volume: "T7", Root: "Footage", Role: "hot"},
			{Volume: "ARCHIVE_01", Root: "Footage", Role: "cold", YearFrom: 2014, YearTo: 2026},
		},
	}
	if p := validateConfig(good); len(p) != 0 {
		t.Errorf("a good config was rejected: %v", p)
	}
}

// The real config on this machine, when there is one, must pass.
func TestValidateTheInstalledConfig(t *testing.T) {
	p, err := expandPath("~/.qcp")
	if err != nil {
		t.Skip()
	}
	data, err := os.ReadFile(p)
	if err != nil {
		t.Skip("no ~/.qcp")
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		t.Fatal(err)
	}
	if problems := validateConfig(cfg); len(problems) != 0 {
		t.Errorf("~/.qcp: %v", problems)
	}
}
