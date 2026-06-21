package main

import (
	"fmt"
	"os/exec"
	"path/filepath"
	"strings"
)

func runEject(cfg Config) {
	var targets []struct{ name, path string }

	for _, c := range mountedCards(cfg) {
		p := filepath.Join("/Volumes", c.Volume)
		targets = append(targets, struct{ name, path string }{c.Volume, p})
	}
	for _, d := range cfg.Drives {
		base := d.basePath()
		if strings.HasPrefix(base, "/Volumes/") && dirExists(base) {
			targets = append(targets, struct{ name, path string }{d.name(), base})
		}
	}

	if len(targets) == 0 {
		fmt.Println("no mounted cards or drives to eject")
		return
	}

	var failed int
	for _, t := range targets {
		out, err := exec.Command("diskutil", "eject", t.path).CombinedOutput()
		msg := strings.TrimSpace(string(out))
		if err != nil {
			fmt.Printf("%s %s: %s\n", red("✗"), bold(t.name), msg)
			failed++
		} else {
			fmt.Printf("%s %s\n", green("✓"), bold(t.name))
		}
	}
	if failed > 0 {
		exit(1, "%d volume(s) failed to eject", failed)
	}
}
