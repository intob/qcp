package main

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
)

type driveInfo struct {
	concurrency int
	kind        string // "SSD" or "HDD"
	protocol    string // e.g. "USB", "Thunderbolt", "NVMe", "SATA"
}

func (d driveInfo) String() string {
	return fmt.Sprintf("%s · %s · %d worker(s)", d.kind, d.protocol, d.concurrency)
}

// probeDrive queries diskutil for drive type and protocol, then picks
// an appropriate concurrency (SSDs benefit from queue depth; HDDs need sequential I/O).
func probeDrive(volPath string) driveInfo {
	out, err := exec.Command("diskutil", "info", volPath).Output()
	if err != nil {
		return driveInfo{concurrency: 1, kind: "HDD", protocol: "unknown"}
	}
	s := string(out)

	kind := "HDD"
	concurrency := 1
	if strings.Contains(s, "Solid State:               Yes") {
		kind = "SSD"
		concurrency = 4
	}

	protocol := "unknown"
	for _, line := range strings.Split(s, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "Protocol:") {
			protocol = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
			break
		}
	}

	return driveInfo{concurrency: concurrency, kind: kind, protocol: protocol}
}

func mountedCards(cfgs []CardConfig) []mountedCard {
	volumes, _ := os.ReadDir("/Volumes")
	var out []mountedCard
	for _, c := range cfgs {
		for _, vol := range volumes {
			if !strings.HasPrefix(vol.Name(), c.Volume) {
				continue
			}
			src := filepath.Join("/Volumes", vol.Name(), c.Sub)
			if dirExists(src) {
				out = append(out, mountedCard{CardConfig{Volume: vol.Name(), Sub: c.Sub}, src})
			}
		}
	}
	return out
}

func availableBytes(path string) uint64 {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0
	}
	return stat.Bavail * uint64(stat.Bsize)
}

func driveSpaceBar(used, total uint64, width int) string {
	if total == 0 {
		return strings.Repeat("░", width)
	}
	filled := int(float64(used) / float64(total) * float64(width))
	if filled > width {
		filled = width
	}
	return strings.Repeat("█", filled) + strings.Repeat("░", width-filled)
}
