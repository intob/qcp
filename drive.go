package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
)

type driveInfo struct {
	concurrency int
	kind        string // "SSD" or "HDD"
	protocol    string // e.g. "USB", "NVMe", "SATA"
}

func (d driveInfo) String() string {
	return fmt.Sprintf("%s · %s · %d worker(s)", d.kind, d.protocol, d.concurrency)
}

func probeDrive(volPath string) driveInfo {
	out, err := exec.Command("diskutil", "info", volPath).Output()
	if err != nil {
		return driveInfo{concurrency: 1, kind: "HDD", protocol: "unknown"}
	}
	s := string(out)

	kind := "HDD"
	if strings.Contains(s, "Solid State:               Yes") {
		kind = "SSD"
	}

	protocol := "unknown"
	bsdName := ""
	for _, line := range strings.Split(s, "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Protocol:") {
			protocol = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
		}
		if strings.HasPrefix(line, "Device Identifier:") {
			bsdName = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
		}
	}

	return driveInfo{
		concurrency: pickConcurrency(kind, protocol, bsdName),
		kind:        kind,
		protocol:    protocol,
	}
}

func pickConcurrency(kind, protocol, bsdName string) int {
	if kind == "HDD" {
		return 1
	}
	switch protocol {
	case "NVMe":
		return 8
	case "USB":
		switch usbDeviceSpeed(bsdName) {
		case "super_speed_plus":
			return 8 // 10 Gbps+
		case "super_speed":
			return 4 // 5 Gbps
		default:
			return 4
		}
	default:
		return 4
	}
}

// usbProfile caches the system_profiler output — it's slow and shared across drives.
var (
	usbProfileOnce sync.Once
	usbProfileRoot map[string]interface{}
)

var partitionSuffix = regexp.MustCompile(`s\d+$`)

func usbDeviceSpeed(bsdName string) string {
	if bsdName == "" {
		return ""
	}
	// resolve the stable media name from the base disk (BSD numbers can be stale in system_profiler)
	baseDisk := partitionSuffix.ReplaceAllString(bsdName, "")
	mediaName := diskMediaName(baseDisk)
	if mediaName == "" {
		return ""
	}

	usbProfileOnce.Do(func() {
		out, err := exec.Command("system_profiler", "SPUSBDataType", "-json").Output()
		if err != nil {
			return
		}
		var root map[string]interface{}
		if err := json.Unmarshal(out, &root); err != nil {
			return
		}
		usbProfileRoot = root
	})

	if usbProfileRoot == nil {
		return ""
	}

	buses, _ := usbProfileRoot["SPUSBDataType"].([]interface{})
	for _, bus := range buses {
		if b, ok := bus.(map[string]interface{}); ok {
			if speed := usbSpeedFromTree(b, mediaName); speed != "" {
				return speed
			}
		}
	}
	return ""
}

// diskMediaName returns the "Device / Media Name" for a base disk identifier
// (e.g. "disk5" → "PSSD T9"). This is stable across remounts unlike BSD numbers.
func diskMediaName(baseDisk string) string {
	out, err := exec.Command("diskutil", "info", baseDisk).Output()
	if err != nil {
		return ""
	}
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Device / Media Name:") {
			return strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
		}
	}
	return ""
}

// usbSpeedFromTree walks the USB device tree looking for the device entry
// with a matching _name and a device_speed field.
func usbSpeedFromTree(node map[string]interface{}, mediaName string) string {
	items, _ := node["_items"].([]interface{})
	for _, item := range items {
		dev, ok := item.(map[string]interface{})
		if !ok {
			continue
		}
		if speed, ok := dev["device_speed"].(string); ok {
			if name, _ := dev["_name"].(string); name == mediaName {
				return speed
			}
		}
		if speed := usbSpeedFromTree(dev, mediaName); speed != "" {
			return speed
		}
	}
	return ""
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
