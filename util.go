package main

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strings"
)

type fileEntry struct {
	rel  string
	size int64
}

type op struct {
	src, dst string
	do       func() <-chan *result
}

type result struct {
	err     error
	n       int64
	srcHash string
	dst     string
	rel     string
	dstRoot string
}

type scannedCard struct {
	mountedCard
	files []fileEntry
}

type mountedCard struct {
	CardConfig
	src string
}

// junkDirs are directories whose contents should never be ingested or synced.
var junkDirs = map[string]bool{
	"@eaDir": true, // Synology extended attributes
	"@tmp":   true, // Synology temp
}

// junkFiles are filenames that should always be skipped.
var junkFiles = map[string]bool{
	"Thumbs.db":   true,
	"desktop.ini": true,
}

// isJunk reports whether a file or directory name should be treated as junk.
// This covers exact matches (junkDirs, junkFiles) as well as Synology resource
// fork entries which are named <original>@SynoResource.
func isJunk(name string, isDir bool) bool {
	if strings.HasSuffix(name, "@SynoResource") {
		return true
	}
	if isDir {
		return junkDirs[name]
	}
	return junkFiles[name] || strings.HasPrefix(name, "._")
}

func findFiles(root string) ([]fileEntry, error) {
	var files []fileEntry
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d == nil {
			return nil
		}
		name := d.Name()
		if d.IsDir() {
			if isJunk(name, true) {
				return filepath.SkipDir
			}
			return nil
		}
		rel := strings.TrimPrefix(path, root+string(os.PathSeparator))
		for _, part := range strings.Split(rel, string(os.PathSeparator)) {
			if strings.HasPrefix(part, ".") || isJunk(part, true) {
				return nil
			}
		}
		if isJunk(name, false) {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return err
		}
		files = append(files, fileEntry{rel: rel, size: info.Size()})
		return nil
	})
	return files, err
}

// missionFiles returns files for a mission dir, using checksums.b3 as the
// manifest if present (preserves sizes for progress), otherwise walks the dir.
func missionFiles(dir string) ([]fileEntry, int64, error) {
	cPath := filepath.Join(dir, "checksums.b3")
	f, err := os.Open(cPath)
	if err == nil {
		defer f.Close()
		var files []fileEntry
		var total int64
		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			parts := strings.SplitN(scanner.Text(), "  ", 2)
			if len(parts) != 2 {
				continue
			}
			rel := parts[1]
			info, err := os.Stat(filepath.Join(dir, rel))
			if err != nil {
				fmt.Printf("%s %s listed in checksums.b3 but missing on disk\n", yellow("warning:"), rel)
				continue
			}
			files = append(files, fileEntry{rel: rel, size: info.Size()})
			total += info.Size()
		}
		return files, total, scanner.Err()
	}
	// fallback: walk
	files, err := findFiles(dir)
	if err != nil {
		return nil, 0, err
	}
	var total int64
	for _, f := range files {
		total += f.size
	}
	return files, total, nil
}

func missionManifestsMatch(a, b []fileEntry) bool {
	if len(a) != len(b) {
		return false
	}
	sizes := make(map[string]int64, len(a))
	for _, f := range a {
		sizes[f.rel] = f.size
	}
	for _, f := range b {
		if sizes[f.rel] != f.size {
			return false
		}
	}
	return true
}

func findMissionSlug(drives []DriveConfig, yearStr string, num int) (string, error) {
	prefix := fmt.Sprintf("%03d_", num)
	for _, d := range drives {
		yearDir := filepath.Join(d.basePath(), d.Root, yearStr)
		entries, err := os.ReadDir(yearDir)
		if err != nil {
			continue
		}
		for _, e := range entries {
			if e.IsDir() && strings.HasPrefix(e.Name(), prefix) {
				return e.Name(), nil
			}
		}
	}
	return "", fmt.Errorf("no mission %s found on any mounted drive", prefix)
}


func mergeChecksums(path string, newLines []string) []string {
	existing := readChecksumFile(path)
	for _, line := range newLines {
		parts := strings.SplitN(line, "  ", 2)
		if len(parts) == 2 {
			existing[parts[1]] = parts[0]
		}
	}
	merged := make([]string, 0, len(existing))
	for rel, hash := range existing {
		merged = append(merged, fmt.Sprintf("%s  %s", hash, rel))
	}
	return merged
}

func readChecksumFile(path string) map[string]string {
	out := make(map[string]string)
	f, err := os.Open(path)
	if err != nil {
		return out
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "  ", 2)
		if len(parts) == 2 {
			out[parts[1]] = parts[0]
		}
	}
	return out
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

func sanitizeMission(name string) string {
	return strings.ReplaceAll(strings.TrimSpace(name), " ", "_")
}

func expandPath(p string) (string, error) {
	if strings.HasPrefix(p, "~") {
		usr, err := user.Current()
		if err != nil {
			return "", err
		}
		return filepath.Join(usr.HomeDir, p[1:]), nil
	}
	return filepath.Abs(p)
}

func confirm() bool {
	fmt.Print("enter \"y\" to confirm: ")
	var resp string
	fmt.Scan(&resp)
	return resp == "y"
}

func fmtSize(size uint64) string {
	const unit = uint64(1024)
	if size < unit {
		return fmt.Sprintf("%dB", size)
	}
	div, exp := unit, 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f%cB", float64(size)/float64(div), "KMGTPE"[exp])
}

// keepAwake runs caffeinate -m in the background to prevent drive sleep.
// It is killed automatically when the process exits.
func keepAwake() {
	cmd := exec.Command("caffeinate", "-mi")
	_ = cmd.Start()
}

func exit(code int, msg string, args ...any) {
	fmt.Printf(msg+"\n", args...)
	os.Exit(code)
}
