package main

import (
	"bufio"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type fileEntry struct {
	rel  string
	size int64
}

type op struct {
	src, dst string
	srcVol   string
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

func (sc scannedCard) totalSize() int64 {
	var n int64
	for _, f := range sc.files {
		n += f.size
	}
	return n
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

// metadataFiles are qcp's own bookkeeping files. They describe the directory
// they sit in, so anything that moves files between directories must leave
// them behind rather than carry them along as if they were footage.
var metadataFiles = map[string]bool{
	"checksums.b3":    true,
	proxyManifestName: true,
	proxyMetaName:     true,
	flagsFileName:     true,
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

// contentFiles lists a mission's content: everything findFiles sees, less qcp's
// own bookkeeping at the top level.
//
// This is the answer to "what is this mission", and every command that compares
// or transfers one has to use the same answer. A manifest describes the
// directory it sits in, so it is not part of the mission it describes: -sync
// and -replicate never carry one across, and -check must not therefore read a
// cold copy that has not been checksummed yet as a copy that is missing a file
// — it used to, and then told the user to run -sync, which would never copy it.
//
// Only top-level entries are excluded, matching what the transfers skip.
// .qcp-flags.json is in metadataFiles for completeness; findFiles already drops
// it with every other dotfile.
func contentFiles(dir string) ([]fileEntry, error) {
	all, err := findFiles(dir)
	if err != nil {
		return nil, err
	}
	files := make([]fileEntry, 0, len(all))
	for _, f := range all {
		if metadataFiles[f.rel] {
			continue
		}
		files = append(files, f)
	}
	return files, nil
}

// missionFiles returns the mission's content as it is on disk — contentFiles,
// so without checksums.b3 or the other bookkeeping. The third return value is the number of files listed in
// checksums.b3 but absent from disk; callers should treat a non-zero value as
// an error.
//
// The listing must come from the disk, not from checksums.b3. Reading the
// manifest instead makes every caller blind to files it does not mention: a
// card ingested into an existing mission after the manifest was written would
// be invisible to -checksum, which would then rewrite the manifest without it,
// and to -sync, which would never copy it to a cold drive.
func missionFiles(dir string) ([]fileEntry, int64, int, error) {
	files, err := contentFiles(dir)
	if err != nil {
		return nil, 0, 0, err
	}
	onDisk := make(map[string]bool, len(files))
	var total int64
	for _, f := range files {
		onDisk[f.rel] = true
		total += f.size
	}

	var ghosts int
	manifest, err := readChecksums(filepath.Join(dir, "checksums.b3"))
	if err != nil {
		return nil, 0, 0, err
	}
	rels := make([]string, 0, len(manifest))
	for rel := range manifest {
		rels = append(rels, rel)
	}
	sort.Strings(rels)
	for _, rel := range rels {
		if rel == "checksums.b3" || onDisk[rel] {
			continue
		}
		fmt.Printf("%s %s listed in checksums.b3 but missing on disk\n", red("ERROR:"), rel)
		ghosts++
	}
	return files, total, ghosts, nil
}

// sizeConflict is a file that is on both sides of a copy under the same name
// but not at the same size.
type sizeConflict struct {
	rel           string
	want, present int64
}

func (c sizeConflict) String() string {
	return fmt.Sprintf("%s is %d bytes here but %d at the source", c.rel, c.present, c.want)
}

// planCopy splits a source's files into those the destination is missing and
// those it holds at a different size.
//
// Every transfer used to decide "already there" by name alone, so a truncated
// file, a different file under the same name, or a short copy left by a tool
// other than qcp was taken for done and never copied or reported. A size
// mismatch is never overwritten here: the destination may be the copy that is
// right, so it is reported for -check and -verify to settle.
func planCopy(src []fileEntry, dst []fileEntry) (missing []fileEntry, conflicts []sizeConflict) {
	have := make(map[string]int64, len(dst))
	for _, f := range dst {
		have[f.rel] = f.size
	}
	for _, f := range src {
		size, ok := have[f.rel]
		switch {
		case !ok:
			missing = append(missing, f)
		case size != f.size:
			conflicts = append(conflicts, sizeConflict{f.rel, f.size, size})
		}
	}
	return missing, conflicts
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

// mergeChecksums returns the checksums.b3 at path with newLines merged in. A
// manifest that exists but cannot be read is an error, not an empty one:
// merging into what could be read and writing that back would drop every entry
// past the failure.
func mergeChecksums(path string, newLines []string) ([]string, error) {
	existing, err := readChecksums(path)
	if err != nil {
		return nil, err
	}
	for _, line := range newLines {
		parts := strings.SplitN(line, "  ", 2)
		if len(parts) == 2 {
			existing[parts[1]] = parts[0]
		}
	}
	// A manifest cannot describe itself. checksums.b3 is copied like any other
	// file, so a transfer hashes it, records that hash, and then overwrites the
	// file with this merge — leaving an entry that can never match. Readers
	// ignore such an entry too, so manifests written before this are harmless;
	// this only stops new ones being created, and clears any that are still
	// there the next time the manifest is written.
	delete(existing, "checksums.b3")

	merged := make([]string, 0, len(existing))
	for rel, hash := range existing {
		merged = append(merged, fmt.Sprintf("%s  %s", hash, rel))
	}
	return merged, nil
}

// readChecksums reads a checksums.b3 into rel → hash. A manifest that does not
// exist is empty, not an error. One that exists but cannot be read in full —
// it cannot be opened, or a read fails part-way — is an error, along with
// whatever was read before the failure.
//
// Every reader used to stop at the first failed read and carry on with what it
// had, so an I/O error part-way through a manifest looked exactly like a
// shorter manifest: -verify checked fewer files and passed, and anything that
// rewrote the manifest dropped the entries it never saw.
func readChecksums(path string) (map[string]string, error) {
	out := make(map[string]string)
	f, err := os.Open(path)
	if os.IsNotExist(err) {
		return out, nil
	}
	if err != nil {
		return out, err
	}
	defer f.Close()
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		parts := strings.SplitN(scanner.Text(), "  ", 2)
		if len(parts) == 2 {
			out[parts[1]] = parts[0]
		}
	}
	if err := scanner.Err(); err != nil {
		return out, fmt.Errorf("reading %s: %w", path, err)
	}
	return out, nil
}

// readChecksumFile is readChecksums for callers that only display what a
// manifest says, or that derive something that is cheap to rebuild: a read
// error is reported and whatever was read is used. Anything that decides what
// is safe, or writes a manifest back, uses readChecksums and stops instead.
func readChecksumFile(path string) map[string]string {
	out, err := readChecksums(path)
	if err != nil {
		fmt.Printf("%s %v\n", yellow("warning:"), err)
	}
	return out
}

// isFullyChecksummed reports whether every file currently on disk in dir has
// an entry in its checksums.b3. Returns false if checksums.b3 is absent or
// doesn't cover all files (e.g. from a partial previous run or new ingest).
func isFullyChecksummed(dir string) bool {
	manifest, err := readChecksums(filepath.Join(dir, "checksums.b3"))
	if err != nil || len(manifest) == 0 {
		return false
	}
	files, err := contentFiles(dir)
	if err != nil || len(files) == 0 {
		return false
	}
	for _, f := range files {
		if manifest[f.rel] == "" {
			return false
		}
	}
	return true
}

// writeFileAtomic writes a file by way of a temporary in the same directory
// and a rename, so a reader either sees the previous contents or the new ones
// and never a half-written file. Used for manifests that are rewritten while
// another qcp command may be reading them — proxies.json is rewritten after
// every clip a -proxy run finishes, and -index reads it as it goes.
//
// The temporary is synced before the rename, so a power cut or a drive pulled
// mid-write cannot leave the new name pointing at contents that never reached
// the disk.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	tmp := partPath(path)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if serr := f.Sync(); err == nil {
		err = serr
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		os.Remove(tmp)
	}
	return err
}

// writeChecksums replaces a checksums.b3 with lines, sorted. It goes through
// writeFileAtomic because the manifest is the only record of what the footage
// hashed to when it was known good: written in place, a crash or an unplugged
// drive mid-write left it truncated, and every hash past the cut was gone.
func writeChecksums(path string, lines []string) error {
	sort.Strings(lines)
	return writeFileAtomic(path, []byte(strings.Join(lines, "\n")+"\n"), 0644)
}

// addChecksums merges lines into the checksums.b3 at path — see mergeChecksums.
func addChecksums(path string, lines []string) error {
	merged, err := mergeChecksums(path, lines)
	if err != nil {
		return err
	}
	return writeChecksums(path, merged)
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// normaliseVol canonicalises a drive name for matching against -to/-from,
// so drives can be named without worrying about case or stray spaces.
func normaliseVol(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

// sanitizeMission turns a typed mission name into what follows "NNN_" in the
// mission's directory name, or "" if nothing usable is left.
//
// It used to replace spaces and nothing else, so the name went into a path as
// typed: a "/" made nested directories, "../" put the footage outside the year
// directory altogether, and the characters exFAT will not store (\ : * ? " < >
// |) made the copy fail part-way on an exFAT drive. Those, and control
// characters, become "_". Trailing dots go too: exFAT and Windows drop them
// silently, which would make the name on the drive differ from the one qcp
// recorded.
func sanitizeMission(name string) string {
	var b strings.Builder
	for _, r := range strings.TrimSpace(name) {
		switch {
		case r == ' ', r < 0x20, r == 0x7f, strings.ContainsRune(`/\:*?"<>|`, r):
			b.WriteRune('_')
		default:
			b.WriteRune(r)
		}
	}
	s := strings.TrimRight(b.String(), ". ")
	if strings.Trim(s, "_.") == "" {
		return "" // nothing but separators and dots: not a name
	}
	return s
}

func expandPath(p string) (string, error) {
	if strings.HasPrefix(p, "~") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		return filepath.Join(home, p[1:]), nil
	}
	return filepath.Abs(p)
}

// stdin is the one reader every prompt goes through. Each prompt used to make
// its own bufio.Reader, or use fmt.Scan, which reads a word and leaves the rest
// of the line behind; a buffered reader can swallow input meant for the next
// prompt, and fmt.Scan's leftover newline reads as an empty answer to it.
var (
	stdin   = bufio.NewReader(os.Stdin)
	stdinMu sync.Mutex
)

// readLine reads one line of input, trimmed. At the end of input it returns
// what it has and io.EOF, so a prompt can stop rather than ask forever.
func readLine() (string, error) {
	stdinMu.Lock()
	defer stdinMu.Unlock()
	line, err := stdin.ReadString('\n')
	line = strings.TrimSpace(line)
	if err != nil && line != "" {
		return line, nil // a last line without a newline still counts
	}
	return line, err
}

func confirm() bool {
	return ask("Confirm?")
}

// ask puts a yes/no question and reports whether the answer was y. No answer —
// the end of input — is a no.
func ask(question string) bool {
	fmt.Printf("  %s [y/n]  ", question)
	resp, _ := readLine()
	return resp == "y"
}

// askYesNo asks until it gets y or n. At the end of input it answers n, so
// nothing is deleted because input ran out.
func askYesNo(question string) bool {
	for {
		fmt.Print(question)
		resp, err := readLine()
		switch {
		case resp == "y":
			return true
		case resp == "n", err != nil:
			return false
		}
	}
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

// keepAwake runs caffeinate in the background to keep the Mac and its disks
// awake for as long as this process runs, and returns it.
//
// -w ties caffeinate's life to this process: it exits when qcp does, however qcp
// ends — a normal return, os.Exit, a panic or kill -9. Without it nothing ever
// stopped it. A child is not killed with its parent on macOS, so every run left
// a caffeinate behind, reparented to launchd, holding the Mac out of idle sleep
// and the disks spinning; 382 of them had accumulated over 47 days.
func keepAwake() *exec.Cmd {
	cmd := exec.Command("caffeinate", "-mi", "-w", strconv.Itoa(os.Getpid()))
	if err := cmd.Start(); err != nil {
		return nil
	}
	go cmd.Wait() // reap it if it ends first, so it never lingers as a zombie
	return cmd
}

func exit(code int, msg string, args ...any) {
	fmt.Printf(msg+"\n", args...)
	os.Exit(code)
}

// isDisabled reports whether a string flag was used to switch a feature off,
// as in -proxy=false. -proxy carries a mission selection elsewhere, so the
// negation has to be recognised by value rather than by a separate bool flag.
func isDisabled(s string) bool {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "false", "no", "off", "0":
		return true
	}
	return false
}
