package main

import (
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"lukechampine.com/blake3"
)

// copyBufSize is the chunk size used for both copying and plain hashing.
const copyBufSize = 4 * 1024 * 1024

// copyDepth is how many chunks are in flight in the copy pipeline. Two is
// enough to keep both ends busy — one buffer filling from the source while the
// other drains to the destination — and costs copyDepth*copyBufSize of live
// buffer per concurrent copy.
const copyDepth = 2

func prepJob(src, dst, rel, dstRoot string, bar *barTracker) func() <-chan *result {
	return func() <-chan *result {
		done := make(chan *result)
		go func() {
			r := job(src, dst, bar)
			r.dst = dst
			r.rel = rel
			r.dstRoot = dstRoot
			done <- r
			close(done)
		}()
		return done
	}
}

// job copies one file and returns its BLAKE3 hash. Note that it opens src per
// destination, so copying to N drives reads the source N times — see "Known
// inefficiencies" in the README for what fixing that would involve.
func job(src, dst string, bar *barTracker) *result {
	rd, err := os.Open(src)
	if err != nil {
		return &result{err: err}
	}
	defer rd.Close()

	info, err := os.Stat(src)
	if err != nil {
		return &result{err: err}
	}
	perm := info.Mode().Perm()

	if err := os.MkdirAll(filepath.Dir(dst), 0777); err != nil {
		return &result{err: err}
	}
	// The bytes go to a temporary and only take the destination name once they
	// are on the disk, so a run killed mid-copy leaves either nothing or a
	// whole file at dst. Everything that decides what still needs copying goes
	// by that name alone — the os.Stat in the ingest scan, findFiles on the
	// sync side — so a truncated file left sitting there would pass for a
	// finished one and never be copied or verified again.
	tmp := partPath(dst)
	wr, err := os.Create(tmp)
	if err != nil {
		return &result{err: err}
	}

	h := blake3.New(32, nil)
	var w io.Writer = wr
	if bar != nil {
		w = &progressWriter{w: wr, tracker: bar}
	}
	n, err := copyPipelined(w, rd, h)
	syncErr := wr.Sync()
	closeErr := wr.Close()
	if err != nil {
		os.Remove(tmp)
		return &result{err: err}
	}
	if syncErr != nil {
		os.Remove(tmp)
		return &result{err: syncErr}
	}
	if closeErr != nil {
		os.Remove(tmp)
		return &result{err: closeErr}
	}

	if err := os.Chmod(tmp, perm); err != nil {
		os.Remove(tmp)
		return &result{err: err}
	}
	// The copy keeps the source's mtime. A card file's mtime is when it was
	// recorded, and -organise dates a file by it when nothing better is
	// available; a copy stamped with the time it was made dated a cold copy by
	// its sync and could file it into a different season than the hot one.
	if err := os.Chtimes(tmp, info.ModTime(), info.ModTime()); err != nil {
		os.Remove(tmp)
		return &result{err: err}
	}
	if err := os.Rename(tmp, dst); err != nil {
		os.Remove(tmp)
		return &result{err: err}
	}

	return &result{n: n, srcHash: hex.EncodeToString(h.Sum(nil))}
}

// discardUnverified removes a copy that failed verification, or whose source no
// longer matched its manifest. Every command decides what still needs copying
// by whether the destination name exists, so a bad file left under its final
// name was skipped by every re-run, and the next -checksum recorded its hash as
// the good one. Only ever called on a file this run wrote; the source still
// holds the good copy, so a re-run copies it again.
func discardUnverified(dst string) {
	if err := os.Remove(dst); err != nil && !os.IsNotExist(err) {
		fmt.Printf("\n%s could not remove %s: %v — delete it before re-running\n", red("ERROR"), dst, err)
	}
}

// sourceManifest is a job's source checksums.b3, or why it could not be read.
type sourceManifest struct {
	sums map[string]string
	err  error
}

// sourceSums reads the checksums.b3 of every job's source directory, keyed by
// the destination directory the job writes to, for sourceMismatch.
func sourceSums[J any](jobs []J, dirs func(J) (src, dst string)) map[string]sourceManifest {
	out := make(map[string]sourceManifest, len(jobs))
	for _, j := range jobs {
		src, dst := dirs(j)
		if _, ok := out[dst]; !ok {
			sums, err := readChecksums(filepath.Join(src, "checksums.b3"))
			out[dst] = sourceManifest{sums, err}
		}
	}
	return out
}

// sourceMismatch reports whether the bytes a drive-to-drive copy read from its
// source disagree with the source's own checksums.b3. Verifying the destination
// against the bytes that were read only proves the copy is faithful; a source
// that has rotted since it was recorded was copied faithfully too, and the
// destination's manifest then recorded the damage as the good hash. A file the
// source manifest does not mention has nothing to be checked against.
//
// A source manifest that exists but cannot be read fails every file copied from
// it: the drive is failing a read, and nothing it holds can be vouched for.
func sourceMismatch(sm sourceManifest, r *result) bool {
	if sm.err != nil {
		return true
	}
	want := sm.sums[r.rel]
	return want != "" && want != r.srcHash
}

// partMarker is in the name of every file qcp is still writing — a copy in
// flight here, an ffmpeg rendition in proxy.go. sweepPartFiles finds them by it.
const partMarker = ".qcp-part"

// partPath is the temporary a copy is written under: the destination name with
// partMarker in front of it, in the destination's own directory so the rename
// is within one filesystem. The leading dot is what keeps a temporary out of
// the scans while it exists — findFiles skips any path component starting with
// one, as does scanUnorganised — so a leftover from a run that was killed
// outright is invisible rather than being taken for footage and carried into a
// manifest, a cold drive or a new mission.
func partPath(dst string) string {
	return filepath.Join(filepath.Dir(dst), partMarker+"-"+filepath.Base(dst))
}

// sweepCopyParts clears copy temporaries out of dirs and reports how many went,
// so the bytes a killed run left behind are reclaimed rather than sitting
// hidden on the drive forever. Callers sweep the mission directories they are
// about to copy into, before any copy of the run has started.
//
// There is no lock over footage the way there is over a proxy tree, so a second
// qcp copying into the same mission at the same time can have a temporary swept
// from under it. That copy fails its rename and is reported as a failed file —
// loud, and no worse than two runs racing for the same destination name already
// is.
func sweepCopyParts(dirs []string) int {
	seen := make(map[string]bool, len(dirs))
	var n int
	for _, dir := range dirs {
		if seen[dir] {
			continue
		}
		seen[dir] = true
		n += len(sweepPartFiles(dir))
	}
	return n
}

// copyPipelined streams src into dst with reads and writes overlapped, so a
// slow destination is not left idle while the next chunk is read. Each chunk is
// hashed into h on the read side: BLAKE3 runs far ahead of any disk, and the
// source is the faster end in the copies that matter, so that is where the
// spare time is.
//
// Chunks are read with ReadFull so the destination always sees full-size
// writes, which matters for the spinning archive drives.
func copyPipelined(dst io.Writer, src io.Reader, h io.Writer) (int64, error) {
	type chunk struct {
		buf []byte
		n   int
	}

	free := make(chan []byte, copyDepth)
	filled := make(chan chunk, copyDepth)
	abort := make(chan struct{}) // closed by the writer when it gives up
	for i := 0; i < copyDepth; i++ {
		free <- make([]byte, copyBufSize)
	}

	// Reader: fill a buffer, hash it, hand it to the writer. Every exit path
	// closes filled, which is what releases the writer below.
	var readErr error
	go func() {
		defer close(filled)
		for {
			var buf []byte
			select {
			case buf = <-free:
			case <-abort:
				return
			}
			n, err := io.ReadFull(src, buf)
			if n > 0 {
				h.Write(buf[:n])
				select {
				case filled <- chunk{buf, n}:
				case <-abort:
					return
				}
			}
			switch err {
			case nil:
			case io.EOF, io.ErrUnexpectedEOF:
				return
			default:
				readErr = err
				return
			}
		}
	}()

	var written int64
	var writeErr error
	for c := range filled {
		n, err := dst.Write(c.buf[:c.n])
		written += int64(n)
		if err == nil && n != c.n {
			err = io.ErrShortWrite
		}
		if err != nil {
			writeErr = err
			break
		}
		// Never blocks: free is sized for every buffer in the pipeline.
		free <- c.buf
	}

	if writeErr != nil {
		close(abort)
		for range filled { // let the reader out of a pending send, then exit
		}
		return written, writeErr
	}
	// The reader closed filled, so its write to readErr is visible here.
	return written, readErr
}

// readerOnly hides any WriteTo method from io.CopyBuffer. CopyBuffer prefers
// src.WriteTo when it exists and then ignores the buffer it was handed —
// *os.File has one, so an unwrapped file would be hashed in 32 KiB chunks.
type readerOnly struct{ io.Reader }

func hashFile(path string, bar *barTracker) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := blake3.New(32, nil)
	var r io.Reader = f
	if bar != nil {
		r = &progressReader{r: f, tracker: bar}
	}
	buf := make([]byte, copyBufSize)
	if _, err := io.CopyBuffer(h, readerOnly{r}, buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
